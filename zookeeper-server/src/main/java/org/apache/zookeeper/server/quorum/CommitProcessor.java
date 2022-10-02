/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.WorkerService;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooKeeperServerListener;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 *
 * The CommitProcessor is multi-threaded. Communication between threads is
 * handled via queues, atomics, and wait/notifyAll synchronized on the
 * processor. The CommitProcessor acts as a gateway for allowing requests to
 * continue with the remainder of the processing pipeline. It will allow many
 * read requests but only a single write request to be in flight simultaneously,
 * thus ensuring that write requests are processed in transaction id order.
 *
 *   - 1   commit processor main thread, which watches the request queues and
 *         assigns requests to worker threads based on their sessionId so that
 *         read and write requests for a particular session are always assigned
 *         to the same thread (and hence are guaranteed to run in order).
 *   - 0-N worker threads, which run the rest of the request processor pipeline
 *         on the requests. If configured with 0 worker threads, the primary
 *         commit processor thread runs the pipeline directly.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 commit
 * processor thread and 32 worker threads.
 *
 * Multi-threading constraints:
 *   - Each session's requests must be processed in order.
 *   - Write requests must be processed in zxid order
 *   - Must ensure no race condition between writes in one session that would
 *     trigger a watch being set by a read request in another session
 *
 * The current implementation solves the third constraint by simply allowing no
 * read requests to be processed in parallel with write requests.
 *
 * CommitProcessor是多线程的，线程间通信通过queue，atomic和wait/notifyAll同步。CommitProcessor扮演一个网关角色，允许请求到剩下的处理管道。在同一瞬间，它支持多个读请求而仅支持一个写请求，这是为了保证写请求在事务中的顺序。
 *
 * 1个主线程，它监控请求队列，并将请求分发到工作线程，分发过程基于sessionId，这样特定session的读写请求通常分发到同一个线程，因而可以保证运行的顺序。
 *
 * 0~N个工作线程，他们在请求上运行剩下的请求处理管道。如果配置为0个工作线程，主线程将会直接运行管道。
 *
 * 经典（默认）线程数是：在32核的机器上，一个主线程和32个工作线程。
 *
 * 多线程的限制：
 *
 * 每个session的请求处理必须是顺序的。
 *
 * 写请求处理必须按照zxid顺序。
 *
 * 必须保证一个session内不会出现写条件竞争，条件竞争可能导致另外一个session的读请求触发监控。
 *
 * 当前实现解决第三个限制，仅仅通过不允许在写请求时允许读进程的处理。
 *
 * 事务提交处理器。对于非事务请求，该处理器会直接将其交付给下一级处理器处理；
 * 对于事务请求，其会等待集群内针对Proposal的投票直到该Proposal可被提交，
 * 利用CommitProcessor，每个服务器都可以很好地控制对事务请求的顺序处理。
 *
 * Commit流程：
 *
 * 1.将请求交付给CommitProcessor处理器。CommitProcessor处理器在收到请求后，并不会立即处理，而是将其放入到queuedRequest队列中
 *
 * 2.处理queuedRequests队列请求。CommitProcessor处理器会有一个单独的线程从queuedRequest队列中取出请求进行处理
 *
 * 3.标记nextPending   对应 run 方法
 *
 *   若从queuedRequest队列中取出的请求是一个事务请求，那么就需要进行集群中各服务器之间的投票处理，同时需要将nextPending标记为当前请求。标记nextPending的作用：
 *   1）确保事务请求的顺序性；
 *   2）便于CommitProcessor处理器检测当前集群中是否正在进行事务请求的投票
 *
 * 4.等待Proposal投票
 *
 *   在Commit流程处理期间，Leader已经根据当前事务请求生成了一个提议Proposal，并广播给所有的Follower。因此，在这个时候，Commit流程需要wait等待，直到投票结束
 *
 * 5.投票通过。若一个提议获得过半的票数，就会进入请求提交阶段。ZK会将该请求放入committedRequests队列中，同时唤醒Commit流程。  对应 commit 方法
 *
 * 6.提交请求  对应 processCommitted 方法
 *
 *  一旦发现committedRequests队列中已经有可以提交的事务请求，那么Commit流程就会开始提交请求。在提交请求前，为了保证事务请求的顺序执行，
 *  Commit流程还会对比之前标记的nextPending和committedRequests队列中第一个请求是否一致。若检测通过，交付给下一个请求处理器FinalRequestProcessor
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /** Default: numCores */
    public static final String ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS = "zookeeper.commitProcessor.numWorkerThreads";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT = "zookeeper.commitProcessor.shutdownTimeout";

    /**
     * Requests that we are holding until the commit comes in.
     */
    protected final LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    /**
     * Requests that have been committed.
     *
     * leader 对于事务请求（create，setData等），leader中ProposalRequestProcessor处理器会将请求提案发送给所有的followers，
     * followers响应leader，leader在收到follower的ack包后，leader中LearnerHandler会调processAck处理响应，当收到过半follower的proposal ack的时候将调CommitProcessor.commit()方法提交，
     * 把请求放入到队列committedRequests中
     */
    protected final LinkedBlockingQueue<Request> committedRequests = new LinkedBlockingQueue<Request>();

    /** Request for which we are currently awaiting a commit */
    protected final AtomicReference<Request> nextPending = new AtomicReference<Request>();
    /** Request currently being committed (ie, sent off to next processor) */
    private final AtomicReference<Request> currentlyCommitting = new AtomicReference<Request>();

    /** The number of requests currently being processed */
    protected AtomicInteger numRequestsProcessing = new AtomicInteger(0);

    RequestProcessor nextProcessor;

    protected volatile boolean stopped = true;
    private long workerShutdownTimeoutMS;
    protected WorkerService workerPool;

    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be false if the CommitProcessor is in a Leader pipeline.
     */
    // 在leader端是false，learner端是true，因为learner端sync请求需要等待leader回复，而leader端本身则不需要
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id, boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    private boolean isProcessingRequest() {
        return numRequestsProcessing.get() != 0;
    }

    private boolean isWaitingForCommit() {
        return nextPending.get() != null;
    }

    private boolean isProcessingCommit() {
        return currentlyCommitting.get() != null;
    }

    protected boolean needCommit(Request request) {
        switch (request.type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createTTL:
            case OpCode.createContainer:
            case OpCode.delete:
            case OpCode.deleteContainer:
            case OpCode.setData:
            case OpCode.reconfig:
            case OpCode.multi:
            case OpCode.setACL:
                return true;
            case OpCode.sync:
                return matchSyncs;
            case OpCode.createSession:
            case OpCode.closeSession:
                return !request.isLocalSession();
            default:
                return false;
        }
    }

    @Override
    public void run() {
        Request request;
        try {
            while (!stopped) {
                synchronized (this) {
                    // 正在运行 &&
                    // (待处理请求队列为空 || 当前有请求等待被commit || 当前有事务请求正在被处理) && (committed请求队列为空 || 当前有请求正在被处理)
                    // 会调用wait的几种情况
                    // 1. 待处理请求队列为空 && committed请求队列为空
                    // 2. 待处理请求队列为空 && 当前有请求正在被处理
                    // 3. 当前有请求等待被commit && committed请求队列为空
                    // 4. 当前有请求等待被commit && 当前有请求正在被处理
                    // 5. 当前有事务请求正在被处理 && committed请求队列为空
                    // 6. 当前有事务请求正在被处理 && 当前有请求正在被处理
                    while (!stopped && ((queuedRequests.isEmpty() || isWaitingForCommit() || isProcessingCommit()) && (committedRequests.isEmpty() || isProcessingRequest()))) {
                        wait();
                    }
                }

                /*
                 * Processing queuedRequests: Process the next requests until we
                 * find one for which we need to wait for a commit. We cannot
                 * process a read request while we are processing write request.
                 */
                // 正在运行 && 没有请求等待提交 && 没有请求正在处理 && 任务队列中有待处理的请求
                while (!stopped && !isWaitingForCommit() && !isProcessingCommit() && (request = queuedRequests.poll()) != null) {
                    // 判断是否需要等待对该请求的提交
                    if (needCommit(request)) {
                        nextPending.set(request);
                    } else {
                        // 不需要等待提交，将请求交给nextProcessor处理
                        sendToNextProcessor(request);
                    }
                }

                /*
                 * Processing committedRequests: check and see if the commit
                 * came in for the pending request. We can only commit a
                 * request when there is no other request being processed.
                 */
                // 走到这里代表
                // 有请求等待提交 || 有请求正在处理 || 任务队列中没有请求
                processCommitted();
            }
        } catch (Throwable e) {
            handleException(this.getName(), e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    /*
     * Separated this method from the main run loop
     * for test purposes (ZOOKEEPER-1863)
     */
    protected void processCommitted() {
        Request request;

        // 正在运行 && 没有请求正在被nextProcessor处理 && 有尚未处理的已提交请求
        if (!stopped && !isProcessingRequest() && (committedRequests.peek() != null)) {

            /*
             * ZOOKEEPER-1863: continue only if there is no new request
             * waiting in queuedRequests or it is waiting for a
             * commit.
             */
            if (!isWaitingForCommit() && !queuedRequests.isEmpty()) {
                return;
            }
            // committedRequests中的请求按zxid有序排列
            request = committedRequests.poll();

            /*
             * We match with nextPending so that we can move to the next request when it is committed.
             * We also want to use nextPending because it has the cnxn member set properly.
             */
            Request pending = nextPending.get();
            // 判断当前等待被提交的请求是否和当前已提交的最小事务id的请求是同一个
            if (pending != null && pending.sessionId == request.sessionId && pending.cxid == request.cxid) {
                // we want to send our version of the request.
                // the pointer to the connection in the request
                pending.setHdr(request.getHdr());
                pending.setTxn(request.getTxn());
                pending.zxid = request.zxid;
                // Set currentlyCommitting so we will block until this completes. Cleared by CommitWorkRequest after nextProcessor returns.
                // 将请求标志为正在被提交
                currentlyCommitting.set(pending);
                // 清空等待被提交请求标志
                nextPending.set(null);
                // 交给下一个processor处理
                sendToNextProcessor(pending);
            } else {
                // this request came from someone else so just send the commit packet
                currentlyCommitting.set(request);
                sendToNextProcessor(request);
            }
        }
    }

    @Override
    public void start() {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numWorkerThreads = Integer.getInteger(ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS, numCores);
        workerShutdownTimeoutMS = Long.getLong(ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT, 5000);

        LOG.info("Configuring CommitProcessor with " + (numWorkerThreads > 0 ? numWorkerThreads : "no") + " worker threads.");
        if (workerPool == null) {
            workerPool = new WorkerService("CommitProcWork", numWorkerThreads, true);
        }
        stopped = false;
        super.start();
    }

    /**
     * Schedule final request processing; if a worker thread pool is not being
     * used, processing is done directly by this thread.
     */
    private void sendToNextProcessor(Request request) {
        // 增加正在处理请求计数
        numRequestsProcessing.incrementAndGet();
        // 任务线程池调度执行
        workerPool.schedule(new CommitWorkRequest(request), request.sessionId);
    }

    /**
     * CommitWorkRequest is a small wrapper class to allow
     * downstream processing to be run using the WorkerService
     */
    private class CommitWorkRequest extends WorkerService.WorkRequest {
        private final Request request;

        CommitWorkRequest(Request request) {
            this.request = request;
        }

        @Override
        public void cleanup() {
            if (!stopped) {
                LOG.error("Exception thrown by downstream processor," + " unable to continue.");
                CommitProcessor.this.halt();
            }
        }

        public void doWork() throws RequestProcessorException {
            try {
                // 1.将请求交给nextProcessor处理
                nextProcessor.processRequest(request);
            } finally {
                // If this request is the commit request that was blocking
                // the processor, clear.
                // 2.清空正在处理事务请求标记
                currentlyCommitting.compareAndSet(request, null);

                /*
                 * Decrement outstanding request count. The processor may be
                 * blocked at the moment because it is waiting for the pipeline
                 * to drain. In that case, wake it up if there are pending
                 * requests.
                 */
                // 3.减少当前正在处理的请求计数
                if (numRequestsProcessing.decrementAndGet() == 0) {
                    if (!queuedRequests.isEmpty() || !committedRequests.isEmpty()) {
                        wakeup();
                    }
                }
            }
        }
    }

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    synchronized private void wakeup() {
        notifyAll();
    }

    // 提交事务请求
    public void commit(Request request) {
        if (stopped || request == null) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Committing request:: " + request);
        }
        committedRequests.add(request);
        if (!isProcessingCommit()) {
            wakeup();
        }
    }

    @Override
    public void processRequest(Request request) {
        if (stopped) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + request);
        }
        // 将请求都添加到queuedRequests集合中
        queuedRequests.add(request);
        if (!isWaitingForCommit()) {
            wakeup();
        }
    }

    private void halt() {
        stopped = true;
        wakeup();
        queuedRequests.clear();
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        halt();

        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }

        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}
