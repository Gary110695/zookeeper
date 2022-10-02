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

package org.apache.zookeeper.server;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * 该处理器将事务请求批量的写入磁盘文件，事务请求在写入磁盘之前是不会被交给下个处理器处理的
 * 对于非事务请求，会直接交给下个处理器处理
 *
 * <p>
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 * send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 * SendAckRequestProcessor which send the packets to leader.
 * SendAckRequestProcessor is flushable which allow us to force
 * push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 * It never send ack back to the leader, so the nextProcessor will
 * be null. This change the semantic of txnlog on the observer
 * since it only contains committed txns.
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private final ZooKeeperServer zks;
    private final LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();
    private final RequestProcessor nextProcessor;

    // 负责快照的线程，保证数据快照过程不影响ZooKeeper的主流程，需创建一个单独的异步线程来进行数据快照
    private Thread snapInProcess = null;
    volatile private boolean running;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    // 在持久化过程中，使用组提交（Group Commits）来优化磁盘I/O操作。
    // 想象一个场景：当客户端有大量的事务请求，如果每次写请求都同步到磁盘，
    // 那么性能就会产生问题。所以设置该链表来暂存需要持久化到磁盘的Request
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    private final Random r = new Random();
    /**
     * The number of log entries to log before starting a snapshot
     */
    // 默认为100000，表示ZooKeeper每隔snapCount次事务日志记录后进行一个数据快照
    private static int snapCount = ZooKeeperServer.getSnapCount();

    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    /**
     * used by tests to check for changing
     * snapcounts
     *
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     *
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }


    /**
     * 什么是事务日志？
     * Zookeeper服务端针对客户端的所有事务请求（create、update、delete）等操作，在返回成功之前，都会将本次操作的内容持久化到磁盘上，完成之后，才返回客户端成功标志
     *
     * 什么是快照日志？
     * Zookeeper全部节点信息的一个快照，从内存中保存在磁盘上
     */
    @Override
    public void run() {
        try {
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            int randRoll = r.nextInt(snapCount / 2);    // 产生 0 ~ snapCount/2 之间的随机数
            while (true) {
                Request si = null;
                if (toFlush.isEmpty()) {
                    // 取出request，如果没有则阻塞
                    si = queuedRequests.take();
                } else {
                    // 取出request，如果没有则返回
                    si = queuedRequests.poll();
                    // 如果queuedRequests中没有数据，但toFlush不空，则表明现在比较空闲，可以进行flush
                    if (si == null) {
                        flush(toFlush);
                        continue;
                    }
                }
                // 判断请求是否是结束请求（在调用shutdown之后，会在队列中添加一个requestOfDeath）
                if (si == requestOfDeath) {
                    break;
                }
                if (si != null) {
                    // track the number of records written to the log
                    // 将Request写入到事务日志中，如果是事务请求，则返回true，写入成功，否则返回false
                    // 注意：写入动作不是真正的写入磁盘，而是暂时缓存下来，flush操作才是真正将缓存的内容写入到磁盘中
                    if (zks.getZKDatabase().append(si)) {
                        logCount++;
                        // 每进行一次事务日志记录之后，ZooKeeper都会检测当前是否需要进行数据快照
                        // 理论上进行snapCount次事务操作后就会开始数据快照，但是考虑到数据快照对于
                        // ZooKeeper所在机器的整体性能影响，需要尽量避免ZooKeeper集群中所有机器在
                        // 同一时刻进行数据快照。因此ZooKeeper在具体的实现中，并不是严格按照这个
                        // 策略执行，而是采取“过半随机”策略
                        // 即满足如下条件，就进行数据快照
                        // 其中logCount代表了当前已经记录的事务日志数量，randRoll为1 ~ snapCount/2之间的随机数，
                        // 因此该条件就相当于：如果我们配置的snapCount为100000，那么ZooKeeper会在50000 ~ 100000次事务日志记录后进行一次数据快照
                        if (logCount > (snapCount / 2 + randRoll)) {
                            randRoll = r.nextInt(snapCount / 2);
                            // roll the log
                            // 什么是 rollLog ？进行事务日志文件的切换
                            // 所谓的事务日志文件切换时指当前的事务日志文件已经“写满”，需要创建一个新的事务日志文件。
                            // 每次快照的时候会强制执行一次 rollLog
                            // 还有什么时候会 rollLog ？
                            // 如果配置了zookeeper.txnLogSizeLimitInKb，默认是 -1，这个配置限制了 log 单个文件大小（单位是 KB）
                            // 在每次归档的时候，将数据统一刷到磁盘后，会检查当前 log 文件大小是否超过了该参数大小，如果超过了就会进行 rollLog
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                // 快照日志单独启动一个线程来执行，避免阻塞主线程执行
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                    public void run() {
                                        try {
                                            zks.takeSnapshot();
                                        } catch (Exception e) {
                                            LOG.warn("Unexpected exception", e);
                                        }
                                    }
                                };
                                snapInProcess.start();
                            }
                            logCount = 0;
                        }
                    }
                    // 如果是非事务请求（读操作）且toFlush为空
                    // 说明近一段时间读多写少，直接交给下一个处理器处理
                    else if (toFlush.isEmpty()) {
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable)nextProcessor).flush();
                            }
                        }
                        continue;
                    }
                    toFlush.add(si);
                    // 超过1000，直接flush
                    if (toFlush.size() > 1000) {
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        } finally {
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush(LinkedList<Request> toFlush) throws IOException, RequestProcessorException {
        if (toFlush.isEmpty()) return;

        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(requestOfDeath);
        try {
            if (running) {
                this.join();
            }
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        // 只是将request添加到queue中，真正的操作由run()方法执行
        queuedRequests.add(request);
    }

}
