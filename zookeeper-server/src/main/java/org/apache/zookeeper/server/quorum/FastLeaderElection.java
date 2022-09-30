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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 * <p>
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */


public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     * <p>
     * finalizeWait这个值会增大，直到增大到maxNotificationInterval
     */

    final static int maxNotificationInterval = 60000;

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     * <p>
     * 通过QuorumCnxManager管理和其他Server之间的TCP连接
     */

    QuorumCnxManager manager;


    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     * <p>
     * Notifications是一个让其它Server知道当前Server已经改变
     * 了投票的通知消息，(为什么它要改变投票呢？)要么是因为它参与了
     * Leader选举(新一轮的投票，首先投向自己)，要么是它知道了另一个
     * Server具有更大的zxid，或者zxid相同但ServerId更大（所以它要
     * 通知给其它所有Server，它要修改自己的选票）
     */

    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */

        public final static int CURRENTVERSION = 0x2;
        int version;

        /*
         * Proposed leader
         * 当前通知所推荐的leader的serverId
         */ long leader;

        /*
         * zxid of the proposed leader
         * 当前通知所推荐的leader的zxid
         */ long zxid;

        /*
         * Epoch
         * 当前通知所处的选举epoch，即逻辑时钟
         */ long electionEpoch;

        /*
         * current state of sender
         * 当前通知发送者的状态  四个状态：LOOKING, FOLLOWING, LEADING, OBSERVING
         */ QuorumPeer.ServerState state;

        /*
         * Address of sender
         * 当前发送者的ServerId
         */ long sid;

        QuorumVerifier qv;
        /*
         * epoch of the proposed leader
         * 当前通知所推荐的leader的epoch
         */ long peerEpoch;
    }

    static byte[] dummyData = new byte[0];

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type, long leader, long zxid, long electionEpoch, ServerState state, long sid, long peerEpoch, byte[] configData) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
            this.configData = configData;
        }

        /*
         * Proposed leader in the case of notification
         */ long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */ long zxid;

        /*
         * Epoch
         */ long electionEpoch;

        /*
         * Current state;
         */ QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */ long sid;

        /*
         * Used to send a QuorumVerifier (configuration info)
         */ byte[] configData = dummyData;

        /*
         * Leader epoch
         */ long peerEpoch;
    }

    /**
     * 选举时发送和接收选票通知都是异步的，先放入队列，有专门线程处理
     * 下面两个就是发送消息队列，和接收消息的队列
     */
    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     * <p>
     * 通过创建该类实例可以创建两个线程分别处理消息发送和接收
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         * <p>
         * 接收QuorumCnxManager实例在方法run()上的消息，并处理这些消息
         */

        class WorkerReceiver extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            /**
             * 通过QuorumCnxManager manager，获取其他Server发来的消息，然后处理，如果
             * 符合一定条件，放到recvqueue队列里
             */
            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) continue;

                        // The current protocol and two previous generations all send at least 28 bytes
                        if (response.buffer.capacity() < 28) {
                            LOG.error("Got a short response: " + response.buffer.capacity());
                            continue;
                        }

                        // this is the backwardCompatibility mode in place before ZK-107
                        // It is for a version of the protocol in which we didn't send peer epoch
                        // With peer epoch and version the message became 40 bytes
                        boolean backCompatibility28 = (response.buffer.capacity() == 28);

                        // this is the backwardCompatibility mode for no version information
                        boolean backCompatibility40 = (response.buffer.capacity() == 40);

                        response.buffer.clear();

                        // Instantiate Notification and set its attributes
                        // 将收到的消息解析后会封装成Notification，放入recvqueue队列
                        Notification n = new Notification();

                        // 在 buildMsg 方法中编码的，在此处按照顺序解码
                        int rstate = response.buffer.getInt();
                        long rleader = response.buffer.getLong();
                        long rzxid = response.buffer.getLong();
                        long relectionEpoch = response.buffer.getLong();
                        long rpeerepoch;

                        int version = 0x0;
                        if (!backCompatibility28) {
                            rpeerepoch = response.buffer.getLong();
                            if (!backCompatibility40) {
                                /*
                                 * Version added in 3.4.6
                                 */

                                version = response.buffer.getInt();
                            } else {
                                LOG.info("Backward compatibility mode (36 bits), server id: {}", response.sid);
                            }
                        } else {
                            LOG.info("Backward compatibility mode (28 bits), server id: {}", response.sid);
                            rpeerepoch = ZxidUtils.getEpochFromZxid(rzxid);
                        }

                        QuorumVerifier rqv = null;

                        // check if we have a version that includes config. If so extract config info from message.
                        if (version > 0x1) {
                            int configLength = response.buffer.getInt();
                            byte b[] = new byte[configLength];

                            response.buffer.get(b);

                            synchronized (self) {
                                try {
                                    rqv = self.configFromString(new String(b));
                                    QuorumVerifier curQV = self.getQuorumVerifier();
                                    if (rqv.getVersion() > curQV.getVersion()) {
                                        LOG.info("{} Received version: {} my version: {}", self.getId(), Long.toHexString(rqv.getVersion()),
                                                Long.toHexString(self.getQuorumVerifier().getVersion()));
                                        if (self.getPeerState() == ServerState.LOOKING) {
                                            LOG.debug("Invoking processReconfig(), state: {}", self.getServerState());
                                            self.processReconfig(rqv, null, null, false);
                                            if (!rqv.equals(curQV)) {
                                                LOG.info("restarting leader election");
                                                self.shuttingDownLE = true;
                                                self.getElectionAlg().shutdown();

                                                break;
                                            }
                                        } else {
                                            LOG.debug("Skip processReconfig(), state: {}", self.getServerState());
                                        }
                                    }
                                } catch (IOException e) {
                                    LOG.error("Something went wrong while processing config received from {}", response.sid);
                                } catch (ConfigException e) {
                                    LOG.error("Something went wrong while processing config received from {}", response.sid);
                                }
                            }
                        } else {
                            LOG.info("Backward compatibility mode (before reconfig), server id: {}", response.sid);
                        }

                        /*
                         * If it is from a non-voting server (such as an observer or
                         * a non-voting follower), respond right away.
                         */
                        if (!validVoter(response.sid)) {
                            Vote current = self.getCurrentVote();
                            QuorumVerifier qv = self.getQuorumVerifier();
                            ToSend notmsg = new ToSend(ToSend.mType.notification, current.getId(), current.getZxid(), logicalclock.get(), self.getPeerState(), response.sid,
                                    current.getPeerEpoch(), qv.toString().getBytes());

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Receive new notification message. My id = " + self.getId());
                            }

                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            // 判断该通知的发送者的状态
                            switch (rstate) {
                                case 0:
                                    ackstate = QuorumPeer.ServerState.LOOKING;
                                    break;
                                case 1:
                                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                                    break;
                                case 2:
                                    ackstate = QuorumPeer.ServerState.LEADING;
                                    break;
                                case 3:
                                    ackstate = QuorumPeer.ServerState.OBSERVING;
                                    break;
                                default:
                                    continue;
                            }

                            n.leader = rleader;
                            n.zxid = rzxid;
                            n.electionEpoch = relectionEpoch;
                            n.state = ackstate;
                            n.sid = response.sid;
                            n.peerEpoch = rpeerepoch;
                            n.version = version;
                            n.qv = rqv;
                            /*
                             * Print notification info
                             */
                            if (LOG.isInfoEnabled()) {
                                printNotification(n);
                            }

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                // 如果当前节点状态是Looking，则将该选票放入recvqueue队列，用来参与选举
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                // 如果发送该选票的节点状态也是Looking，并且它的选举逻辑时钟比我小，则发送我当前的选票给他
                                if ((ackstate == QuorumPeer.ServerState.LOOKING) && (n.electionEpoch < logicalclock.get())) {
                                    Vote v = getVote();
                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification, v.getId(), v.getZxid(), logicalclock.get(), self.getPeerState(), response.sid,
                                            v.getPeerEpoch(), qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                // 如果当前节点状态不是LOOKING，即已经选出Leader了，并且该消息的发送者是Looking，则当前节点会将当前选票发给他
                                Vote current = self.getCurrentVote();
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Sending new notification. My id ={} recipient={} zxid=0x{} leader={} config version = {}", self.getId(), response.sid,
                                                Long.toHexString(current.getZxid()), current.getId(), Long.toHexString(self.getQuorumVerifier().getVersion()));
                                    }

                                    QuorumVerifier qv = self.getQuorumVerifier();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification, current.getId(), current.getZxid(), current.getElectionEpoch(), self.getPeerState(),
                                            response.sid, current.getPeerEpoch(), qv.toString().getBytes());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("Interrupted Exception while waiting for new message" + e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }

        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         * <p>
         * 取出要发送的消息并将其放入QuorumCnxManager的队列中
         */

        class WorkerSender extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            /**
             * 从sendqueue队列中获取要发送的消息ToSend对象，调用上面说过的buildMsg方法
             * 转换成字节数据，再通过QuorumCnxManager manager广播出去
             */
            public void run() {
                while (!stop) {
                    try {
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (m == null) continue;

                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m message to send
             */
            void process(ToSend m) {
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), m.leader, m.zxid, m.electionEpoch, m.peerEpoch, m.configData);

                manager.toSend(m.sid, requestBuffer);

            }
        }

        WorkerSender ws;
        WorkerReceiver wr;
        Thread wsThread = null;
        Thread wrThread = null;

        /**
         * Constructor of class Messenger.
         *
         * @param manager Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            this.ws = new WorkerSender(manager);

            this.wsThread = new Thread(this.ws, "WorkerSender[myid=" + self.getId() + "]");
            this.wsThread.setDaemon(true);

            this.wr = new WorkerReceiver(manager);

            this.wrThread = new Thread(this.wr, "WorkerReceiver[myid=" + self.getId() + "]");
            this.wrThread.setDaemon(true);
        }

        /**
         * Starts instances of WorkerSender and WorkerReceiver
         */
        void start() {
            this.wsThread.start();
            this.wrThread.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    // 逻辑时钟
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */
    // 记录当前Server所推荐的leader信息，即ServerId，Zxid，Epoch
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock.get();
    }


    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch) {
        byte requestBytes[] = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send, this is called directly only in tests
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(0x1);

        return requestBuffer;
    }

    // 发送投票通知的时候，将ToSend封装的选票信息转换成二进制字节数据传输
    static ByteBuffer buildMsg(int state, long leader, long zxid, long electionEpoch, long epoch, byte[] configData) {
        byte requestBytes[] = new byte[44 + configData.length];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send
         */

        requestBuffer.clear();
        // 注意，是按照一定顺序转换的，接收的时候也按照该顺序解析
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        requestBuffer.putInt(configData.length);
        requestBuffer.put(configData);

        return requestBuffer;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();
        this.messenger = new Messenger(manager);
    }

    /**
     * This method starts the sender and receiver threads.
     */
    public void start() {
        this.messenger.start();
    }

    private void leaveInstance(Vote v) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("About to leave FLE instance: leader={}, zxid=0x{}, my id={}, my state={}", v.getId(), Long.toHexString(v.getZxid()), self.getId(), self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;

    public void shutdown() {
        stop = true;
        proposedLeader = -1;
        proposedZxid = -1;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }

    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {
        // 遍历所有具有选举权的server
        for (long sid : self.getCurrentAndNextConfigVoters()) {
            QuorumVerifier qv = self.getQuorumVerifier();
            // notification msg，通知消息，即将推荐的Leader信息封装成ToSend对象放入发送队列，由WorkerSender线程去发送该消息
            ToSend notmsg = new ToSend(ToSend.mType.notification,     // 消息类型
                    proposedLeader,     // 推荐的Leader的ServerId（myid）
                    proposedZxid,     // 推荐的Leader的zxid
                    logicalclock.get(),     // 此次选举的逻辑时钟
                    QuorumPeer.ServerState.LOOKING,     // 当前Server的状态
                    sid,    // 消息接收者的server id
                    proposedEpoch,      // 推荐的Leader的epoch
                    qv.toString().getBytes());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending Notification: " + proposedLeader + " (n.leader), 0x" + Long.toHexString(proposedZxid) + " (n.zxid), 0x" + Long.toHexString(logicalclock.get()) + " (n.round), " + sid + " (recipient), " + self.getId() + " (myid), 0x" + Long.toHexString(proposedEpoch) + " (n.peerEpoch)");
            }
            // 放入发送队列
            sendqueue.offer(notmsg);
        }
    }

    private void printNotification(Notification n) {
        LOG.info("Notification: " + Long.toHexString(n.version) + " (message format version), " + n.leader + " (n.leader), 0x" + Long.toHexString(n.zxid) + " (n.zxid), 0x" + Long.toHexString(n.electionEpoch) + " (n.round), " + n.state + " (n.state), " + n.sid + " (n.sid), 0x" + Long.toHexString(n.peerEpoch) + " (n.peerEPoch), " + self.getPeerState() + " (my state)" + (n.qv != null ? (Long.toHexString(n.qv.getVersion()) + " (n.config version)") : ""));
    }


    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     * <p>
     * 判断谁更适合做leader，返回true说明外来的更适合，否则当前节点更适合
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" + Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));
        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }

        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */

        return ((newEpoch > curEpoch) || ((newEpoch == curEpoch) && ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * Termination predicate. Given a set of votes, determines if have
     * sufficient to declare the end of the election round.
     * <p>
     * 给定一组选票，决定是否有足够的票数宣布选举结束
     *
     * @param votes Set of votes
     * @param vote  Identifier of the vote received last
     */
    protected boolean termPredicate(Map<Long,Vote> votes, Vote vote) {
        SyncedLearnerTracker voteSet = new SyncedLearnerTracker();
        voteSet.addQuorumVerifier(self.getQuorumVerifier());
        if (self.getLastSeenQuorumVerifier() != null && self.getLastSeenQuorumVerifier().getVersion() > self.getQuorumVerifier().getVersion()) {
            voteSet.addQuorumVerifier(self.getLastSeenQuorumVerifier());
        }

        /*
         * First make the views consistent. Sometimes peers will have different
         * zxids for a server depending on timing.
         */
        // 遍历票箱：从票箱中查找与选票vote相同的选票
        for (Map.Entry<Long,Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                voteSet.addAck(entry.getKey());
            }
        }

        return voteSet.hasAllQuorums();
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes         set of votes
     * @param leader        leader id
     * @param electionEpoch epoch id
     */
    protected boolean checkLeader(Map<Long,Vote> votes, long leader, long electionEpoch) {
        // 先默认true，后面排除
        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */
        // 若推荐的leader是别人
        if (leader != self.getId()) {
            if (votes.get(leader) == null) predicate = false;
            else if (votes.get(leader).getState() != ServerState.LEADING) predicate = false;
        }
        // 若推荐的leader是当前server，则判断逻辑时钟和推荐的epoch是否一样
        else if (logicalclock.get() != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    synchronized void updateProposal(long leader, long zxid, long epoch) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x" + Long.toHexString(zxid) + " (newzxid), " + epoch + " (newepoch), " +  proposedLeader + " " +
                    "(oldleader), " +
                    "0x" + Long.toHexString(proposedZxid) + " (oldzxid), " + proposedEpoch + " (oldepoch)");
        }
        // 更新当前节点的推荐信息
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    synchronized public Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (self.getQuorumVerifier().getVotingMembers().containsKey(self.getId())) return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) try {
            return self.getCurrentEpoch();
        } catch (IOException e) {
            RuntimeException re = new RuntimeException(e.getMessage());
            re.setStackTrace(e.getStackTrace());
            throw re;
        }
        else return Long.MIN_VALUE;
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     */
    public Vote lookForLeader() throws InterruptedException {
        // ----------------------- 1 选举前的初始化工作 --------------------
        try {
            // JMX，Oracle提供的分布式应用程序监控技术
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        if (self.start_fle == 0) {
            self.start_fle = Time.currentElapsedTime();
        }
        try {
            // 用来存储每一个服务器给该节点的合法有效投票，key为服务器id，value为其他节点给该节点1的投票信息
            HashMap<Long,Vote> recvset = new HashMap<Long,Vote>();

            // 存放非LOOKING状态节点（FOLLOWING/LEADING）发来的选票
            HashMap<Long,Vote> outofelection = new HashMap<Long,Vote>();

            // notification Timeout
            int notTimeout = finalizeWait;

            // ----------------------- 2 将自己作为初始leader投出去 ---------------------
            synchronized (this) {
                // 每发起一轮选举，都会使逻辑时钟加1
                // 逻辑时钟可以这么理解：logicalclock代表选举逻辑时钟（类比现实中的第十八次全国人大、第十九次全国人大...），
                // 这个值从0开始递增，在同一次选举中，各节点的值基本相同，也有例外情况，比如在第18次选举中，某个节点A挂了，
                // 其他节点完成了Leader选举，但没过多久，该Leader又挂了，于是进入了第19次Leader选举，同时节点A此时恢复，
                // 加入到Leader选举中，那么节点A的logicallock为18，而其他节点的logicallock为19，针对这种情况，节点A的
                // logicallock会被直接更新为19并参与到第19次Leader选举中。 具体逻辑在下面的 case LOOKING  if (n.electionEpoch > logicalclock.get()) 分支
                logicalclock.incrementAndGet();
                // 更新选票信息
                // getInitId()：获取当前server的id
                // getInitLastLoggedZxid()：获取当前server最后的（也是最大的）zxid
                // getPeerEpoch()：获取当前server的epoch
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info("New election. My id =  " + self.getId() + ", proposed zxid=0x" + Long.toHexString(proposedZxid));
            // 把初始的投票数据发送出去（即第一轮投票），假设当前是z1节点，投票数据(1,0,0)
            // Vote(1,0,0)会发往1，2，3号节点
            // 发往1（即本身）的投票会被直接投递到QuorumCnxManager的recvQueue<Message>中
            // 发往2、3的投票会经由网络
            // 具体逻辑在 WorkerSender#run -> QuorumCnxManager#toSend
            sendNotifications();

            /*
             * Loop in which we exchange notifications until we find a leader
             */
            // ----------------------- 3 循环交换投票直至找出Leader ---------------------
            while ((self.getPeerState() == ServerState.LOOKING) && (!stop)) {   // 一但找到Leader，状态就不再是LOOKING
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 */
                // 由WorkerReceiver线程接收其他Server发来的通知，并将接收到的信息解析封装成Notification，放入recvqueue队列
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);

                if (n != null) System.out.println(n);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                // 什么情况取出来是空呢？
                // 假如广播出去8个，由于网络原因可能只收到3个，第四次取的时候就是空的
                // 还有可能收到8个了，但是选举还没结束，再次取的时候也是空的
                if (n == null) {
                    // 条件成立，说明当前Server和集群连接没有问题，所以重新发送当前Server推荐的Leader的选票通知，目的是为了重新再接收其他Server的回复
                    if (manager.haveDelivered()) {
                        // 重新发送，目的是为了重新再接收
                        sendNotifications();
                    } else {    // 否则说明当前Server和集群已经失联
                        // 重新连接zk集群中的每一个server
                        // 为什么重连了，不需要重新发送通知了呢？
                        // 因为我失联了，但是发送队列中的消息是还再的，重新连接后会重新继续发送，
                        // 而且其他Server在recvqueue.poll为null的时候，如果没有和集群失联，也会重新sendNotifications，所以这里是不需要的
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = (tmpTimeOut < maxNotificationInterval ? tmpTimeOut : maxNotificationInterval);
                    LOG.info("Notification time out: " + notTimeout);
                }
                // 如果从recvqueue取出的投票通知不为空，会先验证投票的发送者和推荐者是否合法，合法了再继续处理
                else if (validVoter(n.sid) && validVoter(n.leader)) {
                    /*
                     * Only proceed if the vote comes from a replica in the current or next
                     * voting view for a replica in the current or next voting view.
                     */
                    switch (n.state) {
                        // 发送者状态为LOOKING
                        case LOOKING:
                            // ----------------------- 验证自己与大家的投票谁更适合做leader ---------------------

                            // 什么情况外来投票epoch大，或者小呢？
                            // 比如5个机器，已经选举好Leader了，有两个已经通知了，另外两个不知道，这个时候刚上任的Leader又突然挂了，
                            // 还没通知到另外两个机器的时候，就会造成这种情况
                            // 已经通知的那两个epoch会再次重新选举的，逻辑时钟会再加一，
                            // 即epoch会再加一，未通知的那两个epoch还没变
                            // 站在未通知的Server角度，在接收到已经通知的Server回复的时候，就会发现回复的通知epoch更大
                            // 站在已经通知的Server角度，在接受到未通知的Server发来的通知时，会发现自己比通知的epoch大

                            // If notification > current, replace and send messages out
                            if (n.electionEpoch > logicalclock.get()) {     // 外来投票epoch大，此时自己已经过时了，选谁都没有意义
                                // 更新当前server所在的选举的逻辑时钟
                                logicalclock.set(n.electionEpoch);
                                // 清空投票箱，之前收集的投票都已经过时了，没意义了
                                recvset.clear();
                                // 判断当前server与n谁更适合做leader，无论谁更适合，都需要更新当前server的推荐信息，然后广播出去
                                // 注意不是当前Server所推荐的，而就是当前Server
                                // 返回true，说明n（外来的）更适合
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                    updateProposal(n.leader, n.zxid, n.peerEpoch);
                                } else {
                                    updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                                }
                                sendNotifications();
                            }
                            // 外来投票epoch小，说明外来的过时了，它的选票没有意义，不做任何处理
                            else if (n.electionEpoch < logicalclock.get()) {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x" + Long.toHexString(n.electionEpoch) + ", " +
                                            "logicalclock=0x" + Long.toHexString(logicalclock.get()));
                                }
                                // 直接break掉switch，重新进入循环，从recvqueue取下一个通知，继续处理
                                break;
                            }
                            // n.electionEpoch == logicalclock.get()，即他们在同一轮选举中
                            else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                                sendNotifications();
                            }

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Adding vote: from=" + n.sid + ", proposed leader=" + n.leader + ", proposed zxid=0x" + Long.toHexString(n.zxid) + ", proposed " +
                                        "election" + " epoch=0x" + Long.toHexString(n.electionEpoch));
                            }

                            // don't care about the version if it's in LOOKING state
                            // 处理完上面情况后，如果没有break，即是外来选票的逻辑时钟更大，或者相等，代表外来选票有效，则将选票放入选票箱
                            // 特殊情况：当前服务器收到外来通知发现外来通知推荐的leader更适合以后，会更新自己的推荐信息并再次广播出去，这个时候
                            // recvqueue除了第一次广播推荐自己收到的回复外，还会收到新一轮广播的回复，对于其他Server而言有可能会回复两次通知，
                            // 但对于本地Server是没有影响的，因为投票箱recvset是一个Map，key是发送消息的服务器的ServerId，每个Server只会记录一个投票，新的会覆盖旧的
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                            // ----------------------- 判断本轮选举是否可以结束了 ---------------------
                            /*
                             * 尝试通过现在已经收到的信息，判断是否已经足够确认最终的leader了，通过方法termPredicate()，
                             * 判断标准很简单：是否已经有超过半数的机器所推举的leader为当前自己所推举的leader.
                             * 如果是，保险起见，最多再等待finalizeWait（默认200ms）的时间进行最后的确认，
                             * 如果发现有了更新的leader信息，则把这个Notification重新放回recvqueue，显然，选举将继续进行。
                             * 否则，选举结束，根据选举的leader是否是自己，设置自己的状态为LEADING或者OBSERVING或者FOLLOWING。
                             */
                            if (termPredicate(recvset, new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch))) {

                                // 已经过半了，但是recvqueue里面的通知还没处理完，还有可能有更适合的Leader通知
                                // Verify if there is any change in the proposed leader

                                // 该循环有两个出口：
                                // break：从该出口跳出，说明n的值不为null，说明在剩余的通知中找到了更适合做leader的通知
                                // while()条件：从该出口跳出，说明n的值为null，说明在剩余的通知中没有比当前server所推荐的leader更适合的了
                                while ((n = recvqueue.poll(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                                    if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch, proposedLeader, proposedZxid, proposedEpoch)) {
                                        // 如果有更合适的，将通知重新加入recvqueue队列的尾部，并break退出循环，此时n != null ，不会进行收尾动作，
                                        // 会重新进行选举，最终还是会更新当前Server的推荐信息为这个更适合的Leader，并广播出去
                                        recvqueue.put(n);
                                        break;
                                    }
                                }

                                /*
                                 * This predicate is true once we don't read any new
                                 * relevant message from the reception queue
                                 */
                                // 若n为null，则说明当前server所推荐的leader就是最终的leader，此时就可以进行收尾工作了
                                if (n == null) {
                                    // 修改当前server的状态
                                    // 如果推荐的Leader就是我自己，修改我当前状态为LEADING
                                    // 如果不是我自己，判断自己是否是参与者，如果是则状态置为FOLLOWING，否则是OBSERVING
                                    self.setPeerState((proposedLeader == self.getId()) ? ServerState.LEADING : learningState());
                                    // 形成最终选票
                                    Vote endVote = new Vote(proposedLeader, proposedZxid, logicalclock.get(), proposedEpoch);
                                    // 清空recvqueue队列
                                    leaveInstance(endVote);
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("self.state=" + self.getServerState() + ", proposed leader=" + proposedLeader + ", proposed zxid=0x" + Long.toHexString(proposedZxid) + "," +
                                                " " +
                                                "proposed " +
                                                "election" + " epoch=0x" + Long.toHexString(proposedEpoch));
                                    }
                                    // 返回最终选票
                                    return endVote;
                                }
                            }
                            break;
                        // 发送者状态为OBSERVING
                        case OBSERVING:
                            LOG.debug("Notification from observer: " + n.sid);
                            // 观察者是不参与Leader选举的，所以收到这样的选票不做任何处理
                            break;
                        // ------------------------- 发送者状态为FOLLOWING/LEADING -----------------------
                        // ----------------------- 处理无需选举的情况 ---------------------
                        // 首先要清楚两点：
                        // 1) 当一个Server接收到其它Server的通知后，无论自己处于什么状态，其都会向那个Server发送自己的通知
                        // 2) 一个Server若能够接收到其它Server的通知，说明该Server不是Observer，而是Participant。因为sendNotifications()方法中是不会给Observer发送的

                        // 有两种场景会出现leader或follower给当前server发送通知：
                        // 1）有新Server要加入一个正常运行的集群时，这个新的server在启动时，
                        //    其状态为looking，要查找leader，其向外发送通知。此时的leader、
                        //    follower的状态肯定不是looking，而分别是leading、following状态。
                        //    当leader、follower接收到通知后，就会向其发送自己的通知
                        //	  此时，当前Server选举的逻辑时间与其它follower或leader的epoch相同，也有可能不同
                        //
                        // 2）当其它Server已经在本轮选举中选出了新的leader，但还没有通知到当前Server
                        //    所以当前Server的状态仍保持为looking而其它Server中的部分主机状态可能
                        //    已经是leading或following了
                        //    此时，当前Server选举的逻辑时间与其它follower或leader的epoch肯定相同

                        // 经过分析可知，最终的两种场景是：
                        // 1）当前Server选举的逻辑时间与其它follower或leader的epoch相同
                        // 2）当前Server选举的逻辑时间与其它follower或leader的epoch不同
                        case FOLLOWING:
                        case LEADING:
                            /*
                             * Consider all notifications from the same epoch
                             * together.
                             */
                            if (n.electionEpoch == logicalclock.get()) {    // epoch相同的情况
                                // 如果是相同逻辑时钟的选举的选票通知，则将其封装成选票放入票箱，注意此时虽然选票的状态不是FOLLOWING就是LEADING，但是因为是处于同一逻辑时钟的选票，所以认为是有效的
                                recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                                // 判断当前server是否应该退出本轮选举了
                                // 其首先判断n所推荐的leader在当前Server的票箱中支持率是否过半
                                // 若过半，再判断n所推荐的leader在outofelection中的状态是否合法
                                // 若合法，则可以退出本轮选举了
                                if (termPredicate(recvset, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state)) && checkLeader(outofelection, n.leader,
                                        n.electionEpoch)) {
                                    // 收尾工作
                                    self.setPeerState((n.leader == self.getId()) ? ServerState.LEADING : learningState());
                                    Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                    leaveInstance(endVote);
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("self.state=" + self.getServerState() + ", proposed leader=" + proposedLeader + ", proposed zxid=0x" + Long.toHexString(proposedZxid) + "," +
                                                " " +
                                                "proposed " +
                                                "election" + " epoch=0x" + Long.toHexString(proposedEpoch));
                                    }
                                    return endVote;
                                }
                            }

                            /*
                             * Before joining an established ensemble, verify that
                             * a majority are following the same leader.
                             */
                            // 将状态为FOLLOWING/LEADING的Server发来的选票放到outofelection集合里
                            outofelection.put(n.sid, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state));
                            if (termPredicate(outofelection, new Vote(n.version, n.leader, n.zxid, n.electionEpoch, n.peerEpoch, n.state)) && checkLeader(outofelection, n.leader
                                    , n.electionEpoch)) {
                                synchronized (this) {
                                    logicalclock.set(n.electionEpoch);
                                    self.setPeerState((n.leader == self.getId()) ? ServerState.LEADING : learningState());
                                }
                                Vote endVote = new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch);
                                leaveInstance(endVote);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("self.state=" + self.getServerState() + ", proposed leader=" + proposedLeader + ", proposed zxid=0x" + Long.toHexString(proposedZxid) + "," +
                                            " " +
                                            "proposed " +
                                            "election" + " epoch=0x" + Long.toHexString(proposedEpoch));
                                }
                                return endVote;
                            }
                            break;
                        default:
                            LOG.warn("Notification state unrecoginized: " + n.state + " (n.state), " + n.sid + " (n.sid)");
                            break;
                    }
                } else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}", manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid Server identifier
     * @return boolean
     */
    private boolean validVoter(long sid) {
        return self.getCurrentAndNextConfigVoters().contains(sid);
    }
}
