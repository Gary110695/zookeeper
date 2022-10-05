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

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and
 * SyncRequestProcessor.
 *
 * 事务投票处理器。Leader服务器事务处理流程的发起者，对于非事务性请求，ProposalRequestProcessor会直接将请求
 * 转发到CommitProcessor处理器，不再做任何处理，而对于事务性请求，除了将请求转发到CommitProcessor外，还会
 * 根据请求类型创建对应的Proposal提议，并发送给所有的Follower服务器来发起一次集群内的事务投票。同时，
 * ProposalRequestProcessor还会将事务请求交付给SyncRequestProcessor进行事务日志的记录。
 *
 * 每个事务请求都需要过半机器投票才能被真正应用到ZK的内存数据库中，这个投票 + 统计过程被称为 Proposal流程
 *
 * 1.发起投票。若是事务请求，Leader会发起一轮事务投票。在发起投票之前，会首先检查当前服务端的ZXID是否可用。  具体逻辑在 zks.getLeader().propose(request) 中
 *
 * 2.生成提议Proposal。若ZXID可用，即开始进行事务投票。ZK将请求头 + 事务体 + ZXID + 请求本身序列化到Proposal对象中（该对象就是一个提议，即针对ZK服务器状态的一次变更申请）
 *
 * 3.广播提议。生成提议后，Leader会以ZXID为标识，将给提议放入投票箱outstandingProposals中，同时会将该提议广播给所有Follower服务器。
 *
 * 4.收集投票。Follower收到该提议后，会进入Sync流程进行事务日志记录，一旦记录完成，就会发送ACK给Leader，Leader根据这些ACK消息来统计每个提议的投票情况。当一个提议获得过半机器的投票，就可以认为该提议通过，接下来就可以进入Commit阶段。
 *
 *      具体逻辑在 Follower#followLeader -> processPacket -> case Leader.PROPOSAL 中
 *
 * 5.将请求放入toBeApplied队列。在该提议被提交前，ZK首先会将其放入到toBeApplied队列中
 *
 * 6.广播COMMIT消息。一旦ZK确认一个提议可以被提交，那么Leader就会向Follower发送COMMIT消息，让所有服务器都能够提交该提议。
 */
public class ProposalRequestProcessor implements RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ProposalRequestProcessor.class);

    LeaderZooKeeperServer zks;

    RequestProcessor nextProcessor;

    SyncRequestProcessor syncProcessor;

    public ProposalRequestProcessor(LeaderZooKeeperServer zks, RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());
        syncProcessor = new SyncRequestProcessor(zks, ackProcessor);
    }

    /**
     * initialize this processor
     */
    public void initialize() {
        syncProcessor.start();
    }

    public void processRequest(Request request) throws RequestProcessorException {
        // LOG.warn("Ack>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = " + request.sessionId);
        // request.addRQRec(">prop");


        /* In the following IF-THEN-ELSE block, we process syncs on the leader.
         * If the sync is coming from a follower, then the follower
         * handler adds it to syncHandler. Otherwise, if it is a client of
         * the leader that issued the sync command, then syncHandler won't
         * contain the handler. In this case, we add it to syncHandler, and
         * call processRequest on the next processor.
         */
        // 如果请求来自learner
        // 什么时候发来的是LearnerSyncRequest？在 FollowerRequestProcessor#run -> case OpCode.sync
        if (request instanceof LearnerSyncRequest) {
            zks.getLeader().processSync((LearnerSyncRequest)request);
        } else {
            // 事务和非事务请求都会将该请求流转到下一个processor（CommitProcessor）
            nextProcessor.processRequest(request);
            // 而针对事务请求的话（事务请求头不为空），则还需要进行事务投票等动作
            if (request.getHdr() != null) {
                // We need to sync and get consensus on any transactions
                try {
                    zks.getLeader().propose(request);
                } catch (XidRolloverException e) {
                    throw new RequestProcessorException(e.getMessage(), e);
                }
                // 将事务请求交付给SyncRequestProcessor进行事务日志的记录
                // SyncRequestProcessor接收到请求后，会判断该请求是否为事务请求。针对事务请求，会通过事务日志的形式将其记录下来
                // 完成事务日志记录后，每个Follower服务器会向Leader服务器发送ACK消息，表明自身完成了事务日志的记录，以便Leader统计每个事务请求的投票情况
                syncProcessor.processRequest(request);
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
        syncProcessor.shutdown();
    }

}
