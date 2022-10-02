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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a full featured SessionTracker. It tracks session in grouped by tick
 * interval. It always rounds up the tick interval to provide a sort of grace
 * period. Sessions are thus expired in batches made up of sessions that expire
 * in a given interval.
 */
// 单机模式下默认实现类为SessionTrackerImpl
public class SessionTrackerImpl extends ZooKeeperCriticalThread implements SessionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(SessionTrackerImpl.class);

    // 用于根据sessionID来管理Session实体SessionImpl
    protected final ConcurrentHashMap<Long, SessionImpl> sessionsById = new ConcurrentHashMap<Long, SessionImpl>();

    // 定制化实现的按过期时间分桶管理会话的数据结构
    private final ExpiryQueue<SessionImpl> sessionExpiryQueue;

    // 用于根据sessionID来管理会话的超时时间。该数据结构和ZooKeeper内存数据库相连通，会被定期持久化到快照文件中去。
    // 具体逻辑在SyncRequestProcessor#run中，zks.takeSnapshot -> FileTxnSnapLog.save
    // 在启动时会加载sessionsWithTimeout，ZKDatabase#loadDataBase -> .FileTxnSnapLog#restore ->
    private final ConcurrentMap<Long, Integer> sessionsWithTimeout;
    private final AtomicLong nextSessionId = new AtomicLong();
    /**
     * 客户端通过实例化Zookeeper对象来创建于服务端的连接，创建连接成功后，服务端会创建一个Session（会话），
     * Session拥有唯一的SessionId，后续客户端就使用这个sessionId来标识当前会话
     */
    public static class SessionImpl implements Session {
        // 客户端发起一次连接成功后，服务端会创建一个Session对象
        SessionImpl(long sessionId, int timeout) {
            this.sessionId = sessionId;
            this.timeout = timeout;
            isClosing = false;
        }

        // 唯一的sessionId
        final long sessionId;
        // 协商出的超时时间
        final int timeout;
        boolean isClosing;

        Object owner;

        public long getSessionId() {
            return sessionId;
        }

        public int getTimeout() {
            return timeout;
        }

        public boolean isClosing() {
            return isClosing;
        }

        public String toString() {
            return "0x" + Long.toHexString(sessionId);
        }
    }

    /**
     * Generates an initial sessionId. High order byte is serverId, next 5
     * 5 bytes are from timestamp, and low order 2 bytes are 0s.
     * <p>
     * 生成sessionId
     */
    public static long initializeNextSession(long id) {
        long nextSid;
        nextSid = (Time.currentElapsedTime() << 24) >>> 8;
        nextSid = nextSid | (id << 56);
        if (nextSid == EphemeralType.CONTAINER_EPHEMERAL_OWNER) {
            ++nextSid;  // this is an unlikely edge case, but check it just in case
        }
        return nextSid;
    }

    private final SessionExpirer expirer;

    public SessionTrackerImpl(SessionExpirer expirer, ConcurrentMap<Long, Integer> sessionsWithTimeout, int tickTime, long serverId, ZooKeeperServerListener listener) {
        super("SessionTracker", listener);
        this.expirer = expirer;
        // SessionTrackImpl的构造是从Zookeeper server在启动时构建的。这里的expirationInterval就是我们在zoo.cfg中配置的tickTime，默认为2000
        this.sessionExpiryQueue = new ExpiryQueue<SessionImpl>(tickTime);
        this.sessionsWithTimeout = sessionsWithTimeout;
        this.nextSessionId.set(initializeNextSession(serverId));
        for (Entry<Long, Integer> e : sessionsWithTimeout.entrySet()) {
            addSession(e.getKey(), e.getValue());
        }

        EphemeralType.validateServerId(serverId);
    }

    volatile boolean running = true;

    public void dumpSessions(PrintWriter pwriter) {
        pwriter.print("Session ");
        sessionExpiryQueue.dump(pwriter);
    }

    /**
     * Returns a mapping from time to session IDs of sessions expiring at that time.
     */
    synchronized public Map<Long, Set<Long>> getSessionExpiryMap() {
        // Convert time -> sessions map to time -> session IDs map
        Map<Long, Set<SessionImpl>> expiryMap = sessionExpiryQueue.getExpiryMap();
        Map<Long, Set<Long>> sessionExpiryMap = new TreeMap<Long, Set<Long>>();
        for (Entry<Long, Set<SessionImpl>> e : expiryMap.entrySet()) {
            Set<Long> ids = new HashSet<Long>();
            sessionExpiryMap.put(e.getKey(), ids);
            for (SessionImpl s : e.getValue()) {
                ids.add(s.sessionId);
            }
        }
        return sessionExpiryMap;
    }

    @Override
    public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpSessions(pwriter);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }


    /**
     * SessionTrackerImpl是一个线程，会
     */
    @Override
    public void run() {
        try {
            while (running) {
                // 判断下一个检查过期时间点是否到了
                long waitTime = sessionExpiryQueue.getWaitTime();
                if (waitTime > 0) {
                    // 没到就休眠
                    Thread.sleep(waitTime);
                    continue;
                }
                // 清理过期 session
                for (SessionImpl s : sessionExpiryQueue.poll()) {
                    // 将会话状态设置为正在关闭
                    setSessionClosing(s.sessionId);
                    // 过期的会话桶中的会话依次构建并发起会话关闭请求  ZookeeperServer#expire  ZookeeperServer实现了SessionExpirer接口
                    // 为了使对该会话的关闭操作在整个服务端集群中都生效，ZooKeeper使用了发起会话关闭请求的方式，并立即交付给PrepRequestProcessor处理器进行处理
                     expirer.expire(s);
                }
            }
        } catch (InterruptedException e) {
            handleException(this.getName(), e);
        }
        LOG.info("SessionTrackerImpl exited loop!");
    }

    synchronized public boolean touchSession(long sessionId, int timeout) {
        SessionImpl s = sessionsById.get(sessionId);

        if (s == null) {
            logTraceTouchInvalidSession(sessionId, timeout);
            return false;
        }

        if (s.isClosing()) {
            logTraceTouchClosingSession(sessionId, timeout);
            return false;
        }

        // 更新sessionExpiryQueue中该会话的过期时间
        updateSessionExpiry(s, timeout);
        return true;
    }

    private void updateSessionExpiry(SessionImpl s, int timeout) {
        logTraceTouchSession(s.sessionId, timeout, "");
        sessionExpiryQueue.update(s, timeout);
    }

    private void logTraceTouchSession(long sessionId, int timeout, String sessionStatus) {
        if (!LOG.isTraceEnabled()) return;

        String msg = MessageFormat.format("SessionTrackerImpl --- Touch {0}session: 0x{1} with timeout {2}", sessionStatus, Long.toHexString(sessionId), Integer.toString(timeout));

        ZooTrace.logTraceMessage(LOG, ZooTrace.CLIENT_PING_TRACE_MASK, msg);
    }

    private void logTraceTouchInvalidSession(long sessionId, int timeout) {
        logTraceTouchSession(sessionId, timeout, "invalid ");
    }

    private void logTraceTouchClosingSession(long sessionId, int timeout) {
        logTraceTouchSession(sessionId, timeout, "closing ");
    }

    public int getSessionTimeout(long sessionId) {
        return sessionsWithTimeout.get(sessionId);
    }

    synchronized public void setSessionClosing(long sessionId) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Session closing: 0x" + Long.toHexString(sessionId));
        }
        SessionImpl s = sessionsById.get(sessionId);
        if (s == null) {
            return;
        }
        // 由于整个会话清理过程需要一段的时间，因此为了保证在此期间不再处理来自该客户端的新请求，
        // SessionTracker会首先将该会话的isClosing属性标记为true。这样，即使在会话清理期间接收
        // 到该客户端的新请求，也无法继续处理了
        s.isClosing = true;
    }

    synchronized public void removeSession(long sessionId) {
        LOG.debug("Removing session 0x" + Long.toHexString(sessionId));
        SessionImpl s = sessionsById.remove(sessionId);
        sessionsWithTimeout.remove(sessionId);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK, "SessionTrackerImpl --- Removing session 0x" + Long.toHexString(sessionId));
        }
        if (s != null) {
            sessionExpiryQueue.remove(s);
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        running = false;
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(), "Shutdown SessionTrackerImpl!");
        }
    }

    public long createSession(int sessionTimeout) {
        // 在创建SessionTrackerImpl时会调用initializeNextSession设置一个初始值，后面就是在这个初始值的基础上自增来分配sessionID
        long sessionId = nextSessionId.getAndIncrement();
        addSession(sessionId, sessionTimeout);
        return sessionId;
    }

    public boolean addGlobalSession(long id, int sessionTimeout) {
        return addSession(id, sessionTimeout);
    }

    public synchronized boolean addSession(long id, int sessionTimeout) {
        sessionsWithTimeout.put(id, sessionTimeout);

        boolean added = false;

        SessionImpl session = sessionsById.get(id);
        if (session == null) {
            session = new SessionImpl(id, sessionTimeout);
        }

        // findbugs2.0.3 complains about get after put.
        // long term strategy would be use computeIfAbsent after JDK 1.8
        SessionImpl existedSession = sessionsById.putIfAbsent(id, session);

        if (existedSession != null) {
            session = existedSession;
        } else {
            added = true;
            LOG.debug("Adding session 0x" + Long.toHexString(id));
        }

        if (LOG.isTraceEnabled()) {
            String actionStr = added ? "Adding" : "Existing";
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK, "SessionTrackerImpl --- " + actionStr + " session 0x" + Long.toHexString(id) + " " + sessionTimeout);
        }

        updateSessionExpiry(session, sessionTimeout);
        return added;
    }

    public boolean isTrackingSession(long sessionId) {
        return sessionsById.containsKey(sessionId);
    }

    public synchronized void checkSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException,
            KeeperException.UnknownSessionException {
        LOG.debug("Checking session 0x" + Long.toHexString(sessionId));
        SessionImpl session = sessionsById.get(sessionId);

        if (session == null) {
            throw new KeeperException.UnknownSessionException();
        }

        if (session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }

        if (session.owner == null) {
            session.owner = owner;
        } else if (session.owner != owner) {
            throw new KeeperException.SessionMovedException();
        }
    }

    synchronized public void setOwner(long id, Object owner) throws SessionExpiredException {
        SessionImpl session = sessionsById.get(id);
        if (session == null || session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }
        session.owner = owner;
    }

    public void checkGlobalSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException {
        try {
            checkSession(sessionId, owner);
        } catch (KeeperException.UnknownSessionException e) {
            throw new KeeperException.SessionExpiredException();
        }
    }
}
