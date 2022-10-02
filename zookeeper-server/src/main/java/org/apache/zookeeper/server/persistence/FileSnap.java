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

package org.apache.zookeeper.server.persistence;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.util.SerializeUtils;

/**
 * This class implements the snapshot interface.
 * it is responsible for storing, serializing
 * and deserializing the right snapshot.
 * and provides access to the snapshots.
 * <p>
 * FileSnap实现了SnapShot接口，主要用于存储、序列化、反序列化、访问相应snapshot文件
 */
public class FileSnap implements SnapShot {
    // snapshot目录文件
    File snapDir;
    // 是否已经关闭的标识
    private volatile boolean close = false;
    // 版本号
    private static final int VERSION = 2;
    // database id
    private static final long dbId = -1;
    private static final Logger LOG = LoggerFactory.getLogger(FileSnap.class);
    // 魔数，4个字节 ZKSN
    public final static int SNAP_MAGIC = ByteBuffer.wrap("ZKSN".getBytes()).getInt();

    public static final String SNAPSHOT_FILE_PREFIX = "snapshot";

    public FileSnap(File snapDir) {
        this.snapDir = snapDir;
    }

    /**
     * deserialize a data tree from the most recent snapshot
     *
     * @return the zxid of the snapshot
     */
    public long deserialize(DataTree dt, Map<Long, Integer> sessions) throws IOException {
        // we run through 100 snapshots (not all of them)
        // if we cannot get it running within 100 snapshots
        // we should  give up
        // 从dataDir目录下获取最多100个合法的snapshot文件，并且snapshot文件已经通过zxid进行降序排序
        List<File> snapList = findNValidSnapshots(100);
        if (snapList.size() == 0) {
            return -1L;
        }
        File snap = null;
        boolean foundValid = false;
        for (int i = 0, snapListSize = snapList.size(); i < snapListSize; i++) {
            snap = snapList.get(i);
            LOG.info("Reading snapshot " + snap);
            try (InputStream snapIS = new BufferedInputStream(new FileInputStream(snap)); CheckedInputStream crcIn = new CheckedInputStream(snapIS, new Adler32())) {
                InputArchive ia = BinaryInputArchive.getArchive(crcIn);
                // 反序列化
                deserialize(dt, sessions, ia);
                // 获取校验和
                long checkSum = crcIn.getChecksum().getValue();
                long val = ia.readLong("val");
                // 如果序列化时生成的校验和和反序列化时生成的校验和不一致，则抛出异常
                if (val != checkSum) {
                    throw new IOException("CRC corruption in snapshot :  " + snap);
                }
                // 找到合法的，跳出循环
                foundValid = true;
                break;
            } catch (IOException e) {
                LOG.warn("problem reading snap file " + snap, e);
            }
        }
        // 在遍历完这些snapshot文件后，仍无法找到合法的一个，则抛出异常
        if (!foundValid) {
            throw new IOException("Not able to find valid snapshots in " + snapDir);
        }
        // 从文件名中解析出zxid
        dt.lastProcessedZxid = Util.getZxidFromName(snap.getName(), SNAPSHOT_FILE_PREFIX);
        return dt.lastProcessedZxid;
    }

    /**
     * deserialize the datatree from an inputarchive
     *
     * @param dt       the datatree to be serialized into
     * @param sessions the sessions to be filled up
     * @param ia       the input archive to restore from
     * @throws IOException
     */
    public void deserialize(DataTree dt, Map<Long, Integer> sessions, InputArchive ia) throws IOException {
        FileHeader header = new FileHeader();
        // 反序列化至header
        header.deserialize(ia, "fileheader");
        // 验证魔数是否相等
        if (header.getMagic() != SNAP_MAGIC) {
            throw new IOException("mismatching magic headers " + header.getMagic() + " !=  " + FileSnap.SNAP_MAGIC);
        }
        // 反序列化至dt、sessions
        SerializeUtils.deserializeSnapshot(dt, ia, sessions);
    }

    /**
     * find the most recent snapshot in the database.
     *
     * @return the file containing the most recent snapshot
     */
    public File findMostRecentSnapshot() throws IOException {
        List<File> files = findNValidSnapshots(1);
        if (files.size() == 0) {
            return null;
        }
        return files.get(0);
    }

    /**
     * find the last (maybe) valid n snapshots. this does some
     * minor checks on the validity of the snapshots. It just
     * checks for / at the end of the snapshot. This does
     * not mean that the snapshot is truly valid but is
     * valid with a high probability. also, the most recent
     * will be first on the list.
     *
     * @param n the number of most recent snapshots
     * @return the last n snapshots (the number might be
     * less than n in case enough snapshots are not available).
     * @throws IOException
     */
    private List<File> findNValidSnapshots(int n) throws IOException {
        // 按照zxid对snapshot文件进行降序排序
        List<File> files = Util.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        for (File f : files) {
            // we should catch the exceptions
            // from the valid snapshot and continue
            // until we find a valid one
            try {
                // 验证文件是否合法
                // 在写snapshot文件时服务器宕机，此时的snapshot文件非法；非snapshot文件也非法
                if (Util.isValidSnapshot(f)) {
                    list.add(f);
                    count++;
                    if (count == n) {
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.info("invalid snapshot " + f, e);
            }
        }
        return list;
    }

    /**
     * find the last n snapshots. this does not have
     * any checks if the snapshot might be valid or not
     *
     * @param n the number of most recent snapshots
     * @return the last n snapshots
     * @throws IOException
     */
    public List<File> findNRecentSnapshots(int n) throws IOException {
        List<File> files = Util.sortDataDir(snapDir.listFiles(), SNAPSHOT_FILE_PREFIX, false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        for (File f : files) {
            if (count == n) break;
            if (Util.getZxidFromName(f.getName(), SNAPSHOT_FILE_PREFIX) != -1) {
                count++;
                list.add(f);
            }
        }
        return list;
    }

    /**
     * serialize the datatree and sessions
     *
     * 序列化dt、sessions和header
     *
     * @param dt       the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param oa       the output archive to serialize into
     * @param header   the header of this snapshot
     * @throws IOException
     */
    protected void serialize(DataTree dt, Map<Long, Integer> sessions, OutputArchive oa, FileHeader header) throws IOException {
        // this is really a programmatic error and not something that can
        // happen at runtime
        if (header == null) throw new IllegalStateException("Snapshot's not open for writing: uninitialized header");
        header.serialize(oa, "fileheader");
        SerializeUtils.serializeSnapshot(dt, oa, sessions);
    }

    /**
     * serialize the datatree and session into the file snapshot
     *
     * @param dt       the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param snapShot the file to store snapshot into
     */
    public synchronized void serialize(DataTree dt, Map<Long, Integer> sessions, File snapShot) throws IOException {
        if (!close) {
            try (OutputStream sessOS = new BufferedOutputStream(new FileOutputStream(snapShot)); CheckedOutputStream crcOut = new CheckedOutputStream(sessOS, new Adler32())) {
                // CheckedOutputStream cout = new CheckedOutputStream()
                OutputArchive oa = BinaryOutputArchive.getArchive(crcOut);
                FileHeader header = new FileHeader(SNAP_MAGIC, VERSION, dbId);
                serialize(dt, sessions, oa, header);
                // 获取校验和
                long val = crcOut.getChecksum().getValue();
                // 写入校验和
                oa.writeLong(val, "val");
                // 写入"/"
                oa.writeString("/", "path");
                // 刷新
                sessOS.flush();
            }
        }
    }

    /**
     * synchronized close just so that if serialize is in place
     * the close operation will block and will wait till serialize
     * is done and will set the close flag
     */
    @Override
    public synchronized void close() throws IOException {
        close = true;
    }

}
