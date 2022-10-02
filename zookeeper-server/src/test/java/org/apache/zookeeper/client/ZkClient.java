package org.apache.zookeeper.client;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ZkClient {

    private String connectString = "127.0.0.1:2181";
    private int sessionTimeout = 300000000;
    ZooKeeper zkCli = null;

    // 初始化客户端
    @Before
    public void init() throws IOException {
        zkCli = new ZooKeeper(connectString, sessionTimeout, null);
    }

    // 创建子节点
    @Test
    public void createZnode() throws KeeperException, InterruptedException {
        String path = zkCli.create("/hello", "world".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    @Test
    public void test() {
        while (true);
    }
}