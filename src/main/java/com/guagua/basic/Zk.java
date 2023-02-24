package com.guagua.basic;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author guagua
 * @date 2023/2/24 16:33
 * @describe
 */
public class Zk {

    private String connectionString = "hadoop-01:2181,hadoop-02:2181,hadoop-03:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    public Zk() throws IOException {
        zk = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    public String createNode(String path, String value) throws InterruptedException, KeeperException {
        return zk.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public static void main(String[] args) {

    }
}
