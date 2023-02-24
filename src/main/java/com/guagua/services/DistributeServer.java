package com.guagua.services;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author guagua
 * @date 2023/2/23 16:01
 * @describe
 */
public class DistributeServer {

    private String connectionString = "hadoop-01:2181,hadoop-02:2181,hadoop-03:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;
    private String path = "/services";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeServer distributeServer = new DistributeServer();

        distributeServer.getConnection();

        distributeServer.create(args[0]);

        // 处理业务逻辑
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }

    private void create(String node) throws InterruptedException, KeeperException {
        String currentNode = zk.create(path + "/" + node, node.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(currentNode + " on line");
    }

    private void getConnection() throws IOException {
        zk = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }
}
