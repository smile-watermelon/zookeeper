package com.guagua.services;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author guagua
 * @date 2023/2/23 16:01
 * @describe
 */
public class DistributeClient {

    private String connectionString = "hadoop-01:2181,hadoop-02:2181,hadoop-03:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;
    private static String path = "/services";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        DistributeClient distributeClient = new DistributeClient();

        distributeClient.getConnection();

        distributeClient.create(path, "services");

        distributeClient.getChildren(path);

        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }

    /**
     * 创建临时节点
     *
     * @param path
     * @param value
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void create(String path, String value) throws InterruptedException, KeeperException {
        if (isExists(path)) {
            return;
        }
        String nodeStr = zk.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(nodeStr);
    }

    public boolean isExists(String path) throws InterruptedException, KeeperException {
        Stat exists = zk.exists(path, false);
        return exists != null;
    }

    public void getChildren(String path) throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren(path, true);
        System.out.println(children);
    }


    public void getConnection() throws IOException {
        zk = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    System.out.println("-------------");
                    getChildren(path);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        });

    }












}
