package com.guagua.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author guagua
 * @date 2023/2/24 15:25
 * @describe
 */
public class ZkLock {

    private String connectionString = "hadoop-01:2181,hadoop-02:2181,hadoop-03:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;

    private String currentNode;

    private String path = "/locks";

    private CountDownLatch connectionLatch = new CountDownLatch(1);

    private CountDownLatch waitLatch = new CountDownLatch(1);

    private String waitPath;

    public ZkLock() throws IOException, InterruptedException {
        zk = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("zk 连接成功");
                    connectionLatch.countDown();
                }
                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)) {

                    waitLatch.countDown();
                }
            }
        });

        connectionLatch.await();
        if (!isExists(path)) {
            create(path);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZkLock zkLock = new ZkLock();
        ZkLock zkLock1 = new ZkLock();


        new Thread(() -> {
            zkLock.lock();
            System.out.println(Thread.currentThread().getName() + " 加锁");
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            zkLock.unlock();
            System.out.println(Thread.currentThread().getName() + " 释放锁");

        }).start();

        new Thread(() -> {
            zkLock1.lock();
            System.out.println(Thread.currentThread().getName() + " 加锁");
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            zkLock1.unlock();
            System.out.println(Thread.currentThread().getName() + " 释放锁");

        }).start();
    }

    public void lock() {
        try {
            currentNode = createNode();

            TimeUnit.MILLISECONDS.sleep(100);

            List<String> children = zk.getChildren(path, false);

            int size = children.size();
            if (size == 1) {
                return;
            } else {
                Collections.sort(children);
                String node = currentNode.substring(path.length() + 1);
                int index = children.indexOf(node);

                if (index == -1) {
                    System.out.println("异常");
                    return;
                } else if (index == 0){ // 创建节点的顺序在前，获取锁
                    return;
                } else {
                    waitPath = path + "/" + children.get(index - 1);

                    zk.getData(waitPath, true, new Stat());

                    waitLatch.await();
                    return;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public boolean isExists(String path) {
        Stat stat = null;
        try {
            stat = zk.exists(path, false);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return stat != null;
    }

    public String create(String path) {
        String nodeStr = null;
        try {
            nodeStr = zk.create(path, path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return nodeStr;
    }

    /**
     * 创建临时锁 /locks/seq-0000000000
     *
     * @return
     * @throws InterruptedException
     * @throws KeeperException
     */
    private String createNode() throws InterruptedException, KeeperException {
        return zk.create(path + "/" + "seq-", "1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public void unlock() {
        try {
            zk.delete(currentNode, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


}
