package com.guagua.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

/**
 * @author guagua
 * @date 2023/2/24 16:43
 * @describe
 */
public class CuratorDemo {

    private static String connectionString = "hadoop-01:2181,hadoop-02:2181,hadoop-03:2181";
    private static int sessionTimeout = 2000;

    public static void main(String[] args) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(connectionString)
                .sessionTimeoutMs(sessionTimeout)
                .retryPolicy(retryPolicy)
                .build();

        client.start();

        InterProcessMutex lock1 = new InterProcessMutex(client, "/locks");
        InterProcessMutex lock2 = new InterProcessMutex(client, "/locks");

        new Thread(() -> {
            try {
                lock1.acquire();
                System.out.println(Thread.currentThread().getName() + " 加锁");
                TimeUnit.SECONDS.sleep(2);
                lock1.release();
                System.out.println(Thread.currentThread().getName() + " 释放锁");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                lock2.acquire();
                System.out.println(Thread.currentThread().getName() + " 加锁");
                TimeUnit.SECONDS.sleep(2);
                lock2.release();
                System.out.println(Thread.currentThread().getName() + " 释放锁");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

    }
}
