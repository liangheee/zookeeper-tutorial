package com.study.zookeeper.case3;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author Hliang
 * @create 2022-10-18 18:28
 */
public class CuratorLockTest {
    public static void main(String[] args) {

        // 获取curator客户端
        InterProcessMutex lock1 = new InterProcessMutex(getCuratorFramework(), "/locks");
        InterProcessMutex lock2 = new InterProcessMutex(getCuratorFramework(), "/locks");


        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.acquire();
                    System.out.println("lock1获得锁");
                    lock1.acquire();
                    System.out.println("lock1再次获得锁");
                    lock1.release();
                    System.out.println("lock1第一次释放锁");
                    lock1.release();
                    System.out.println("lock1再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.acquire();
                    System.out.println("lock2获得锁");
                    lock2.acquire();
                    System.out.println("lock2再次获得锁");
                    lock2.release();
                    System.out.println("lock2第一次释放锁");
                    lock2.release();
                    System.out.println("lock2再次释放锁");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();


    }

    private static CuratorFramework getCuratorFramework() {
        // 重试策略，3秒之后，进行3次重试
        ExponentialBackoffRetry policy = new ExponentialBackoffRetry(3000, 3);
        // 通过工厂来进行创建Curator客户端
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("hadoop102:2181,hadoop103:2181,hadoop104:2181")
                .connectionTimeoutMs(2000)
                .sessionTimeoutMs(2000)
                .retryPolicy(policy).build();
        // 开启连接
        client.start();
        // 返回客户端进行相关操作
        return client;
    }
}
