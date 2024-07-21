package com.study.zookeeper.case2;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @author Hliang
 * @create 2022-10-18 17:05
 */
public class DistributeLockTest {
    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        DistributeLocks lock1 = new DistributeLocks();
        DistributeLocks lock2 = new DistributeLocks();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.zkLock();
                    System.out.println("lock1，拿到了锁");
                    lock1.unZkLock();
                    System.out.println("lock1，释放了锁");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.zkLock();

                    System.out.println("lock2，拿到了锁");
                    lock2.unZkLock();
                    System.out.println("lock2，释放了锁");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }
}
