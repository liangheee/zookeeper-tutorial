package com.study.zookeeper.reviewcase2;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @author Hliang
 * @create 2023-11-21 14:16
 */
public class DistributeLockTest {

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        DistributeLocks locks1 = new DistributeLocks();
        DistributeLocks locks2 = new DistributeLocks();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    locks1.zkLock();
                    System.out.println("locks1获取锁！");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    locks1.unZkLock();
                    System.out.println("locks1释放锁！");
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
                    locks2.zkLock();
                    System.out.println("locks2获取锁！");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    locks2.unZkLock();
                    System.out.println("locks2释放锁！");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

    }
}
