package com.study.zookeeper.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Hliang
 * @create 2022-10-18 16:40
 */
public class DistributeLocks {
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zk;
    private String createNode;

    private CountDownLatch connectLatch = new CountDownLatch(1);
    private String waitPath;
    private CountDownLatch waitLatch = new CountDownLatch(1);

    public DistributeLocks() throws IOException, KeeperException, InterruptedException {
        // 1、创建zookeeper连接
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 建立连接时，打开latch，唤醒wait在该latch上的线程
                if(event.getState() == Event.KeeperState.SyncConnected){
                    connectLatch.countDown();
                }
                // 如果发生了删除节点事件，以及该节点是上一个节点
                if(event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)){
                    waitLatch.countDown();
                }
            }
        });

        // 阻塞该线程，等到zk连接后再执行下面的代码
        connectLatch.await();
        System.out.println(Thread.currentThread().getName() + "========");

        // 2、判断是否存在/locks目录，不存在则创建
        Stat stat = zk.exists("/locks", false);
        if(stat == null){
            zk.create("/locks","locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }


    }


    // 2、上锁
    public void zkLock() throws KeeperException, InterruptedException {
        // 创建暂时带有序号的节点
        createNode = zk.create("/locks/" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // 判断该节点是否是现在顺序号最小的节点
        List<String> children = zk.getChildren("/locks", false);
            if(children.size() == 1){
            // 只有当前这一个节点，拿到锁
            return;
        }else{
            // 有多个节点
            Collections.sort(children);
            String thisNode = createNode.substring("/locks/".length());
            int index = children.indexOf(thisNode);
            if(index == -1){
                System.out.println(Thread.currentThread().getName() + "数据异常");
                return;
            }else{
                if(index == 0){
                    // 说明当前节点是顺序号最小的节点，获得锁
                    return;
                }else{
                    // 获取比当前节点顺序号小1的节点路径
                    waitPath = "/locks/" + children.get(index-1);
                    zk.getData(waitPath, true, new Stat());

                    // 阻塞，进入等待锁装填，等待监听结束后，再执行后面的代码
                    waitLatch.await();
                    return;
                }
            }
        }
    }

    // 3、解锁
    public void unZkLock() throws KeeperException, InterruptedException {
        zk.delete(createNode ,-1);
    }
}
