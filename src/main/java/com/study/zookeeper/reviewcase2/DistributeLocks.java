package com.study.zookeeper.reviewcase2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author Hliang
 * @create 2023-11-21 14:16
 */
public class DistributeLocks {
    private ZooKeeper zkClient;
    private String CONNECT_STRING = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int SESSION_TIMEOUT = 2000;

    private String createNode;
    private String waitNode;
    private CountDownLatch connectLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);


    public DistributeLocks() throws IOException, InterruptedException, KeeperException {
        zkClient = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if(event.getState() == Event.KeeperState.SyncConnected){
                    connectLatch.countDown();
                    System.out.println("zk客户端连接成功！");
                }

                if(event.getType() == Event.EventType.NodeDeleted && waitNode.equals(event.getPath())){
                    waitLatch.countDown();
                    System.out.println("获取锁成功！");
                }
            }
        });

        // 阻塞等待zk客户端创建完成
        connectLatch.await();

        // 判断/locks节点是否存在，不存在则创建
        Stat stat = zkClient.exists("/locks", false);
        if(stat == null)
            zkClient.create("/locks",null, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
    }

    public void zkLock() throws KeeperException, InterruptedException {
        // 创建自己的临时带序号节点
        createNode = zkClient.create("/locks/seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // 判断自己是否是/locks目录下当前序号最小的节点
        List<String> children = zkClient.getChildren("/locks", false);

        if(children.size() == 1){
            // 如果当前只有一个节点，那就是自己，获取锁
            return;
        }

        // /locks下有多个节点，监听比自己序号小1的节点
        Collections.sort(children);
        String seqName = createNode.substring("/locks".length());
        int index = children.indexOf(seqName);
        if(index == -1){
            throw new RuntimeException("当前zk节点异常！");
        }

        if(index == 0){
            // 说明当前节点是序号最小的节点，获取到锁
            return;
        }

        waitNode = "/locks/" + children.get(index - 1);

        zkClient.getData(waitNode,true,new Stat());

        waitLatch.await();

    }

    public void unZkLock() throws KeeperException, InterruptedException {
        zkClient.delete(createNode,-1);
    }
}
