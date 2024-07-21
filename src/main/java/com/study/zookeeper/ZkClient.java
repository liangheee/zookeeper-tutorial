package com.study.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author Hliang
 * @create 2022-10-17 22:40
 */
public class ZkClient {

    // 注意在配置连接地址的时候，多个ip之间的逗号两边不能有空格
    private static String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private static int sessionTimeout = 2000;
    private static ZooKeeper zkClient;

    @Before
    public void init() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());

                // 再次监听
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/", false);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("===================");
                for (String child : children) {
                    System.out.println(child);
                }
                System.out.println("****************");
            }
        });
    }

    // 创建节点
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        zkClient.create("/atguigu","shuaige".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    // 获取子节点并监听节点变化
    // boolean watch 代表是否需要监听这个节点
              // 如果watch=true   那么就监听这个节点，并且使用我们Init方法种的默认process方法处理
              // 如果watch=false  则不监听，仅仅获取该路径下的节点名称
    // Watcher watcher 代表使用该监听器来监听，执行其中的process方法来处理
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", false);
        Thread.sleep(Integer.MAX_VALUE);
    }

    // 判断Znode节点是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/atguigu", false);
        System.out.println(stat == null ? "no exist" : "exist");
    }

}
