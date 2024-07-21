package com.study.zookeeper.case1;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author Hliang
 * @create 2022-10-18 16:03
 */
public class DistributeServer {

    private String connectString = "hadoop102:2181，hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    ZooKeeper zk;

    // 1、创建zookeeper连接
    public void getConnect() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }

    // 2、服务器上线注册
    public void register(String hostname) throws KeeperException, InterruptedException {
        String createNode = zk.create("/server/" + hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(createNode + "========");
    }

    // 3、具体的业务代码
    public void business(String hostname) throws InterruptedException {
        System.out.println(hostname + "is working......");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeServer distributeServer = new DistributeServer();
        distributeServer.getConnect();
        distributeServer.register(args[0]);
        distributeServer.business(args[0]);
    }


}
