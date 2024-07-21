package com.study.zookeeper.case1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hliang
 * @create 2022-10-18 16:03
 */
public class DistributeClient {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout = 2000;
    ZooKeeper zk;


    public static void main(String[] args) {
        DistributeClient distributeClient = new DistributeClient();
        try {
            // 1、与zookeeper建立连接
            distributeClient.getConnect();

            // 2、监听服务器列表 /server下面的，可以不要
//            distributeClient.listen();

            // 3、执行业务代码
            distributeClient.business();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void listen() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/server", true);

        ArrayList<String> childrenList = new ArrayList<>();

        for (String child : children) {
            byte[] data = zk.getData("/server/" + child, false, null);
            childrenList.add(new String(data));
        }

        System.out.println(childrenList);
    }

    private void business() throws InterruptedException {
        System.out.println("client is working......");
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getConnect() throws IOException {
        // 默认是异步连接zookeeper的
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    listen();
                    System.out.println("***");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }


}
