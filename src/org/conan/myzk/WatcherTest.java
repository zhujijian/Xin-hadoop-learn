package org.conan.myzk;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

public class WatcherTest {

    ZooKeeper zk;
    @Before
    public void init() throws IOException, KeeperException, InterruptedException {
        zk= new ZooKeeper("ydt1:2181,ydt2:2181,ydt3:2181"
                , Integer.MAX_VALUE,new Watcher() {
            //全局监听
            public void process(WatchedEvent watchedEvent) {
                //客户端回调Watcher
                System.out.println("-----------------------------------------");
                System.out.println("connect state:" + watchedEvent.getState());
                System.out.println("event type:" + watchedEvent.getType());
                System.out.println("znode path:" + watchedEvent.getPath());
                System.out.println("-----------------------------------------");
            }
        }
        );
    }

    /**
     * exists监听事件：
     *      NodeCreated：节点创建
     *      NodeDeleted：节点删除
     *      NodeDataChanged：节点内容
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void test1() throws KeeperException, InterruptedException {
        //exists注册监听
        zk.exists("/watcher-exists", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("-----------------------------------------");
                System.out.println("connect state:" + watchedEvent.getState());
                System.out.println("event type:" + watchedEvent.getType());
                System.out.println("znode path:" + watchedEvent.getPath());
                System.out.println("-----------------------------------------");
                try {
                    zk.exists("/watcher-exists", this);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }
        });
        //不开启ACL，以持久化自动生成序列方式创建
        zk.create("/watcher-exists", "watcher-exists".getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //通过修改的事务类型操作来触发监听事件
        zk.setData("/watcher-exists", "watcher-exists2".getBytes(), -1);
        //删除节点看看能否触发监听事件
        zk.delete("/watcher-exists", -1);
    }

    /**
     * getData监听事件：
     *      NodeDeleted：节点删除
     *      NodeDataChange：节点内容发生变化
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void test2() throws IOException, KeeperException, InterruptedException {
        //不开启ACL，以持久化自动生成序列方式创建
        zk.create("/watcher-getData", "watcher-getData".getBytes()
                , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //getData注册监听
        zk.getData("/watcher-getData", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("-----------------------------------------");
                System.out.println("connect state:" + watchedEvent.getState());
                System.out.println("event type:" + watchedEvent.getType());
                System.out.println("znode path:" + watchedEvent.getPath());
                System.out.println("-----------------------------------------");
                try {
                    zk.getData("/watcher-getData", this, null);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, null);
        //通过修改的事务类型操作来触发监听事件
        zk.setData("/watcher-getData", "watcher-getData2".getBytes(), -1);
        //删除节点看看能否触发监听事件
        zk.delete("/watcher-getData", -1);
    }

    /**
     * getChildren监听事件：
     *      NodeChildrenChanged：子节点发生变化
     *      NodeDeleted：节点删除
     * @throws IOException
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void test3() throws IOException, KeeperException, InterruptedException {
        zk.create("/watcher-getChildren",null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/watcher-getChildren/watcher-getChildren01","watcher-getChildren01".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //getChildren注册监听
        zk.getChildren("/watcher-getChildren", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println("-----------------------------------------");
                System.out.println("connect state:" + watchedEvent.getState());
                System.out.println("event type:" + watchedEvent.getType());
                System.out.println("znode path:" + watchedEvent.getPath());
                System.out.println("-----------------------------------------");
                try {
                    zk.getChildren("/watcher-getChildren",this);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        zk.setData("/watcher-getChildren/watcher-getChildren01","watcher-getChildren02".getBytes(), -1);//修改子节点
        zk.delete("/watcher-getChildren/watcher-getChildren01", -1);//删除子节点
        zk.delete("/watcher-getChildren", -1);//删除根节点
    }

}
