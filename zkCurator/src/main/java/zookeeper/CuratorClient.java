package zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;

public class CuratorClient {

    /**
     * zookeeper 信息
     * */
    private static final String ZK_ADRESS = "node01:2181,node02:2181,node03:2181";
    private static final String ZK_PATH = "/zk_test";

    static CuratorFramework client = null;

    //初始化，创建连接
    public static void init() {
        //重连策略 共计重连10次，每次休眠5000毫秒，即5秒
        RetryNTimes retryNTimes = new RetryNTimes(10, 5000);
        /**
         * 初始化客户端
         *  @param zookeeper集群地址
         *  @param 重连策略
         * */
        client = CuratorFrameworkFactory.newClient(ZK_ADRESS, retryNTimes);
        client.start();
    }

    //关闭连接
    public static void close() {
        client.close();
        System.out.println("zk close");
    }

    /**
     * 创建永久节点
     *   client.create()            //cilent客户端创建
     .creatingParentContainersIfNeeded()    //是否需要创建父节点
     .withMode(CreateMode.PERSISTENT)   //节点类型
     .forPath("", zNodedata)     //路径 存放的节点数据（byte类型）
     * */
    public static void createPersistentNode() {
        String zNodeData = "hello1";
        try {
            client.create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath("/a/b/c", zNodeData.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建临时节点 CreateMode.EPHEMERAL
     *      临时有序节点  CreateMode.EPHEMERAL_SEQUENTIAL
     *      永久有序节点  CreateMode.PERSISTENT_SEQUENTIAL
     * */
    public static void createEphemeralNode() {
        String zNodeData = "hello2";
        try {
            client.create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)     //节点类型为临时节点
                    .forPath("/ephemeralNode", zNodeData.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *  查看节点信息
     *
     * */
    public static void queryNodeMsg() {
        String path = "/kkb";
        try {
            print("ls", path);
            print(client.getChildren().forPath(path));
            System.out.println();
            print("get", path);
            print(client.getData().forPath(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 修改节点数据
     *
     * */
    public static void modifyZNodeData() {
        String data = "world";
        String path = "/kkb";
        try {
            print("get", path); //查看信息
            print(client.getData().forPath(path));
            System.out.println();
            print("set", path, data);
            client.setData().forPath(path, data.getBytes());    //修改节点数据
            System.out.println();
            print("get", path);
            print(client.getData().forPath(path));  //在重新查看一次

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点
     * */

    public static void delZNodeData() {
        String delPath = "/kkb";
        print("delete", delPath);
        try {
            client.delete().forPath(delPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        print("ls", "/");
        try {
            client.getChildren().forPath("/");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 监听器 watch
     *  /kkb的节点监听策略
     *  如果需要监听子节点的话。需要将TreeCache -> PathChildrenCache
     *                              TreeCacheListener - > PathChildrenCacheListener
     * */
    public static void watchNode() {
        //设置节点的cacheNode
        TreeCache treeCache = new TreeCache(client, "/kkb");

        //添加监听事件
        treeCache.getListenable().addListener(new TreeCacheListener() {
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();   //获取监听事件的节点数据
                System.out.println("监听事件 data:" + data);
                if (data != null) {
                    switch (event.getType()) {
                        case NODE_ADDED:
                            System.out.println("监听事件-> node_added:" + data.getPath() + " 数据：" + new String(data.getData()));
                            break;

                        case NODE_REMOVED:
                            System.out.println("监听事件-> node_remove:" + data.getPath() + " 数据：" + new String(data.getData()));
                            break;

                        case NODE_UPDATED:
                            System.out.println("监听事件-> node_update:" + data.getPath() + " 数据：" + new String(data.getData()));
                            break;

                        default:
                            break;
                    }
                } else {
                    System.out.println("data is null");
                }

            }
        });
        try {
            treeCache.start();  //开始监听
            Thread.sleep(600000);
            treeCache.close();  //关闭监听
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws InterruptedException {
        init();
        //  createPersistentNode();
        // createEphemeralNode();
        // queryNodeMsg();
       // modifyZNodeData();
        watchNode();
        Thread.sleep(600000);
        close();
    }

    /**
     * 自定义输出方式
     * */
    private static void print(String... cmds) {
        StringBuilder text  = new StringBuilder("$ ");
        for (String cmd : cmds) {
            text.append(cmd).append(" ");
        }
        System.out.println(text.toString());
    }

    private static void print(Object result) {
        System.out.println(result instanceof byte[] ? new String((byte[]) result) : result);
    }
}
