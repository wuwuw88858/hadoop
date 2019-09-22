package zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;

/**
 * @author zhijie
 * @date 2019-09-21 14:51
 */
public class CuratorWatchClient {

    /**
     * zk 信息
     * */
    private static  String ZK_ADRESS = "node01:8121,node02:8121,node03";

    static CuratorFramework client = null;

    public static void init() {
        //重连策略
        // @param重新连接次数10次
        //间隔5s
        RetryNTimes retryNTimes = new RetryNTimes(10, 5000);
        client = CuratorFrameworkFactory.newClient(ZK_ADRESS, retryNTimes); //初始化客户端
        client.start(); //启动客户端
    }

    /**
     * 添加监听策略
     *  /kkb 子节点下的监听策略
     *
     * */
    public static void watchZNode() {
        final String path = "/kkb";
        PathChildrenCache watcher = new PathChildrenCache(client, path, true);

        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        System.out.println("node add:" + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    case CHILD_UPDATED:
                        System.out.println("node change:" + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    case CHILD_REMOVED:
                        System.out.println("node remove:" + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                }
            }
        };
        watcher.getListenable().addListener(listener);
        try {
            watcher.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            System.out.println(" zk watcher success!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        init();
        watchZNode();
    }
}
