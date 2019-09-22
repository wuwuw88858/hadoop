package zkLock;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.security.acl.Group;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author zhijie
 * @date 2019-09-22 13:58
 * @Description:
 *  main函数里面循环创建10个线程
 *     1 ---> 初始化DistributedLock,并且传入线程的id   new DistrubutedLock()
 *     2 ---> 连接zk集群 createConnection() 并且开始等待
 *     3 ---> 等待结束后，创建临时节点
 *     4 ---> 获取锁
 */
public class DistrubutedLock implements Watcher {

    private int threadId;   //线程数量
    private String LOG_PREFIX_OF_THREAD;    //日志
    private ZooKeeper zk = null;    //zk实例
    private static final int THREAD_NUM = 10;   //线程数量
    private String selfPath;    //自身的路径
    private String waitPath;    //没有或获取到锁 等待路径

    private static String GROUP_PATH = "/disLock"; //
    private static String SUB_PATH = "/disLock/sub";   //节点路径

    private static final String ZK_ADRESS = "node01:2181,node02:2181,node03:2181";//连接的集群信息

    /**
     * CountDownLatch是一个计数器闭锁，
     * 通过它可以完成类似于阻塞当前线程的功能，
     * 即：一个线程或多个线程一直等待，直到其他线程执行的操作完成。
     * CountDownLatch用一个给定的计数器来初始化，该计数器的操作是原子操作
     * ，即同时只能有一个线程去操作该计数器
     * 调用该类await方法的线程会一直处于阻塞状态，
     * 直到其他线程调用countDown方法使当前计数器的值变为零，
     * 每次调用countDown计数器的值减1。当计数器值减至零时，
     * 所有因调用await()方法而处于等待状态的线程就会继续往下执行。
     * */

    //确保zk连接成功
    private CountDownLatch connectedSemaphore = new CountDownLatch(1);

    //确保所有线程运行结束，semaphore信号
    private static final CountDownLatch threadSemaphore = new CountDownLatch(THREAD_NUM);

    private static final Logger LOG = Logger.getLogger(DistrubutedLock.class);

    public DistrubutedLock(int threadId) {
        this.threadId = threadId;
        LOG_PREFIX_OF_THREAD = "第" + threadId+ "个线程";
    }

    /**
     * zk 连接
     *  @param conStr 连接的集群信息
     *  @param sessionTime 连接超时时间设置
     * */
    public void createConnection(String conStr, int sessionTime) throws IOException, InterruptedException {
        zk = new ZooKeeper(conStr, sessionTime, this);
        System.out.println("1 -- >创建连接， 并且等待");
        connectedSemaphore.await();
        System.out.println("2 --> 等待结束，执行步骤3");
    }
    /**
     * 创建节点
     *  @param path 节点路径
     *  @param data 初始数据内容
     *  @param needWatch 是否需要开启watch
     * */
    public boolean createPath(String path, String data, boolean needWatch) throws KeeperException, InterruptedException {
        if (zk.exists(path, needWatch) == null) {
            LOG.warn(LOG_PREFIX_OF_THREAD + "节点创建成功， path:"
            + this.zk.create(path,
                    data.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT)
            + ", 内容为：" + data);
        }
        return true;
    }

    /**
     * 获取锁
     * */
    private void getLock() throws KeeperException, InterruptedException {
        /**
         * @param path  （节点所在路径）
         *                the path for the node
         * @param data  （初始化的节点data）
         *                the initial data for the node
         * @param acl   (权限)
         *                the acl for the node
         * @param createMode    （节点类型 有序的 永久的）
         *                specifying whether the node to be created is ephemeral
         *                and/or sequential
         *
         * */
        selfPath = zk.create(SUB_PATH,
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        LOG.info(LOG_PREFIX_OF_THREAD + "创建锁路径 - > " + selfPath);

        if (checkMinPath()) {   //判断当前线程创建的临时节点的编号是否是最小的，如果是，表示该线程可以获得锁
            getLockSuccess();
        }
    }

    /**
     * 获取锁成功
     * */
    private void getLockSuccess() throws KeeperException, InterruptedException {
        if (zk.exists(this.selfPath, false) == null) {
            LOG.error(LOG_PREFIX_OF_THREAD + "的节点不存在");
            return;
        }
        LOG.info(LOG_PREFIX_OF_THREAD + "锁成功！");
        Thread.sleep(2000); //实际上执行业务代码
        LOG.info(LOG_PREFIX_OF_THREAD + "删除本节点" + selfPath);
        //删除本节点
        zk.delete(this.selfPath, -1);
        //释放zk连接
        releaseConnection();

        //threadSemaphore 数字递减。达到0之后，然等待的先吃继续执行
        threadSemaphore.countDown();
    }

    /**
     * 释放连接
     * */
    private void releaseConnection() throws InterruptedException {
        if (zk != null) {
            this.zk.close();
        }
        LOG.info(LOG_PREFIX_OF_THREAD + " 释放连接");
    }

    /**
     *  p判断当前线程的节点编号是否是最小的
     *  如果当前线程创建的节点是最小的
     *      返回true
     *
     *  如果当前节点不是最小的
     *      找到比自己最小的，紧邻的临时节点
     *      注册监听器
     *      返回false
     * */
    private boolean checkMinPath() throws KeeperException, InterruptedException {

            //获取子节点录节目
           List<String> subNodes = zk.getChildren(GROUP_PATH, false);
            Collections.sort(subNodes); //排序

            //获取当前线程创建的临时节点，在子节点列表中排第几
            int index = subNodes.indexOf(selfPath.substring(GROUP_PATH.length() + 1));

            switch (index) {
                case -1:
                    LOG.error(LOG_PREFIX_OF_THREAD + "本节点不存在" + selfPath);

                case 0:
                    LOG.info(LOG_PREFIX_OF_THREAD + "子节点，该节点最大" + selfPath);
                    return true;

                default:
                    this.waitPath = GROUP_PATH + "/" + subNodes.get(index -1);
                    LOG.info(LOG_PREFIX_OF_THREAD + "获取子节点中，排在该线程前面的" + waitPath);
                 try {
                     zk.getData(waitPath, true, new Stat()); //注册监听器
                     return false;
                 } catch (KeeperException e) {
                     //zk.exists(waitPath, false)
                     //@return  给定路径节点的状态;如果不存在这样的节点，则返回null。
                     if (zk.exists(waitPath, false) == null) {
                         LOG.info(LOG_PREFIX_OF_THREAD + "的子节点，排在该线程前面的" + waitPath+ "不能存在");
                         return checkMinPath();
                     } else {
                         throw e;
                     }
                 }
        }

    }


    //回调函数
    public void process(WatchedEvent event) {
        if (event == null) {
            return;
        }
        Event.KeeperState keeperState = event.getState();   //状态
        Event.EventType eventType = event.getType();
        //判断客户端的连接状态 3--> 连接成功
        if (Event.KeeperState.SyncConnected == keeperState) {
            if (Event.EventType.None == eventType) {
                LOG.info(LOG_PREFIX_OF_THREAD + "连接上zk服务器");
                System.out.println("4、createConnection后，客户端成功连接zkServer");
                connectedSemaphore.countDown();
                System.out.println("5、connectedSemaphore 递减为0后，执行步骤2");
            }

            else if (Event.EventType.NodeDeleted == event.getType() && event.getPath().equals(waitPath)) {
                LOG.info(LOG_PREFIX_OF_THREAD + "收到信息，排在该线程的线程挂了，轮到该节点");
                try {
                    if (checkMinPath()) {
                        getLockSuccess();
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        else if (Event.KeeperState.Disconnected == keeperState) {
            LOG.info(LOG_PREFIX_OF_THREAD + "与zk断开连接");
        } else if (Event.KeeperState.AuthFailed == keeperState) {
            LOG.info(LOG_PREFIX_OF_THREAD + "权限检查失败");
        } else if (Event.KeeperState.Expired == keeperState) {
            LOG.info(LOG_PREFIX_OF_THREAD + "会话失败");
        }
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        for (int i = 0; i < THREAD_NUM; i++) {
            final int threadId = i + 1;
            new Thread() {
                public void run() {
                    DistrubutedLock lock = new DistrubutedLock(threadId);   //实例化线程
                    //连接zk 集群
                    try {
                        lock.createConnection(ZK_ADRESS, 10000);
                        System.out.println("3 --> 等待线程结束，继续执行");
                        synchronized (threadSemaphore) {
                            lock.createPath(GROUP_PATH, "该节点由线程" + threadId + "创建", true);

                        }
                        lock.getLock();
                    } catch (Exception e) {
                        LOG.error("第" + threadId + "个线程有异常" );
                        e.printStackTrace();
                    }
                }
            }.start();
        }
        try {
            threadSemaphore.await();
            LOG.info("线程运行结束");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
