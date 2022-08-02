package cn.zcn.distributed.lock.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class ZookeeperLockImpl implements ZookeeperLock {

    private static class LockHolder {
        private final String lockPath;
        private final AtomicInteger count = new AtomicInteger(1);

        private LockHolder(String lockPath) {
            this.lockPath = lockPath;
        }
    }

    private static final String LOCK_NAME = "lock-";

    private final String containerPath;
    private final String lockPath;
    protected final CuratorFramework client;
    private final Map<Thread, LockHolder> locks = new ConcurrentHashMap<>();

    private final Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            client.postSafeNotify(ZookeeperLockImpl.this);
        }
    };

    ZookeeperLockImpl(String path, CuratorFramework client) {
        PathUtils.validatePath(path);

        this.client = client;
        this.containerPath = path;
        this.lockPath = containerPath + "/" + LOCK_NAME;
    }

    @Override
    public void lock() throws Exception {
        doLock(-1, null);
    }

    @Override
    public boolean tryLock(long waitTime, TimeUnit waitTimeUnit) throws Exception {
        return doLock(waitTime, waitTimeUnit);
    }

    private boolean doLock(long waitTime, TimeUnit waitTimeUnit) throws Exception {
        Thread thread = Thread.currentThread();
        LockHolder lockHolder = locks.get(thread);

        //reenter lock
        if (lockHolder != null) {
            lockHolder.count.incrementAndGet();
            return true;
        }

        String lockPath = attemptLock(waitTime, waitTimeUnit);
        if (lockPath != null) {
            lockHolder = new LockHolder(lockPath);
            locks.put(thread, lockHolder);
            return true;
        }

        return false;
    }

    private String attemptLock(long waitTime, TimeUnit waitTimeUnit) throws Exception {
        long startTime = System.currentTimeMillis();
        Long timeToWait = (waitTimeUnit == null) ? null : waitTimeUnit.toMillis(waitTime);

        boolean locked = false;
        boolean doClean = false;

        String nodePath = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(this.lockPath);

        while (client.getState() == CuratorFrameworkState.STARTED && !locked) {
            try {
                List<String> nodes = getSortedNodes();
                String nodeName = nodePath.substring(containerPath.length() + 1);

                int index = nodes.indexOf(nodeName);

                if (index < 0) {
                    throw new KeeperException.NoNodeException("Node not found: " + nodePath);
                }

                if (index == 0) {
                    locked = true;
                } else {
                    String prevNodePath = ZKPaths.makePath(containerPath, nodes.get(index - 1));

                    synchronized (this) {
                        try {
                            client.getData().usingWatcher(watcher).forPath(prevNodePath);

                            if (timeToWait != null) {
                                timeToWait -= (System.currentTimeMillis() - startTime);
                                startTime = System.currentTimeMillis();

                                if (timeToWait <= 0) {
                                    doClean = true;
                                    break;
                                }

                                wait(timeToWait);
                            } else {
                                wait();
                            }
                        } catch (KeeperException.NoNodeException ignored) {
                        }
                    }
                }
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }

                doClean = true;
                throw e;
            } finally {
                if (doClean && nodePath != null) {
                    try {
                        client.delete().guaranteed().forPath(nodePath);
                    } catch (KeeperException.NoNodeException ignored) {
                    }
                }
            }
        }

        return locked ? nodePath : null;
    }

    @Override
    public void unlock() throws Exception {
        Thread thread = Thread.currentThread();
        LockHolder lockHolder = locks.get(thread);

        if (lockHolder == null) {
            throw new IllegalMonitorStateException("Do not has lock.");
        }

        int count = lockHolder.count.decrementAndGet();


        if (count < 0) {
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + lockPath);
        } else if (count == 0) {
            try {
                client.delete().guaranteed().forPath(lockHolder.lockPath);
            } catch (KeeperException.NodeExistsException ignored) {

            } finally {
                locks.remove(thread);
            }
        }
    }

    private List<String> getSortedNodes() throws Exception {
        List<String> children = client.getChildren().forPath(containerPath);
        children.sort((o1, o2) -> getLockSequence(o1).compareTo(getLockSequence(o2)));
        return children;
    }

    private String getLockSequence(String name) {
        int index = name.indexOf(LOCK_NAME);
        if (index >= 0) {
            index += LOCK_NAME.length();
            return index <= name.length() ? name.substring(index) : "";
        }
        return name;
    }

    @Override
    public boolean heldByCurrentThread() {
        LockHolder lockHolder = locks.get(Thread.currentThread());
        return lockHolder != null && lockHolder.count.get() > 0;
    }
}
