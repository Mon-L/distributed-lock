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

class ZkLockImpl implements ZkLock {

    protected static class AcquireLockResult {
        private final boolean isLock;
        private final String watchedNodeName;

        protected AcquireLockResult(boolean isLock, String watchedNodeName) {
            this.isLock = isLock;
            this.watchedNodeName = watchedNodeName;
        }
    }

    private static class LockHolder {
        private final String lockPath;
        private final AtomicInteger count = new AtomicInteger(1);

        private LockHolder(String lockPath) {
            this.lockPath = lockPath;
        }
    }

    private static final String LOCK_NAME = "lock-";

    protected final String containerPath;
    private final String lockPath;
    protected final CuratorFramework client;
    private final Map<Thread, LockHolder> locks = new ConcurrentHashMap<>();

    private final Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            client.postSafeNotify(ZkLockImpl.this);
        }
    };

    protected ZkLockImpl(String path, CuratorFramework client) {
        this(path, LOCK_NAME, client);
    }

    protected ZkLockImpl(String path, String lockName, CuratorFramework client) {
        PathUtils.validatePath(path);

        this.client = client;
        this.containerPath = path;
        this.lockPath = ZKPaths.makePath(containerPath, lockName);
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

        String lockPath = acquireLock(waitTime, waitTimeUnit);
        if (lockPath != null) {
            lockHolder = new LockHolder(lockPath);
            locks.put(thread, lockHolder);
            return true;
        }

        return false;
    }

    private String acquireLock(long waitTime, TimeUnit waitTimeUnit) throws Exception {
        long startTime = System.currentTimeMillis();
        Long timeToWait = (waitTimeUnit == null) ? null : waitTimeUnit.toMillis(waitTime);

        boolean locked = false;
        boolean doClean = false;

        String nodePath = client.create().creatingParentsIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(this.lockPath);

        while (client.getState() == CuratorFrameworkState.STARTED && !locked) {
            try {
                AcquireLockResult ret = isAcquired(nodePath);

                if (ret.isLock) {
                    locked = true;
                } else {
                    String prevNodePath = ZKPaths.makePath(containerPath, ret.watchedNodeName);

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
            throw new IllegalMonitorStateException("Unexpected negative count for lock: " + lockPath);
        } else if (count == 0) {
            try {
                client.delete().guaranteed().forPath(lockHolder.lockPath);
            } catch (KeeperException.NodeExistsException ignored) {

            } finally {
                locks.remove(thread);
            }
        }
    }

    protected List<String> getSortedNodes() throws Exception {
        List<String> children = client.getChildren().forPath(containerPath);
        children.sort((o1, o2) -> getLockSequence(o1).compareTo(getLockSequence(o2)));
        return children;
    }

    protected AcquireLockResult isAcquired(String nodePath) throws Exception {
        List<String> nodes = getSortedNodes();
        String nodeName = nodePath.substring(containerPath.length() + 1);

        int index = nodes.indexOf(nodeName);

        if (index < 0) {
            throw new KeeperException.NoNodeException("Node not found: " + nodePath);
        } else if (index == 0) {
            return new AcquireLockResult(true, null);
        } else {
            return new AcquireLockResult(false, nodes.get(index - 1));
        }
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
