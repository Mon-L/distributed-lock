package cn.zcn.distributed.lock.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.PathUtils;

import java.util.List;

public class ZookeeperReadWriteLock {

    private static final String READ_LOCK_NAME = "r-lock-";
    private static final String WRITE_LOCK_NAME = "w-lock-";

    private final ZookeeperLock readLock;
    private final ZookeeperLock writeLock;

    public ZookeeperReadWriteLock(String path, CuratorFramework client) {
        PathUtils.validatePath(path);

        this.writeLock = new WriteLock(path, WRITE_LOCK_NAME, client);
        this.readLock = new ReadLock(this.writeLock, path, READ_LOCK_NAME, client);
    }

    public ZookeeperLock readLock() {
        return readLock;
    }

    public ZookeeperLock writeLock() {
        return writeLock;
    }

    private static class ReadLock extends ZookeeperLock {

        private final ZookeeperLock writeLock;

        ReadLock(ZookeeperLock writeLock, String path, String lockName, CuratorFramework client) {
            super(path, lockName, client);
            this.writeLock = writeLock;
        }

        @Override
        protected ZookeeperLock.AcquireLockResult isAcquired(String nodePath) throws Exception {
            if (writeLock.heldByCurrentThread()) {
                return new AcquireLockResult(true, null);
            }

            List<String> nodes = getSortedNodes();
            String nodeName = nodePath.substring(containerPath.length() + 1);

            int index = nodes.indexOf(nodeName);

            for (int i = 0; i < index; i++) {
                if (nodes.get(i).contains(WRITE_LOCK_NAME)) {
                    return new AcquireLockResult(false, nodes.get(i));
                }
            }

            return new AcquireLockResult(true, null);
        }
    }


    private static class WriteLock extends ZookeeperLock {

        WriteLock(String path, String lockName, CuratorFramework client) {
            super(path, lockName, client);
        }
    }
}
