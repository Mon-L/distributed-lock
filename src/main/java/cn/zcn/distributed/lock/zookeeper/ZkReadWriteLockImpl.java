package cn.zcn.distributed.lock.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.PathUtils;

import java.util.List;

class ZkReadWriteLockImpl implements ZkReadWriteLock {

    private static final String READ_LOCK_NAME = "r-lock-";
    private static final String WRITE_LOCK_NAME = "w-lock-";

    private final ZkLock readLock;
    private final ZkLock writeLock;

    protected ZkReadWriteLockImpl(String path, CuratorFramework client) {
        PathUtils.validatePath(path);

        this.writeLock = new WriteLock(path, WRITE_LOCK_NAME, client);
        this.readLock = new ReadLock(this.writeLock, path, READ_LOCK_NAME, client);
    }

    @Override
    public ZkLock readLock() {
        return readLock;
    }

    @Override
    public ZkLock writeLock() {
        return writeLock;
    }

    private static class ReadLock extends ZkLockImpl {

        private final ZkLock writeLock;

        ReadLock(ZkLock writeLock, String path, String lockName, CuratorFramework client) {
            super(path, lockName, client);
            this.writeLock = writeLock;
        }

        @Override
        protected ZkLockImpl.AcquireLockResult isAcquired(String nodePath) throws Exception {
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


    private static class WriteLock extends ZkLockImpl {

        WriteLock(String path, String lockName, CuratorFramework client) {
            super(path, lockName, client);
        }
    }
}
