package cn.zcn.distributed.lock.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.PathUtils;

import java.util.List;

class ZookeeperReadWriteLockImpl implements ZookeeperReadWriteLock {

    private static final String READ_LOCK_NAME = "r-lock-";
    private static final String WRITE_LOCK_NAME = "w-lock-";

    private final ZookeeperLock readLock;
    private final ZookeeperLock writeLock;

    protected ZookeeperReadWriteLockImpl(String path, CuratorFramework client) {
        PathUtils.validatePath(path);

        this.readLock = new ReadLock(path, READ_LOCK_NAME, client);
        this.writeLock = new WriteLock(path, WRITE_LOCK_NAME, client);
    }

    @Override
    public ZookeeperLock readLock() {
        return readLock;
    }

    @Override
    public ZookeeperLock writeLock() {
        return writeLock;
    }

    private static class ReadLock extends ZookeeperLockImpl {

        ReadLock(String path, String lockName, CuratorFramework client) {
            super(path, lockName, client);
        }

        @Override
        protected ZookeeperLockImpl.AcquireLockResult isAcquired(String nodePath) throws Exception {
            List<String> nodes = getSortedNodes();
            String nodeName = nodePath.substring(containerPath.length() + 1);

            int index = nodes.indexOf(nodeName);

            for (int i = 0; i < index; i++) {
                if (nodes.get(i).contains(WRITE_LOCK_NAME)) {
                    return new AcquireLockResult(false, nodes.get(i));
                }
            }

            System.out.printf("index=%d, name=%s\r\n", index, nodeName);
            return new AcquireLockResult(true, null);
        }
    }


    private static class WriteLock extends ZookeeperLockImpl {

        WriteLock(String path, String lockName, CuratorFramework client) {
            super(path, lockName, client);
        }
    }
}
