package cn.zcn.distributed.lock.zookeeper;

import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class BaseZookeeperTest {

    protected TestingServer server;

    @BeforeEach
    void baseLockTest_before() throws Exception {
        server = new TestingServer();
        server.start();

    }

    @AfterEach
    void baseLockTest_after() {
        CloseableUtils.closeQuietly(server);
    }
}
