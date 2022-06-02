package cn.zcn.distributed.lock;

public interface DistributedLockClientBuilder {

    DistributedLockClientBuilder withConfig(Config config);

    DistributedLockClient build();
}
