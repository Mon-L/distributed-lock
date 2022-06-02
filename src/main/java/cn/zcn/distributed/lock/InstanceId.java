package cn.zcn.distributed.lock;

import java.util.UUID;

public class InstanceId {
    public static final String VALUE = UUID.randomUUID().toString().replace("-", "");
}
