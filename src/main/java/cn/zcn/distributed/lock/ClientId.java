package cn.zcn.distributed.lock;

import java.util.UUID;

public class ClientId {
    public static final String VALUE = UUID.randomUUID().toString().replace("-", "");
}
