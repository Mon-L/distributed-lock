package cn.zcn.distributed.lock;

import java.util.UUID;

/**
 * 客户端唯一 ID。每个客户端ID都必须是唯一的，这里使用 UUID 实现。
 */
public class ClientId {
    public static final String VALUE = UUID.randomUUID().toString().replace("-", "");
}
