package cn.zcn.distributed.lock.redis;

import java.util.UUID;

/**
 * 客户端唯一 ID。每个客户端ID都必须是唯一的，这里使用 UUID 实现。
 */
public class ClientId {
    public final String value;

    private ClientId(){
        this.value = UUID.randomUUID().toString().replace("-", "");
    }

    public String getValue() {
        return value;
    }

    public static ClientId create(){
        return new ClientId();
    }
}
