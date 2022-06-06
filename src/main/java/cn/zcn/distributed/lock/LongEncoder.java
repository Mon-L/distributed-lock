package cn.zcn.distributed.lock;

import java.nio.ByteBuffer;

public class LongEncoder {

    public static byte[] encode(long l) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(l);
        return buffer.array();
    }
}
