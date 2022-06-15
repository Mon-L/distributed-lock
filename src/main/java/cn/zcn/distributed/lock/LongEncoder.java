package cn.zcn.distributed.lock;

import java.nio.ByteBuffer;

public class LongEncoder {

    /**
     * 将 long 转换为对应字节数组
     *
     * @param value 待编码的值
     * @return 字节数组
     */
    public static byte[] encode(long value) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        return buffer.array();
    }
}
