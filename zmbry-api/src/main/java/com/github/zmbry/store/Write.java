package com.github.zmbry.store;

import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * @author zifeng
 *
 */
public interface Write {
    int appendFrom(ByteBuffer buffer);

    void appendFrom(ReadableByteChannel channel, long size);
}
