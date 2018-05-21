package com.github.zmbry.store;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * @author zifeng
 *
 */
public interface MessageReadSet {
    long writeTo(int index, WritableByteChannel channel, long relativeOffset, long maxSize) throws IOException;

    int count();

    long sizeInBytes();

    StoreKey getKeyAt(int index);
}
