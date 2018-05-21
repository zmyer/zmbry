package com.github.zmbry.network;

import java.nio.channels.WritableByteChannel;

/**
 * @author zifeng
 *
 */
public interface Send {
    long writeTo(final WritableByteChannel channel);

    boolean isSendComplete();

    long sizeInBytes();
}
