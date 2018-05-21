package com.github.zmbry.store;

import java.nio.ByteBuffer;

/**
 * @author zifeng
 *
 */
public interface Read {
    void readInfo(final ByteBuffer buffer, final long position);
}
