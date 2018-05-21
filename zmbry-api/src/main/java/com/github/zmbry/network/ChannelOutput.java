package com.github.zmbry.network;

import java.io.InputStream;

/**
 * @author zifeng
 *
 */
public class ChannelOutput {
    private InputStream mInputStream;

    private long streamSize;

    public ChannelOutput(final InputStream inputStream, final long streamSize) {
        this.mInputStream = inputStream;
        this.streamSize = streamSize;
    }

    public InputStream getInputStream() {
        return mInputStream;
    }

    public long getStreamSize() {
        return streamSize;
    }
}
