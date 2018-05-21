package com.github.zmbry.store;

import java.nio.channels.ReadableByteChannel;

/**
 * @author zifeng
 *
 */
public class HardDeleteInfo {
    private ReadableByteChannel hardDeleteChannel;
    private long hardDeletedMessageSize;
    private long startOffsetInMessage;
    private byte[] recoveryInfo;

    public HardDeleteInfo(ReadableByteChannel hardDeletedMessage, long hardDeletedMessageSize,
            long startOffsetInMessage,
            byte[] recoveryInfo) {
        this.hardDeleteChannel = hardDeletedMessage;
        this.hardDeletedMessageSize = hardDeletedMessageSize;
        this.startOffsetInMessage = startOffsetInMessage;
        this.recoveryInfo = recoveryInfo;
    }

    public ReadableByteChannel getHardDeleteChannel() {
        return hardDeleteChannel;
    }

    public long getHardDeletedMessageSize() {
        return hardDeletedMessageSize;
    }

    public long getStartOffsetInMessage() {
        return startOffsetInMessage;
    }

    public byte[] getRecoveryInfo() {
        return recoveryInfo;
    }
}
