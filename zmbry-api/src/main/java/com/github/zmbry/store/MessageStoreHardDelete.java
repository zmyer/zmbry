package com.github.zmbry.store;

import java.util.Iterator;
import java.util.List;

/**
 * @author zifeng
 *
 */
public interface MessageStoreHardDelete {
    Iterator<HardDeleteInfo> getHardDeleteMessages(final MessageReadSet readSet, final StoreKeyFactory factory,
            final List<byte[]> recoveryInfoList);

    MessageInfo getMessageInfo(final Read read, final long offset, final StoreKeyFactory factory);
}
