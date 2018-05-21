package com.github.zmbry.messageformat;

import com.github.zmbry.store.HardDeleteInfo;
import com.github.zmbry.store.MessageInfo;
import com.github.zmbry.store.MessageReadSet;
import com.github.zmbry.store.MessageStoreHardDelete;
import com.github.zmbry.store.Read;
import com.github.zmbry.store.StoreKeyFactory;

import java.util.Iterator;
import java.util.List;

/**
 * @author zifeng
 *
 */
public class BlobStoreHardDelete implements MessageStoreHardDelete {
    @Override
    public Iterator<HardDeleteInfo> getHardDeleteMessages(final MessageReadSet readSet, final StoreKeyFactory factory,
            final List<byte[]> recoveryInfoList) {
        return null;
    }

    @Override
    public MessageInfo getMessageInfo(final Read read, final long offset, final StoreKeyFactory factory) {
        return null;
    }
}
