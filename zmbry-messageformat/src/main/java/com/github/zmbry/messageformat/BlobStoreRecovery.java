package com.github.zmbry.messageformat;

import com.github.zmbry.store.MessageInfo;
import com.github.zmbry.store.MessageStoreRecovery;
import com.github.zmbry.store.Read;
import com.github.zmbry.store.StoreKeyFactory;

import java.util.List;

/**
 * @author zifeng
 *
 */
public class BlobStoreRecovery implements MessageStoreRecovery {
    @Override
    public List<MessageInfo> recover(final Read read, final long startOffset, final long endOffset,
            final StoreKeyFactory factory) {
        return null;
    }
}
