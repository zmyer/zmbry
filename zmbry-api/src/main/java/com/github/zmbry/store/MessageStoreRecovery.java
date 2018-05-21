package com.github.zmbry.store;

import java.util.List;

/**
 * @author zifeng
 *
 */
public interface MessageStoreRecovery {
    List<MessageInfo> recover(final Read read, final long startOffset, final long endOffset, final StoreKeyFactory
            factory);
}
