package com.github.zmbry.store;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * @author zifeng
 *
 */
public interface Store {
    void start();

    StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions);

    void put(MessageWriteSet messageWriteSet);

    void delete(MessageWriteSet messageWriteSet);

    FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries);

    Set<StoreKey> findMissingKeys(List<StoreKey> keys);

    StoreStats getStoreStats();

    boolean isKeyDeleted(StoreKey key);

    long getSizeInBytes();

    void shutDown();

}
