package com.github.zmbry.store;

import java.io.DataInputStream;

/**
 * @author zifeng
 *
 */
public interface StoreKeyFactory {
    StoreKey getStoreKey(DataInputStream stream);
}
