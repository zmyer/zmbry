package com.github.zmbry.store;

import java.io.DataInputStream;

/**
 * @author zifeng
 *
 */
public interface FindTokenFactory {
    FindToken getFindToken(DataInputStream stream);

    FindToken getNewFindToken();
}
