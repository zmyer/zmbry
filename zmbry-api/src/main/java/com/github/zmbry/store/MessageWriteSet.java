package com.github.zmbry.store;;

import java.util.List;

/**
 * @author zifeng
 *
 */
public interface MessageWriteSet {
    public long writeTo(Write writeChannel);

    public List<MessageInfo> getMessageInfo();
}
