package com.github.zmbry.store;

import java.util.List;

/**
 * @author zifeng
 *
 */
public class StoreInfo {
    private final MessageReadSet readSet;
    private final List<MessageInfo> messageSetInfos;

    public StoreInfo(final MessageReadSet readSet, List<MessageInfo> messageSetInfos) {
        this.readSet = readSet;
        this.messageSetInfos = messageSetInfos;
    }

    public MessageReadSet getMessageReadSet() {
        return readSet;
    }

    public List<MessageInfo> getMessageReadSetInfo() {
        return messageSetInfos;
    }
}
