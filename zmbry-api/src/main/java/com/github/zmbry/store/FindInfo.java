package com.github.zmbry.store;

import java.util.List;

/**
 * @author zifeng
 *
 */
public class FindInfo {
    private final List<MessageInfo> messageEntries;
    private final FindToken findToken;

    public FindInfo(List<MessageInfo> messageEntries, FindToken findToken) {
        this.messageEntries = messageEntries;
        this.findToken = findToken;
    }

    public List<MessageInfo> getMessageEntries() {
        return messageEntries;
    }

    public FindToken getFindToken() {
        return findToken;
    }

}
