package com.github.zmbry.network;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

/**
 * @author zifeng
 *
 */
public interface ConnectedChannel {
    void send(Send request) throws ClosedChannelException;

    ChannelOutput receive() throws IOException;

    String getRemoteHost();

    int getRemotePort();
}
