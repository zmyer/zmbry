package com.github.zmbry.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;

/**
 * @author zifeng
 *
 */
public class BlockingChannel implements ConnectedChannel {
    private Logger logger = LoggerFactory.getLogger(getClass());
    protected final String host;
    protected final int port;
    protected final int readBufferSize;
    protected final int writeBufferSize;
    protected final int readTimeoutMs;
    protected final int connectionTimeoutMs;
    protected boolean connected = false;
    protected InputStream readChannel = null;
    protected WritableByteChannel writeChannel = null;
    protected Object lock = new Object();
    private SocketChannel channel = null;

    public BlockingChannel(final String host, final int port, final int readBufferSize, final int writeBufferSize,
            final int readTimeoutMs, final int connectionTimeoutMs) {
        this.host = host;
        this.port = port;
        this.readBufferSize = readBufferSize;
        this.writeBufferSize = writeBufferSize;
        this.readTimeoutMs = readTimeoutMs;
        this.connectionTimeoutMs = connectionTimeoutMs;
    }

    public void connect() throws IOException {
        synchronized (lock) {
            if (!connected) {
                channel = SocketChannel.open();
                if (readBufferSize > 0) {
                    channel.socket().setReceiveBufferSize(readBufferSize);
                }
                if (writeBufferSize > 0) {
                    channel.socket().setSendBufferSize(writeBufferSize);
                }
                channel.configureBlocking(true);
                channel.socket().setSoTimeout(readTimeoutMs);
                channel.socket().setKeepAlive(true);
                channel.socket().setTcpNoDelay(true);
                channel.socket().connect(new InetSocketAddress(host, port), connectionTimeoutMs);
                writeChannel = channel;
                readChannel = channel.socket().getInputStream();
                connected = true;
                logger.debug("Created socket with SO_TIMEOUT = {} (requested {}), "
                                + "SO_RCVBUF = {} (requested {}), SO_SNDBUF = {} (requested {})",
                        channel.socket().getSoTimeout(),
                        readTimeoutMs, channel.socket().getReceiveBufferSize(), readBufferSize,
                        channel.socket().getSendBufferSize(), writeBufferSize);
            }
        }
    }

    public void disconnect() {
        synchronized (lock) {
            try {
                if (connected || channel != null) {
                    channel.close();
                    channel.socket().close();
                    if (readChannel != null) {
                        readChannel.close();
                        readChannel = null;
                    }
                    if (writeChannel != null) {
                        writeChannel.close();
                        writeChannel = null;
                    }
                    channel = null;
                    connected = false;
                }
            } catch (Exception e) {
                logger.error("error while disconnecting {}", e);
            }
        }
    }

    public boolean isConnected() {
        return connected;
    }

    @Override
    public void send(final Send request) throws ClosedChannelException {
        if (!connected) {
            throw new ClosedChannelException();
        }
        while (!request.isSendComplete()) {
            request.writeTo(writeChannel);
        }
    }

    @Override
    public ChannelOutput receive() throws IOException {
        if (!connected) {
            throw new ClosedChannelException();
        }
        ByteBuffer streamSizeBuffer = ByteBuffer.allocate(8);
        while (streamSizeBuffer.position() < streamSizeBuffer.capacity()) {
            int read = readChannel.read();
            if (read == -1) {
                throw new IOException("Could not read complete size from readChannel ");
            }
            streamSizeBuffer.put((byte) read);
        }
        streamSizeBuffer.flip();
        return new ChannelOutput(readChannel, streamSizeBuffer.getLong() - 8);
    }

    @Override
    public String getRemoteHost() {
        return host;
    }

    @Override
    public int getRemotePort() {
        return port;
    }
}