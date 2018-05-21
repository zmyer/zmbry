package com.github.zmbry.network;

import com.github.zmbry.config.SSLConfig;
import com.github.zmbry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.Channels;
import java.util.ArrayList;

/**
 * @author zifeng
 *
 */
public class SSLBlockingChannel extends BlockingChannel {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private SSLSocket mSSLSocket = null;
    private final SSLSocketFactory mSSLSocketFactory;
    private final SSLConfig mSSLConfig;

    public SSLBlockingChannel(final String host, final int port, final int connectionPoolReadBufferSizeBytes,
            final int connectionPoolWriteBufferSizeBytes, final int connectionPoolReadTimeoutMs,
            final int connectionPoolConnectTimeoutMs, final SSLSocketFactory sslSocketFactory,
            final SSLConfig sslConfig) {
        super(host, port, connectionPoolReadBufferSizeBytes, connectionPoolWriteBufferSizeBytes,
                connectionPoolReadTimeoutMs, connectionPoolConnectTimeoutMs);
        if (sslSocketFactory == null) {
            throw new IllegalArgumentException("sslSocketFactory is null when creating SSLBlockingChannel");
        }
        this.mSSLSocketFactory = sslSocketFactory;
        this.mSSLConfig = sslConfig;
    }

    @Override
    public void connect() throws IOException {
        synchronized (lock) {
            if (!isConnected()) {
                Socket socket = new Socket();
                socket.setSoTimeout(readTimeoutMs);
                socket.setKeepAlive(true);
                socket.setTcpNoDelay(true);
                if (readBufferSize > 0) {
                    socket.setReceiveBufferSize(readBufferSize);
                }
                if (writeBufferSize > 0) {
                    socket.setSendBufferSize(writeBufferSize);
                }
                socket.connect(new InetSocketAddress(host, port), connectionTimeoutMs);
                mSSLSocket = (SSLSocket) mSSLSocketFactory.createSocket(socket, host, port, true);
                ArrayList<String> protocolsList = Utils.splitString(mSSLConfig.sslEnabledProtocols, ",");
                if (protocolsList != null && protocolsList.size() > 0) {
                    String[] enabledProtocols = protocolsList.toArray(new String[protocolsList.size()]);
                    mSSLSocket.setEnabledProtocols(enabledProtocols);
                }

                ArrayList<String> cipherSuitesList = Utils.splitString(mSSLConfig.sslCipherSuites, ",");
                if (cipherSuitesList != null && cipherSuitesList.size() > 0 && !(cipherSuitesList.size() == 1
                        && cipherSuitesList.get(0).equals(""))) {
                    String[] cipherSuites = cipherSuitesList.toArray(new String[cipherSuitesList.size()]);
                    mSSLSocket.setEnabledCipherSuites(cipherSuites);
                }

                // handshake in a blocking way
                try {
                    mSSLSocket.startHandshake();
                } catch (IOException e) {
                    throw e;
                }
                writeChannel = Channels.newChannel(mSSLSocket.getOutputStream());
                readChannel = mSSLSocket.getInputStream();
                connected = true;
                logger.debug(
                        "Created socket with SO_TIMEOUT = {} (requested {}), SO_RCVBUF = {} (requested {}), SO_SNDBUF = {} (requested {})",
                        mSSLSocket.getSoTimeout(), readTimeoutMs, mSSLSocket.getReceiveBufferSize(), readBufferSize,
                        mSSLSocket.getSendBufferSize(), writeBufferSize);
            }
        }
    }

    @Override
    public void disconnect() {
        synchronized (lock) {
            try {
                if (connected || mSSLSocket != null) {
                    // closing the main socket channel *should* close the read channel
                    // but let's do it to be sure.
                    mSSLSocket.close();
                    if (readChannel != null) {
                        readChannel.close();
                        readChannel = null;
                    }
                    if (writeChannel != null) {
                        writeChannel.close();
                        writeChannel = null;
                    }
                    mSSLSocket = null;
                    connected = false;
                }
            } catch (Exception e) {
                logger.error("error while disconnecting {}", e);
            }
        }
    }
}
