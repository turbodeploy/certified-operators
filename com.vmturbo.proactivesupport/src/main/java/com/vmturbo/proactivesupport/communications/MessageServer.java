package com.vmturbo.proactivesupport.communications;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.annotations.VisibleForTesting;

/**
 * The MessageServer implements a listening socket.
 */
public class MessageServer implements AutoCloseable, Runnable {
    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger(MessageServer.class);

    /**
     * Backlog.
     */
    private static final int BACKLOG = 50;

    /**
     * The server socket.
     */
    @VisibleForTesting
    ServerSocket serverSocket_;

    /**
     * The callback.
     */
    private final ServerChannelListener listener_;

    /**
     * Constructs the message server.
     *
     * @param port     The listening port.
     * @param listener The callback listener.
     * @throws IOException In case of an error constructing the server.
     */
    public MessageServer(final int port, final @Nonnull ServerChannelListener listener)
            throws IOException {
        listener_ = Objects.requireNonNull(listener);
        serverSocket_ = new ServerSocket();
        serverSocket_.setSoTimeout(AbstractChannel.getSoTimeout());
        serverSocket_.setReuseAddress(true);
        serverSocket_.bind(new InetSocketAddress(port), BACKLOG);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(serverSocket_);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = serverSocket_.accept();
                Thread thread = new Thread(new ServerChannel(socket, listener_),
                                           "The processor for " +
                                           socket.getRemoteSocketAddress().toString());
                thread.start();
            } catch (SocketTimeoutException e) {
                logger_.trace("The server socket timed out. Restarting the accept cycle.");
            } catch (Exception e) {
                if (serverSocket_.isClosed()) {
                    logger_.debug("The socket accept has been closed." +
                                  " Terminating the accept loop.");
                } else {
                    logger_.error("The socket accept has failed. Terminating the accept loop.", e);
                    IOUtils.closeQuietly(serverSocket_);
                    // Make this null, so that the close() method won't fail.
                    serverSocket_ = null;
                }
                break;
            }
        }
    }
}
