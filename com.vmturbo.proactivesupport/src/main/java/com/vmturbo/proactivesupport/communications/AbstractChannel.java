package com.vmturbo.proactivesupport.communications;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;

/**
 * The AbstractChannel implements the common abstraction for the communication channel.
 */
abstract class AbstractChannel implements AutoCloseable {
    /**
     * The maximum message length.
     */
    static final int MAX_MSG_LENGTH_RCV = MessageFrame.MAX_MSG_LENGTH + MessageFrame.MSG_OVERHEAD;

    /**
     * The ACK value. Randomly generated once.
     * The chances of a random data being identical to this value is rather small.
     */
    static final long ACK_VALUE = 0x69cc362d5d3c7624L;

    /**
     * The ACK in byte array form ready for transmission (including the 4 bytes length).
     */
    static final byte[] ACK_BYTE_ARRAY = new byte[]{0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0};

    // Fill in the {@link #ACK_BYTE_ARRAY}.
    static {
        for (int i = 4, j = 56; i < 12; i++, j -= 8) {
            ACK_BYTE_ARRAY[i] = (byte)((ACK_VALUE >>> j) & 0xFF);
        }
    }

    /**
     * The socket.
     */
    Socket socket_;

    /**
     * The socket timeout property.
     */
    static final String SO_TIMEOUT_PROPERTY =
            "com.vmturbo.proactivesupport.communications.so_timeout";

    /**
     * The default socket timeout.
     */
    private static final int SO_TIMEOUT = 30000;

    /**
     * The input stream.
     */
    DataInputStream in_;

    /**
     * The output stream.
     */
    DataOutputStream out_;

    /**
     * The messages charset.
     */
    static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * Returns the socket timeout.
     *
     * @return The socket timeout.
     */
    static int getSoTimeout() {
        return Integer.getInteger(SO_TIMEOUT_PROPERTY, SO_TIMEOUT);
    }

    /**
     * Performs common socket setup steps.
     *
     * @throws IOException In case of an error setting up the socket.
     */
    void setupSocket() throws IOException {
        socket_.setSoLinger(true, 10);
        socket_.setSoTimeout(getSoTimeout());
        socket_.setReceiveBufferSize(MAX_MSG_LENGTH_RCV);
        socket_.setSendBufferSize(MAX_MSG_LENGTH_RCV);
    }

    /**
     * Performs common socket setup steps.
     *
     * @throws IOException In case of an error setting up the socket.
     */
    void postSetupSocket() throws IOException {
        in_ = new DataInputStream(socket_.getInputStream());
        out_ = new DataOutputStream(socket_.getOutputStream());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(in_);
        IOUtils.closeQuietly(out_);
        IOUtils.closeQuietly(socket_);
        in_ = null;
        out_ = null;
        socket_ = null;
    }
}
