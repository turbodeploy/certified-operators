package com.vmturbo.proactivesupport.communications;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.annotations.VisibleForTesting;

import static com.vmturbo.proactivesupport.communications.MessageFrame.Flag;

/**
 * The ClientChannel represents the client communication channel.
 */
@NotThreadSafe
public class ClientChannel extends AbstractChannel {
    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger(ClientChannel.class);

    /**
     * The midflight messages flags.
     */
    private static final MessageFrame.Flag[] FLAGS_MID = new MessageFrame.Flag[]{Flag.MESSAGE};

    /**
     * The last message flags.
     */
    private static final Flag[] FLAGS_LAST = new MessageFrame.Flag[]{Flag.MESSAGE, MessageFrame.Flag.EOF};

    /**
     * The target host.
     */
    private final String host_;

    /**
     * The target port.
     */
    private final int port_;

    /**
     * Constructs the client channel.
     *
     * @param host The target host.
     * @param port The target port.
     */
    public ClientChannel(final @Nonnull String host, final int port) {
        host_ = Objects.requireNonNull(host);
        port_ = port;
        if (port <= 0 || port >= 65536) {
            throw new IllegalArgumentException("The invalid target port");
        }
    }

    /**
     * Performs the connection.
     *
     * @throws IOException In case of an error connecting.
     */
    public void connect() throws IOException {
        // Perform the connection.
        socket_ = new Socket();
        socket_.bind(new InetSocketAddress(0));
        setupSocket();
        socket_.connect(new InetSocketAddress(host_, port_));
        postSetupSocket();
    }

    /**
     * Sends the data stream.
     *
     * @param id The ID.
     * @param in The input stream.
     * @throws IOException In case of an error either reading the data or sending it.
     */
    public void send(final @Nonnull String id, final @Nonnull InputStream in) throws IOException {
        byte[] data = new byte[MessageFrame.MAX_MSG_LENGTH];
        connect();
        sendId(id);
        for (int cbRead = in.read(data, 0, data.length); cbRead != -1;
             cbRead = in.read(data, 0, data.length)) {
            sendNext(data, 0, cbRead, in.available() <= 0);
        }
    }

    /**
     * Sends the ID.
     *
     * @param id The start.
     * @throws IOException In the case of an error sending the data.
     */
    void sendId(final @Nonnull String id) throws IOException {
        byte[] data = id.getBytes(CHARSET);
        MessageFrame frame = new MessageFrame(data, 0, data.length, MessageFrame.Flag.ID);
        sendFrame(frame);
    }

    /**
     * Sends the next chunk.
     *
     * @param data The data.
     * @param last The flag specifying whether this is the last one.
     * @throws IOException In the case of an error sending the data.
     */
    void sendNext(final @Nonnull byte[] data, boolean last) throws IOException {
        sendNext(data, 0, data.length, last);
    }

    /**
     * Sends the next chunk.
     *
     * @param data   The data.
     * @param offset The offset.
     * @param length The length.
     * @param last   The flag specifying whether this is the last one.
     * @throws IOException In the case of an error sending the data.
     */
    void sendNext(final @Nonnull byte[] data, final int offset, final int length,
                  boolean last) throws IOException {
        MessageFrame frame = new MessageFrame(data, offset, length, last ? FLAGS_LAST : FLAGS_MID);
        sendFrame(frame);
        if (last) {
            receiveAck();
        }
    }

    /**
     * Receives the ACK.
     *
     * @throws IOException In case of an error receiving an acknowledgement.
     */
    @VisibleForTesting
    void receiveAck() throws IOException {
        // Read the ack.
        // Read everything being sent, so that we don't leave anything in the OS buffers.
        int length = in_.readInt();
        long ack = in_.readLong();
        byte[] data = new byte[Math.max(0, length - 8)];
        in_.readFully(data, 0, data.length);
        if (length != 8 || ack != ACK_VALUE) {
            throw new IOException("Invalid ACK");
        }
    }

    /**
     * Constructs the raw message to be sent across the wire.
     *
     * @param frame The frame to construct the message from..
     * @return The message.
     */
    private byte[] constructRawMessage(final @Nonnull MessageFrame frame) {
        // int length + frame
        int frameLength = frame.getByteArrayLength();
        byte[] msg = new byte[4 + frameLength];
        frame.toByteArray(msg, 4);
        // Length
        msg[0] = (byte)((frameLength >>> 24) & 0xFF);
        msg[1] = (byte)((frameLength >>> 16) & 0xFF);
        msg[2] = (byte)((frameLength >>> 8) & 0xFF);
        msg[3] = (byte)(frameLength & 0xFF);
        return msg;
    }

    /**
     * Sends the message frame.
     *
     * @param frame The message frame.
     * @throws IOException In the case of an error sending the frame.
     */
    private void sendFrame(final @Nonnull MessageFrame frame) throws IOException {
        // We really want to send the entire message in one go, so that we don't have
        // to deal with the effects of Naggle algorithm due to the message exceeding MTU and
        // being fragmented. Or worse off, portions of the message (less than MTU) getting sent
        // and the entire transmission being delayed.
        final byte[] msg = constructRawMessage(frame);
        out_.write(msg, 0, msg.length);
        out_.flush();
    }
}
