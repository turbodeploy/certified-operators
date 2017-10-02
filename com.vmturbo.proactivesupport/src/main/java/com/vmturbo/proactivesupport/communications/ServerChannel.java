package com.vmturbo.proactivesupport.communications;

import java.io.IOException;
import java.net.Socket;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The {@link ServerChannel} implements the server-side communication channel.
 */
@NotThreadSafe
public class ServerChannel implements Runnable {
    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger(ServerChannel.class);

    /**
     * The socket.
     */
    private final Socket socket_;

    /**
     * The callback.
     */
    private final ServerChannelListener listener_;

    /**
     * Constructs the server channel.
     *
     * @param socket   The socket from accept method.
     * @param listener The callback listener.
     * @throws IOException In case of an error constructing the channel.
     */
    ServerChannel(final @Nonnull Socket socket,
                  final @Nonnull ServerChannelListener listener) throws IOException {
        socket_ = Objects.requireNonNull(socket);
        listener_ = Objects.requireNonNull(listener);
    }

    /**
     * Retrieves the data
     * We send an ACK after the last message, so that we handle the situation where
     * the last frame fits within the MTU, yet the packet gets lost.
     * WE want to make sure that the sender gets notified of the failure.
     * In case we have an error, the ACK will not be sent, in which case the sender
     * will simply timeout waiting for it after the last message.
     */
    public void run() {
        try (ServerChannelImpl impl = new ServerChannelImpl(socket_)) {
            listener_.start(impl.readId());
            while (impl.hasNext()) {
                listener_.nextFrame(impl.readNext());
            }
            listener_.complete();
            impl.sendAck();
        } catch (Exception e) {
            logger_.error("The data transfer aborted", e);
            try {
                listener_.error();
            } catch (IOException eAbort) {
                logger_.error("Error aborting transfer.", e);
            }
        }
    }

    /**
     * The actual implementation of the server channel.
     */
    @NotThreadSafe
    private static class ServerChannelImpl extends AbstractChannel {

        /**
         * The has next flag.
         */
        private boolean hasNextFlag_;

        /**
         * Constructs the server channel implementation.
         *
         * @param socket The socket from accept method.
         * @throws IOException In case of an error constructing the channel.
         */
        ServerChannelImpl(final @Nonnull Socket socket) throws IOException {
            socket_ = Objects.requireNonNull(socket);
            setupSocket();
            postSetupSocket();
            hasNextFlag_ = true;
        }

        /**
         * Returns the flag indicating whether we have more data.
         *
         * @return The flag indicating whether we have more data.
         */
        private boolean hasNext() {
            return hasNextFlag_;
        }

        /**
         * Reads the frame.
         *
         * @return The frame.
         * @throws IOException In case of an error reading the frame.
         */
        private MessageFrame readFrame() throws IOException {
            // The length is always length of an entire transmission.
            int length = in_.readInt();
            if (length < 0 || length > MAX_MSG_LENGTH_RCV) {
                throw new IOException("Wrong length");
            }
            byte[] msg = new byte[length];
            in_.readFully(msg, 0, msg.length);
            return new MessageFrame(msg);
        }

        /**
         * Sends a positive acknowledgement.
         *
         * @throws IOException In case of an error sending a positive acknowledgement.
         */
        private void sendAck() throws IOException {
            out_.write(ACK_BYTE_ARRAY, 0, ACK_BYTE_ARRAY.length);
            out_.flush();
        }

        /**
         * Reads the ID.
         *
         * @return The start.
         * @throws IOException In case of an error reading the start, or out of order message.
         */
        private @Nonnull
        String readId() throws IOException {
            MessageFrame frame = readFrame();
            if (!frame.containsFlag(MessageFrame.Flag.ID) ||
                frame.containsFlag(MessageFrame.Flag.MESSAGE)) {
                throw new IOException("Out of order frame. The start is expected first");
            }
            return new String(frame.getData(), CHARSET);
        }

        /**
         * Reads the next frame.
         *
         * @return The next chunk of data.
         * @throws IOException In the case of an error reading the data.
         */
        private @Nonnull
        byte[] readNext() throws IOException {
            MessageFrame frame = readFrame();
            if (!frame.containsFlag(MessageFrame.Flag.MESSAGE) ||
                frame.containsFlag(MessageFrame.Flag.ID)) {
                throw new IOException("Out of order frame.");
            }
            hasNextFlag_ = !frame.containsFlag(MessageFrame.Flag.EOF);
            if (!hasNextFlag_) {
                sendAck();
            }
            return frame.getData();
        }

    }
}
