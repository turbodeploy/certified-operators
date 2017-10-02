package com.vmturbo.proactivesupport.communications;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * The MessageFrame represents a message frame being used to send a data across a TCP/IP connection.
 */
public class MessageFrame {

    /**
     * The data.
     */
    private final byte[] data_;

    /**
     * The message frame's data offset within the data_ byte array.
     */
    private final int offset_;

    /**
     * The message frame's data length within the data_ byte array.
     */
    private final int length_;

    /**
     * The maximum message length of 64K.
     */
    static final int MAX_MSG_LENGTH = 0x10000;

    /**
     * The necessary message overhead to be checked against when verifying the length.
     */
    static final int MSG_OVERHEAD = 1;

    /**
     * The flags.
     */
    public enum Flag {
        ID(0),
        MESSAGE(1),
        EOF(2);

        // The bit mask.
        private byte bitmask_;

        /**
         * Constructs the Flag.
         *
         * @param index The 0-based index.
         */
        Flag(int index) {
            bitmask_ = (byte)(1 << index);
        }
    }

    /**
     * The message flag.
     */
    private byte flags_;

    /**
     * Constructs the message frame.
     *
     * @param data   The data.
     * @param offset The offset.
     * @param length The data length.
     * @param flags  The message flags.
     */
    MessageFrame(final @Nonnull byte[] data, final int offset, final int length,
                 final @Nonnull Flag... flags) {
        if (offset < 0 || length < 0 || offset + length > data.length || length > MAX_MSG_LENGTH) {
            throw new IllegalArgumentException("Invalid frame size " + data.length);
        }
        // Save the data.
        data_ = Objects.requireNonNull(data);
        offset_ = offset;
        length_ = length;
        // Set the flags
        for (Flag flag : flags) {
            flags_ |= flag.bitmask_;
        }
    }

    /**
     * Constructs the frame from the byte array message.
     * The message contains: flags, data (in this order).
     *
     * @param msg The byte array message.
     * @throws IOException In case of an error parsing data.
     */
    MessageFrame(final @Nonnull byte[] msg) throws IOException {
        if (msg.length > (MAX_MSG_LENGTH + MSG_OVERHEAD)) {
            throw new IllegalArgumentException("Invalid frame size " + msg.length);
        }
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(msg));
        flags_ = in.readByte();
        data_ = new byte[msg.length - MSG_OVERHEAD];
        offset_ = 0;
        length_ = data_.length;
        in.readFully(data_);
    }

    /**
     * Returns the data.
     *
     * @return The data.
     */
    public final byte[] getData() {
        return data_;
    }

    /**
     * Checks whether the required flag has been passed in the message.
     *
     * @param flag The flag.
     * @return {@code true} iff the flag has been passed through.
     */
    public final boolean containsFlag(final @Nonnull Flag flag) {
        return 0 != (flags_ & flag.bitmask_);
    }

    /**
     * Returns the  byte array length required for the frame serialization.
     *
     * @return The byte array length required for the frame serialization.
     */
    int getByteArrayLength() {
        return MSG_OVERHEAD + length_;
    }

    /**
     * Returns the byte array representation of the message.
     * The message contains: flags, data (in this order).
     * We are reading data raw so that we avoid additional memory allocation and copy.
     *
     * @param msg  The message.
     * @param offs The offset.
     * @return The byte array representation of the message.
     */
    byte[] toByteArray(final @Nonnull byte[] msg, final int offs) {
        if (msg.length - offs < MSG_OVERHEAD + length_) {
            throw new ArrayIndexOutOfBoundsException();
        }
        // Flags
        msg[offs] = flags_;
        // Data
        System.arraycopy(data_, offset_, msg, offs + MSG_OVERHEAD, length_);
        return msg;
    }
}
