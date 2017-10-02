package com.vmturbo.proactivesupport.communications;

import java.io.IOException;
import javax.annotation.Nonnull;

/**
 * The {@link ServerChannelListener} implements a callback for all the messages.
 */
public interface ServerChannelListener {
    /**
     * Handles the received start.
     *
     * @param id The received start.
     * @exception IOException In the case of an error handling the ID.
     */
    void start(final @Nonnull String id) throws IOException;

    /**
     * Handles the next received frame of data.
     *
     * @param data The next received frame of data.
     * @exception IOException In the case of an error handling the data frame.
     */
    void nextFrame(final @Nonnull byte[] data) throws IOException;

    /**
     * Handles the end of stream.
     *
     * @exception IOException In the case of an error handling the end of transmission.
     */
    void complete() throws IOException;

    /**
     * Abort and cleanup due to the transmission failure.
     *
     * @exception IOException In the case of an error handling the transmission error.
     */
    void error() throws IOException;
}
