package com.vmturbo.components.api.server;

import com.google.protobuf.Empty;

import com.vmturbo.communication.ITransport;

/**
 * Interface to reflect byte transport-aware objects. These objects should receive all the
 * incoming byte-transports.
 *
 * @param <T> ype of message to send using the transports.
 */
public interface IByteTransferNotificationSender<T> {
    /**
     * Method to be called, when new transport appears.
     * @param transport transport that has appeared
     */
    void addTransport(ITransport<T, Empty> transport);
}
