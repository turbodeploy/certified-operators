package com.vmturbo.topology.processor.communication;

import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;

/**
 * An interface for handlers for messages received when communicating with probes.
 * These operations are asynchronous and may be long running. A time-to-live protocol
 * has been implemented for these sorts of communications to differentiate situations
 * when operations take a long time from situations when the message recipient
 * has lost its connection to us.
 *
 * MessageHandlers are given an expiration time that is refreshed when a probe
 * responds with a keep-alive message. If a probe fails to respond with a keep-alive
 * in time, the associated handler for the probe's response is expired and removed.
 *
 * A complete message (determined by calling isComplete()) is considered to have
 * handled all the messages it expects and will return an expirationTime of 0
 * so that it will be treated as expired.
 */
public interface ExpiringMessageHandler extends ExpiringValue {
    /**
     * HandlerStatus is used to indicate whether a handler has completed handling
     * all the messages it expects to receive. It is returned by the onReceive method
     * and when a status of {@link HandlerStatus::COMPLETE} is returned, it indicates
     * the handler is done with its work and can be discarded.
     */
    enum HandlerStatus {
        IN_PROGRESS,
        COMPLETE
    }

    /**
     * Handle receiving a message.
     * This method is called from within the thread context of the transport that
     * produced the message and should be a "fast" method call that quickly returns control
     * to the caller. It should not do much more than, for example, queue the message to be
     * processed elsewhere.
     *
     * @param receivedMessage The message that was received and should now be handled.
     * @return A status to indicate whether the message handler has finished handling all
     *         the messages that it expects to receive. When it returns a status of
     *         {@link HandlerStatus::COMPLETE} the handler can be discarded.
     */
    HandlerStatus onReceive(MediationClientMessage receivedMessage);

    /**
     * Handle the case, when transport goes down (is closed), instead of receiving the awaited next
     * message. After this method is called, no further interactions with the handler are expected.
     */
    void onTransportClose();
}
