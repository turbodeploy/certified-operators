package com.vmturbo.sample.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.sample.Echo.EchoResponse;

/**
 * The {@link EchoListener} listens for {@link EchoResponse} notifications
 * from the sample component. The client (i.e. the component that wants
 * to receive notifications from the sample component) is responsible for
 * giving an implementation of {@link EchoListener} to
 * {@link SampleComponent#addEchoListener(EchoListener)}.
 */
public interface EchoListener {

    /**
     * This function is called when a notification from the sample component comes in.
     * <p>
     * The sample component will generate this notification on the completion of every echo request.
     * Multi-echo requests will only generate one notification.
     *
     * @param echoResponse A response message identical to the one that the sample component
     *                     returned to the caller.
     */
    void onEchoResponse(@Nonnull final EchoResponse echoResponse);

}
