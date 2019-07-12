package com.vmturbo.topology.processor.communication;

import java.util.Collection;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;

/**
 * Interface implemented by classes that provide a way to choose a transport to communicate with
 * when multiple probe containers of the same type are running.  For example, a round robin chooser
 * would alternate messages among all possible transports in order.
 */
public interface ProbeContainerChooser {

    /**
     * Choose the next ITransport to use to communicate with a probe of a particular type.
     *
     * @param containerCollection {@link Collection} of {@link ITransport} to choose from.
     * @return {@link ITransport} to use for communicating with the probe.
     */
     ITransport<MediationServerMessage, MediationClientMessage> choose(
        Collection<ITransport<MediationServerMessage, MediationClientMessage>> containerCollection);

}
