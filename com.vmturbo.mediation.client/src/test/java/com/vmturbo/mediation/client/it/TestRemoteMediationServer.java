package com.vmturbo.mediation.client.it;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.junit.Assert;

import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.sdk.common.MediationMessage.ContainerInfo;
import com.vmturbo.platform.sdk.common.MediationMessage.InitializationContent;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.SetProperties;
import com.vmturbo.topology.processor.communication.ProbeContainerChooserImpl;
import com.vmturbo.topology.processor.communication.RemoteMediationServer;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Implementation of {@link RemoteMediationServer}, suitable for mocks.
 */
public class TestRemoteMediationServer extends RemoteMediationServer {

    private final Semaphore closeSemaphore = new Semaphore(1);
    private final Semaphore transportSemaphore = new Semaphore(0);

     /**
     * Creates a new TestRemoteMediationServer.
     * @param  probeStore contains the probes
     */
    public TestRemoteMediationServer(ProbeStore probeStore) {
        super(probeStore, null, new ProbeContainerChooserImpl(probeStore));
    }

    @Override
    protected void processContainerClose(
                    ITransport<MediationServerMessage, MediationClientMessage> endpoint) {
        super.processContainerClose(endpoint);
        getLogger().debug("Container has been unregistered");
        closeSemaphore.release();
    }

    /**
     * Await until container is unregistered from the remote mediation server.
     *
     * @throws InterruptedException if thread is interrupted
     */
    public void awaitContainerClosed() throws InterruptedException {
        getLogger().debug("Awaiting container to unregister");
        Assert.assertTrue("Failed to await container close",
                        closeSemaphore.tryAcquire(30, TimeUnit.SECONDS));
        getLogger().debug("Successfully awaited container to unregister");
    }

    @Override
    public void registerTransport(ContainerInfo containerInfo,
                    ITransport<MediationServerMessage, MediationClientMessage> serverEndpoint) {
        try {
            super.registerTransport(containerInfo, serverEndpoint);
        } finally {
            transportSemaphore.release();
        }
    }

    @Override
    public InitializationContent getInitializationContent(@Nonnull ContainerInfo containerInfo) {
        return InitializationContent.newBuilder()
                .setProbeProperties(SetProperties.newBuilder().build())
                .build();
    }

    public void awaitTransportRegistered() throws InterruptedException {
        getLogger().debug("Awaiting container to register");
        Assert.assertTrue("Transport did not appear",
                        transportSemaphore.tryAcquire(30, TimeUnit.SECONDS));
        getLogger().debug("Successfully awaited container to register");
    }

}
