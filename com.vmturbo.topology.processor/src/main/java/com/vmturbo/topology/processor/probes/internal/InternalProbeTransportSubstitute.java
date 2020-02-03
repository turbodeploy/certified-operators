package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage.MediationServerMessageCase.DISCOVERYREQUEST;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;

/**
 * This class should substitute a probe`s transport logic:
 * - receive a message/command to start discovery process;
 * - send back a discovery response.
 */
public class InternalProbeTransportSubstitute implements ITransport<MediationServerMessage, MediationClientMessage> {

    private static final Logger LOGGER = LogManager.getLogger();

    private final IDiscoveryProbe internalProbe;
    private EventHandler<MediationClientMessage> clientHandler;

    /**
     * Constructor.
     *
     * @param internalProbe - an internal probe, which is registered inside the Topology-Processor.
     */
    public InternalProbeTransportSubstitute(@Nonnull IDiscoveryProbe internalProbe) {
        this.internalProbe = Objects.requireNonNull(internalProbe);
    }

    @Override
    public void close() {

    }

    @Override
    public void send(MediationServerMessage serverMessage) throws CommunicationException, InterruptedException {
        if (serverMessage != null && serverMessage.getMediationServerMessageCase() == DISCOVERYREQUEST) {
            LOGGER.debug("Discovery request received");
            if (Objects.nonNull(clientHandler)) {
                final int messageId = serverMessage.getMessageID();
                final DiscoveryResponse response = getDiscoveryResponse();
                clientHandler.onMessage(MediationClientMessage
                        .newBuilder()
                        .setMessageID(messageId)
                        .setDiscoveryResponse(response)
                        .build());
            } else {
                LOGGER.error("Internal probe`s client handler is NULL");
            }
        }
    }

    @Nonnull
    private DiscoveryResponse getDiscoveryResponse() {
        try {
            final Object account = internalProbe.getAccountDefinitionClass().newInstance();
            final DiscoveryResponse response = internalProbe.discoverTarget(account);
            return response;
        } catch (Exception e) {
            LOGGER.error("Error while discovery an internal probe {}", e.getMessage());
            return SDKUtil.createDiscoveryError(e.getMessage());
        }
    }

    @Override
    public void addEventHandler(EventHandler<MediationClientMessage> handler) {
        LOGGER.debug("addEventHandler");
        this.clientHandler = handler;
    }

    @Override
    public void removeEventHandler(EventHandler<MediationClientMessage> handle) {
        LOGGER.debug("removeEventHandler");
    }

}
