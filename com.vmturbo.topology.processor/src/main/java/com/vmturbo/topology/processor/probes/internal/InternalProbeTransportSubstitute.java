package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse.getDescriptor;
import static com.vmturbo.platform.sdk.common.MediationMessage.MediationServerMessage.MediationServerMessageCase.DISCOVERYREQUEST;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.chunking.MessageChunker;
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
    InternalProbeTransportSubstitute(@Nonnull IDiscoveryProbe internalProbe) {
        this.internalProbe = Objects.requireNonNull(internalProbe);
    }

    @Override
    public void close() {
        LOGGER.debug("close");
    }

    @Override
    public void send(MediationServerMessage request) {
        if (request != null && request.getMediationServerMessageCase() == DISCOVERYREQUEST) {
            if (Objects.nonNull(clientHandler)) {
                LOGGER.info("Start discovery UDE internal probe.");
                final DiscoveryResponse response = getDiscoveryResponse();
                LOGGER.info("Discovery UDE internal probe finished.");
                sendResponse(response, request.getMessageID());
            } else {
                LOGGER.error("Internal probe`s client handler is NULL");
            }
        }
    }

    @Nonnull
    private DiscoveryResponse getDiscoveryResponse() {
        try {
            final Object account = internalProbe.getAccountDefinitionClass().newInstance();
            return internalProbe.discoverTarget(account);
        } catch (Exception e) {
            LOGGER.error("Error while discovery an internal probe {}", e.getMessage());
            return SDKUtil.createDiscoveryError(e.getMessage());
        }
    }

    private void sendResponse(@Nonnull DiscoveryResponse response, int messageId) {
        final MessageChunker<DiscoveryResponse> discoveryChunker =
                new MessageChunker<>(getDescriptor(), DiscoveryResponse::newBuilder);
        for (DiscoveryResponse chunk : discoveryChunker.chunk(response)) {
            final MediationClientMessage msgToSend =
                    MediationClientMessage.newBuilder().setDiscoveryResponse(chunk)
                            .setMessageID(messageId).build();
            clientHandler.onMessage(msgToSend);
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
