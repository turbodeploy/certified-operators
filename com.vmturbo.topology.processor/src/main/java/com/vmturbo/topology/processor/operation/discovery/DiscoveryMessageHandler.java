package com.vmturbo.topology.processor.operation.discovery;

import java.time.Clock;

import javax.annotation.Nonnull;

import com.vmturbo.communication.chunking.ChunkConfiguration;
import com.vmturbo.communication.chunking.ChunkConfigurationImpl;
import com.vmturbo.communication.chunking.ChunkReceiver;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.sdk.common.MediationMessage.MediationClientMessage;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.OperationMessageHandler;

/**
 * Handles discovery responses from probes for a {@link Discovery} operation.
 */
public class DiscoveryMessageHandler extends OperationMessageHandler<Discovery> {

    private final DiscoveryResponse.Builder discoveryBuilder;
    private final ChunkReceiver<DiscoveryResponse> chunkReceiver;
    private static final ChunkConfiguration<DiscoveryResponse, DiscoveryResponse.Builder>
            CHUNK_CONFIG;

    static {
        CHUNK_CONFIG =
                new ChunkConfigurationImpl.Builder<DiscoveryResponse, DiscoveryResponse.Builder>().addRepeatedField(
                        DiscoveryResponse::getEntityDTOList,
                        DiscoveryResponse.Builder::addAllEntityDTO)
                        .addRepeatedField(DiscoveryResponse::getErrorDTOList,
                                DiscoveryResponse.Builder::addAllErrorDTO)
                        .addRepeatedField(DiscoveryResponse::getDiscoveredGroupList,
                                DiscoveryResponse.Builder::addAllDiscoveredGroup)
                        .addRepeatedField(DiscoveryResponse::getEntityProfileList,
                                DiscoveryResponse.Builder::addAllEntityProfile)
                        .addRepeatedField(DiscoveryResponse::getDeploymentProfileList,
                                DiscoveryResponse.Builder::addAllDeploymentProfile)
                        .addRepeatedField(DiscoveryResponse::getNotificationList,
                                DiscoveryResponse.Builder::addAllNotification)
                        .addRepeatedField(DiscoveryResponse::getMetadataDTOList,
                                DiscoveryResponse.Builder::addAllMetadataDTO)
                        .addRepeatedField(DiscoveryResponse::getDerivedTargetList,
                                DiscoveryResponse.Builder::addAllDerivedTarget)
                        .addRepeatedField(DiscoveryResponse::getCostDTOList,
                                DiscoveryResponse.Builder::addAllCostDTO)
                        .addRepeatedField(DiscoveryResponse::getWorkflowList,
                            DiscoveryResponse.Builder::addAllWorkflow)
                        .addRepeatedField(DiscoveryResponse::getNonMarketEntityDTOList,
                            DiscoveryResponse.Builder::addAllNonMarketEntityDTO)
                        .addField(DiscoveryResponse::hasPriceTable,
                                DiscoveryResponse::getPriceTable,
                                DiscoveryResponse.Builder::setPriceTable)
                        .build();
    }

    public DiscoveryMessageHandler(@Nonnull final OperationManager manager,
            @Nonnull final Discovery discovery,
            @Nonnull final Clock clock,
            final long timeoutMilliseconds) {
        super(manager, discovery, clock, timeoutMilliseconds);
        this.discoveryBuilder = DiscoveryResponse.newBuilder();
        this.chunkReceiver = CHUNK_CONFIG.newMessage(discoveryBuilder);
    }

    @Override
    @Nonnull
    public HandlerStatus onMessage(@Nonnull final MediationClientMessage receivedMessage) {
        switch (receivedMessage.getMediationClientMessageCase()) {
            case DISCOVERYRESPONSE:
                chunkReceiver.processNextChunk(receivedMessage.getDiscoveryResponse());
                if (chunkReceiver.isComplete()) {
                    getLogger().debug("Successfully received last chunk for message {}",
                            receivedMessage.getMessageID());
                    manager.notifyDiscoveryResult(operation, discoveryBuilder.build());
                    return HandlerStatus.COMPLETE;
                } else {
                    getLogger().debug("Successfully received next chunk for message {}",
                            receivedMessage.getMessageID());
                    return HandlerStatus.IN_PROGRESS;
                }
            default:
                return super.onMessage(receivedMessage);
        }
    }
}
