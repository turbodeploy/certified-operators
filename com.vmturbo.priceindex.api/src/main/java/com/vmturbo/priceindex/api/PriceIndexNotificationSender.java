package com.vmturbo.priceindex.api;

import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;

/**
 * Implementation of the sending side of the Price Index API.  This class provides a method to
 * send price index information over a Websocket connection.  It also receives responses, though
 * the API doesn't actually have any, so its response to all received messages is to log an
 * error and discard the message.
 */
public class PriceIndexNotificationSender extends ComponentNotificationSender<PriceIndexMessage> {

    private final IMessageSender<PriceIndexMessage> sender;
    /**
     * Constructs the backend.
     */
    public PriceIndexNotificationSender(@Nonnull IMessageSender<PriceIndexMessage> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    /**
     * Notify the counterpart about the PriceIndices for all the traders in the market.
     *
     * @param topologyInfo The {@link TopologyInfo} of the topology the price index describes.
     * @param priceIndexMessage The message to send.
     */
    public void sendPriceIndex(@Nonnull final TopologyInfo topologyInfo,
                               final PriceIndexMessage priceIndexMessage) {
        PriceIndexMessage.Builder builder = PriceIndexMessage.newBuilder();
        final PriceIndexMessage serverMessage =
                builder.addAllPayload(priceIndexMessage.getPayloadList().stream()
                    .map(p -> createPayload(
                        p.getOid(),
                        (float) p.getPriceindexCurrent(),
                        (float) p.getPriceindexProjected()))
                    .collect(Collectors.toList()))
                    .setMarketId(priceIndexMessage.getMarketId())
                    .setTopologyContextId(priceIndexMessage.getTopologyContextId())
                    .setTopologyId(topologyInfo.getTopologyId())
                    .setSourceTopologyCreationTime(topologyInfo.getCreationTime())
                    .build();
        sendMessage(sender, serverMessage);
    }

    /**
     * Creates the payload.
     *
     * @param oid         The OID.
     * @param piNow       The current Price Index.
     * @param piProjected The projected Price Index.
     * @return The price index message payload.
     */
    @Nonnull
    private PriceIndexMessagePayload createPayload(final long oid, final float piNow,
                                                                 final float piProjected) {
        return PriceIndexMessagePayload.newBuilder().setOid(oid)
            .setPriceindexCurrent(piNow)
            .setPriceindexProjected(piProjected).build();
    }

    @Override
    protected String describeMessage(@Nonnull PriceIndexMessage priceIndexMessage) {
        return PriceIndexMessage.class.getSimpleName() + "[" + priceIndexMessage.getMarketId() +
                "]";
    }
}
