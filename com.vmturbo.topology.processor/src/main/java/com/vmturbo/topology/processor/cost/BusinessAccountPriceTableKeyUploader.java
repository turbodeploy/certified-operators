package com.vmturbo.topology.processor.cost;

import java.text.MessageFormat;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Pricing.BusinessAccountPriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey.Builder;
import com.vmturbo.common.protobuf.cost.Pricing.UploadAccountPriceTableKeyRequest;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Uploads Business account to priceTable mappings discovered by the cloud probes, to the cost component.
 */
public class BusinessAccountPriceTableKeyUploader {
    private static final Logger logger = LogManager.getLogger();
    /**
     * grpc client for price service.
     */
    private final PricingServiceBlockingStub priceServiceClient;

    private final TargetStore targetStore;

    public BusinessAccountPriceTableKeyUploader(@Nonnull final PricingServiceBlockingStub priceServiceClient,
                                                @Nonnull final TargetStore targetStore) {
        this.priceServiceClient = priceServiceClient;
        this.targetStore = targetStore;
    }

    /**
     * Upload the business Account to PriceTable key mappings to cost component over rpc.
     * @param stitchingContext the current topology being broadcasted.
     * @param probeTypesForTargetId map of probeTypes associated to a target.
     *                              Used to determine ProbeType.
     */
    public void uploadAccountPriceTableKeys(final StitchingContext stitchingContext,
                                            final Map<Long, SDKProbeType> probeTypesForTargetId) {
        if (stitchingContext.getEntitiesByEntityTypeAndTarget().isEmpty()) {
            logger.debug("No entities found in current stitching context");
            return;
        }
        final Map<Long, PriceTableKey> uploadingData = Maps.newHashMap();
        try {
            stitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT)
                    .forEach(topologyStitchingEntity -> {
                        final long accountId = topologyStitchingEntity.getOid();
                        if (probeTypesForTargetId
                                .get(topologyStitchingEntity.getTargetId()) == null) {
                            logger.debug("Current target not found in probeStore");
                            return;
                        }
                        Builder priceTableKeyBuilder = PriceTableKey.newBuilder();
                        // Add the key material from the probe
                        topologyStitchingEntity.getEntityBuilder()
                                .getBusinessAccountData().getPriceTableKeysList().forEach(priceTableKey ->
                                priceTableKeyBuilder
                                        .putProbeKeyMaterial(priceTableKey.getIdentifierName().name(),
                                                priceTableKey.getIdentifierValue()));

                        // Add the pricing group to the key
                        final long targetId = topologyStitchingEntity.getTargetId();
                        final Optional<SDKProbeType> probeType =
                                targetStore.getProbeTypeForTarget(targetId);
                        final Optional<String> pricingGroup = probeType
                                .map(PricingGroupMapper::getPricingGroupForProbeType)
                                .orElseGet(() -> {
                                    logger.error("TargetID {} was not found in the targetStore.",
                                            targetId);
                                    return Optional.empty();
                                });

                        if (pricingGroup.isPresent()) {
                            priceTableKeyBuilder.setPricingGroup(pricingGroup.get());

                            try {
                                final PriceTableKey priceTableKey;
                                if (uploadingData.containsKey(accountId)) {
                                    logger.debug("PriceTableKey for accountID {} already found in uploadingData." +
                                            "Resolving by merging keys", accountId);
                                    priceTableKey = compareAndUpdatePriceTableKey(priceTableKeyBuilder.build(),
                                            uploadingData.get(accountId));
                                    logger.debug("PriceTableKey for accountID {} merged", accountId);
                                } else {
                                    priceTableKey = priceTableKeyBuilder.build();
                                }
                                uploadingData.put(accountId, priceTableKey);
                            } catch (PriceTableKeyException e) {
                                logger.error("Unable to compile PriceTableKey material.", e);
                                //remove the older controversial account as well to avoid inconsistent data.
                                uploadingData.remove(accountId);
                            }

                        } else {
                            logger.error("Unable to find pricing group for target (TargetID={}, AccountID={})",
                                    targetId, accountId);
                        }
                    });

            if (!uploadingData.isEmpty()) {
                priceServiceClient.uploadAccountPriceTableKeys(UploadAccountPriceTableKeyRequest.newBuilder()
                        .setBusinessAccountPriceTableKey(BusinessAccountPriceTableKey.newBuilder()
                                .putAllBusinessAccountPriceTableKey(uploadingData).build()).build());
            }
        } catch (NullPointerException e) {
            logger.error("Entity not found in targetStore", e);
        }
    }

    @Nonnull
    private PriceTableKey compareAndUpdatePriceTableKey(@Nonnull final PriceTableKey currentPriceTableKey,
                                                        @Nonnull final PriceTableKey previousPriceTableKey)
            throws PriceTableKeyException {
        final Builder resultPriceTableKey = PriceTableKey.newBuilder();
        if (!currentPriceTableKey.getPricingGroup().equals(previousPriceTableKey.getPricingGroup())) {
            throw new PriceTableKeyException(MessageFormat
                    .format("pricing_group did not match. current : {0} vs previous {1}",
                            currentPriceTableKey.getPricingGroup(),
                            previousPriceTableKey.getPricingGroup()));
        } else {
            resultPriceTableKey.setPricingGroup(currentPriceTableKey.getPricingGroup());
        }
        Map<String, String> currentPriceTableKeyMap = Maps.newHashMap(currentPriceTableKey
                .getProbeKeyMaterialMap());
        try {
            previousPriceTableKey.getProbeKeyMaterialMap().forEach((key, value) ->
                    currentPriceTableKeyMap.merge(key, value, (previousValue, currentValue) -> {
                        if (!previousValue.equals(currentValue)) {
                            throw new IllegalArgumentException(MessageFormat
                                    .format("Pricing material did not match: for {0}. " +
                                            "Old value : {1}, newer value : {2}",
                                            key, previousValue, currentValue));
                        } else {
                            return currentValue.isEmpty() ? previousValue : currentValue;
                        }
                    }));
        } catch (IllegalArgumentException e) {
            throw new PriceTableKeyException("Exception during calculating pricing keys", e);
        }
        return resultPriceTableKey.putAllProbeKeyMaterial(currentPriceTableKeyMap).build();
    }

    /**
     * Exception caused when priceTableKey material mismatch occurs.
     */
    private static final class PriceTableKeyException extends Exception {
        PriceTableKeyException(String message, Throwable cause) {
            super(message, cause);
        }

        PriceTableKeyException(String message) {
            super(message);
        }
    }
}