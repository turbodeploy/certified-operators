package com.vmturbo.topology.processor.cost;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.AliasedOidsServiceGrpc.AliasedOidsServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.AliasedOidsServices.UploadAliasedOidsRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.mediation.hybrid.cloud.common.PropertyName;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;

/**
 * Uploads to the cost component a mapping of billingId-based OIDs to "real" entity OIDs.
 */
public class AliasedOidsUploader {

    private static final Logger logger = LogManager.getLogger();

    private final AliasedOidsServiceBlockingStub aliasedOidsService;
    private final Map<Long, SDKProbeType> activeTargetProbeTypes = new ConcurrentHashMap<>();
    private final Map<Long, SDKProbeType> billingTargetProbeTypes = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param aliasedOidsService Aliased OIDs RPC service.
     */
    public AliasedOidsUploader(final AliasedOidsServiceBlockingStub aliasedOidsService) {
        this.aliasedOidsService = aliasedOidsService;
    }

    /**
     * Specify a target for which aliased OID mapping should be uploaded.
     *
     * @param targetId The target ID.
     * @param targetProbeType The target probe's type.
     */
    public void addTargetProbeType(@Nonnull final Long targetId,
            @Nullable final SDKProbeType targetProbeType) {
        activeTargetProbeTypes.put(Objects.requireNonNull(targetId),
                Objects.requireNonNull(targetProbeType));
    }

    /**
     * Specify a billing target as a source of aliased OIDs to be mapped to "real" entity OIDs.
     *
     * @param billingTargetId The billing target ID.
     * @param probeType The billing target probe type.
     */
    public void addBillingTargetId(@Nonnull final Long billingTargetId,
            final SDKProbeType probeType) {
        billingTargetProbeTypes.put(Objects.requireNonNull(billingTargetId),
                Objects.requireNonNull(probeType));
    }

    /**
     * Remove target if no longer discovered.
     *
     * @param targetId The target ID.
     */
    public void removeTarget(@Nonnull final Long targetId) {
        activeTargetProbeTypes.remove(Objects.requireNonNull(targetId));
        billingTargetProbeTypes.remove(targetId);
    }

    /**
     * Upload to the cost component a mapping of billingId-based OIDs to "real" entity OIDs.
     */
    public void uploadOidMapping(@Nonnull final TopologyPipelineContext topologyPipelineContext,
            @Nonnull final StitchingContext stitchingContext) {
        Objects.requireNonNull(topologyPipelineContext);
        final Map<Long, Long> aliasOidsToRealOids = mapAliasOidsToRealOids(
                Objects.requireNonNull(stitchingContext));
        final TopologyInfo topologyInfo = topologyPipelineContext.getTopologyInfo();
        if (aliasOidsToRealOids.isEmpty()) {
            logger.info("No aliased OIDs to upload for topologyId [{}], creationTime [{}].",
                    topologyInfo.getTopologyId(), topologyInfo.getCreationTime());
        } else {
            sendUploadRequest(aliasOidsToRealOids, topologyInfo.getCreationTime());
        }
    }

    @Nonnull
    private Map<Long, Long> mapAliasOidsToRealOids(final StitchingContext stitchingContext) {
        final Map<String, Long> billingIdsToAliasOids = getBillingIdToAliasOidMap(stitchingContext);
        final Map<Long, Long> aliasOidsToRealOids = new HashMap<>();
        activeTargetProbeTypes.keySet().forEach(targetId -> Stream.concat(
                getEntityStreamByTypeAndTarget(stitchingContext, EntityType.DATABASE, targetId),
                getEntityStreamByTypeAndTarget(stitchingContext, EntityType.VIRTUAL_MACHINE,
                        targetId)).forEach(entity -> {
            final Optional<String> billingIdOptional = getBillingId(entity);
            if (billingIdOptional.isPresent()) {
                final String billingId = billingIdOptional.get();
                final Long aliasOid = billingIdsToAliasOids.get(billingId);
                if (null == aliasOid) {
                    logger.warn(
                            "No OID alias found for entity with real OID [{}] and billingId [{}].",
                            entity.getOid(), billingId);
                } else {
                    aliasOidsToRealOids.put(aliasOid, entity.getOid());
                }
            }
        }));
        return aliasOidsToRealOids;
    }

    @Nonnull
    private Map<String, Long> getBillingIdToAliasOidMap(final StitchingContext stitchingContext) {
        return billingTargetProbeTypes.keySet().stream()
                .map(stitchingContext::getTargetEntityLocalIdToOid)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    }

    private static Stream<TopologyStitchingEntity> getEntityStreamByTypeAndTarget(
            final StitchingContext stitchingContext, final EntityType entityType,
            final Long targetId) {
        return CollectionUtils.emptyIfNull(stitchingContext.getEntitiesByEntityTypeAndTarget()
                .getOrDefault(entityType, Collections.emptyMap())
                .get(targetId)).stream();
    }

    @Nonnull
    private Optional<String> getBillingId(final TopologyStitchingEntity entity) {
        return entity.getEntityBuilder().getEntityPropertiesList().stream()
                .filter(entityProperty -> PropertyName.BILLING_ID.equals(entityProperty.getName()))
                .map(EntityProperty::getValue)
                .findAny();
    }

    private void sendUploadRequest(final Map<Long, Long> aliasOidsToRealOids,
            final long topologyCreationTime) {
        aliasedOidsService.uploadAliasedOids(UploadAliasedOidsRequest.newBuilder()
                .putAllAliasIdToRealId(aliasOidsToRealOids)
                .setBroadcastTimeUtcMs(topologyCreationTime)
                .build());
        logger.info("Uploaded {} aliased OIDs to cost component.", aliasOidsToRealOids.size());
    }
}
