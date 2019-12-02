package com.vmturbo.topology.processor.actions;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion.Builder;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.CoreQuotaInfo.CoreQuotaByBusinessAccount.CoreQuotaByRegion.CoreQuotaByFamily;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintType;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.UploadActionConstraintInfoRequest;
import com.vmturbo.common.protobuf.action.ActionConstraintsServiceGrpc.ActionConstraintsServiceStub;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * The {@link ActionConstraintsUploader} is used for uploading action constraints to the action
 * orchestrator. Uploading action constraints DOES clear the latest action constraints stored in AO.
 */
public class ActionConstraintsUploader {

    private static final Logger logger = LogManager.getLogger();

    private final EntityStore entityStore;

    // This is an async stub.
    private final ActionConstraintsServiceStub actionConstraintsServiceClient;

    static final int NUMBER_OF_BUSINESS_ACCOUNT_PER_CHUNK = 20;

    /**
     * Construct an {@link ActionConstraintsUploader} which is used to upload action constraints.
     *
     * @param entityStore the {@link EntityStore} in which all entities are stored
     * @param actionConstraintsServiceClient action constraints service for uploading action constraints
     */
    ActionConstraintsUploader(
            @Nonnull final EntityStore entityStore,
            @Nonnull final ActionConstraintsServiceStub actionConstraintsServiceClient) {
        this.entityStore = Objects.requireNonNull(entityStore);
        this.actionConstraintsServiceClient = Objects.requireNonNull(actionConstraintsServiceClient);
    }

    /**
     * This method is used to upload action constraint info to action orchestrator.
     *
     * @param stitchingContext the stitching context that is used to look up action constraint info
     */
    public void uploadActionConstraintInfo(@Nonnull final StitchingContext stitchingContext) {
        final StreamObserver<UploadActionConstraintInfoRequest> requestObserver =
            actionConstraintsServiceClient.uploadActionConstraintInfo(new StreamObserver<Empty>() {
            @Override
            public void onNext(final Empty empty) {}

            @Override
            public void onError(final Throwable throwable) {}

            @Override
            public void onCompleted() {}
        });

        buildCoreQuotaInfo(stitchingContext, requestObserver);

        requestObserver.onCompleted();
    }

    /**
     * Build core quota info using stitchingContext and upload it to AO.
     *
     * @param stitchingContext the stitching context that is used to look up action constraint info
     * @param requestObserver stream observer on which to write the action constraint info
     */
    void buildCoreQuotaInfo(
            @Nonnull final StitchingContext stitchingContext,
            @Nonnull final StreamObserver<UploadActionConstraintInfoRequest> requestObserver) {

        // This map is used to store all core quota info which needs to be uploaded to AO.
        // The reason that we keep all info is that one business account id can appear in many regions.
        // We need to get all [region, family, quota] info for one business account before uploading it.
        // See ActionConstraintDTO.proto for the definition of the message and why we define it this way.
        // business account id -> region id -> CoreQuotaByRegion.Builder
        Map<Long, Map<Long, CoreQuotaByRegion.Builder>> coreQuotaInfoMap = new HashMap<>();

        // Get region entities from stitchingContext and extract core quota info.
        stitchingContext.getEntitiesOfType(EntityType.REGION)
            .filter(entity -> !entity.getEntityBuilder().getEntityPropertiesList().isEmpty())
            .forEach(entity -> updateCoreQuotaInfoMap(entity, coreQuotaInfoMap));

        // Chunk messages in order not to exceed gRPC message maximum size, which is 4MB by default.
        Iterators.partition(coreQuotaInfoMap.entrySet().iterator(), NUMBER_OF_BUSINESS_ACCOUNT_PER_CHUNK)
            .forEachRemaining(chunk -> {
                final CoreQuotaInfo.Builder coreQuotaInfoBuilder = CoreQuotaInfo.newBuilder();
                for (Map.Entry<Long, Map<Long, CoreQuotaByRegion.Builder>> entry : chunk) {
                    coreQuotaInfoBuilder.addCoreQuotaByBusinessAccount(
                        CoreQuotaByBusinessAccount.newBuilder()
                            .setBusinessAccountId(entry.getKey())
                            .addAllCoreQuotaByRegion(entry.getValue().values().stream()
                                .map(Builder::build).collect(Collectors.toList())));
                }

                requestObserver.onNext(UploadActionConstraintInfoRequest.newBuilder()
                    .addActionConstraintInfo(ActionConstraintInfo.newBuilder()
                        .setActionConstraintType(ActionConstraintType.CORE_QUOTA)
                        .setCoreQuotaInfo(coreQuotaInfoBuilder)).build());
            });
    }

    /**
     * Extract business account id, region, family and quota info from an region entity and
     * update the coreQuotaInfoMap which is used to store this information.
     *
     * @param entity the region entity which may contain core quota info
     * @param coreQuotaInfoMap the map which is used to store all core quota info
     */
    private void updateCoreQuotaInfoMap(
            @Nonnull final TopologyStitchingEntity entity,
            @Nonnull final Map<Long, Map<Long, CoreQuotaByRegion.Builder>> coreQuotaInfoMap) {
        if (entity.getEntityType() != EntityType.REGION ||
            entity.getEntityBuilder().getEntityPropertiesList().isEmpty()) {
            return;
        }

        for (EntityProperty property : entity.getEntityBuilder().getEntityPropertiesList()) {
            if (!property.getName().startsWith(StringConstants.CORE_QUOTA_PREFIX)) {
                continue;
            }

            try {
                // coreQuota should be size of 3.
                // {CORE_QUOTA_PREFIX, subscription id, family name}.
                final String[] coreQuota = property.getName().split(StringConstants.CORE_QUOTA_SEPARATOR);
                if (coreQuota.length != 3) {
                    logger.warn("Core quota info of entity {}: {} is missing {}",
                        entity.getDisplayName(), entity.getOid(), property.getName());
                    continue;
                }
                final String family = coreQuota[2];
                final int quota = Integer.valueOf(property.getValue());

                Optional<Long> businessAccountId =
                    entityStore.getTargetEntityIdMap(entity.getTargetId()).map(localIdToEntityId ->
                        // Get business account id from subscription id.
                        // TODO: Is there a better way to get the business account id?
                        //  The format of the subscription id in localIdToEntityId can change.
                        //  It was changed from BUSINESS_ACCOUNT::subscriptionId to subscriptionId.
                        //  If it is changed by someone without modifying the format here, then
                        //  we can't populate the coreQuotaInfoMap.
                        localIdToEntityId.get(coreQuota[1]));
                if (businessAccountId.isPresent()) {
                    final CoreQuotaByRegion.Builder builder = coreQuotaInfoMap
                        .computeIfAbsent(businessAccountId.get(), key -> new HashMap<>())
                        .computeIfAbsent(entity.getOid(), key ->
                            CoreQuotaByRegion.newBuilder().setRegionId(entity.getOid()));

                    if (StringConstants.TOTAL_CORE_QUOTA.equals(family)) {
                        builder.setTotalCoreQuota(quota);
                    } else {
                        builder.addCoreQuotaByFamily(
                            CoreQuotaByFamily.newBuilder().setFamily(family).setQuota(quota));
                    }
                } else {
                    logger.warn("Subscription id {} not found", coreQuota[1]);
                }
            } catch (RuntimeException e) {
                logger.error("Error in processing entity property {} of entity {}: {}",
                    property, entity.getOid(), e.getStackTrace());
            }
        }
    }
}