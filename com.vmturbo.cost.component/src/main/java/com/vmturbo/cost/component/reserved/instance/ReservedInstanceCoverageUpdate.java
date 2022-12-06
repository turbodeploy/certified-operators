package com.vmturbo.cost.component.reserved.instance;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.AccountRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType;
import com.vmturbo.common.protobuf.plan.PlanProgressStatusEnum.Status;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.component.notification.CostNotificationSender;
import com.vmturbo.cost.component.reserved.instance.coverage.analysis.SupplementalCoverageAnalysis;
import com.vmturbo.cost.component.reserved.instance.coverage.analysis.SupplementalCoverageAnalysisFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * This class used to handle reserved instance coverage update. Because of reserved instance
 * coverage, it needs to get both {@link EntityRICoverageUpload} data and real time
 * topology data. It uses a {@link Cache} to store the received {@link EntityRICoverageUpload}
 * first, then, if it receives the real time topology with same topology id, it will store the data
 * into the database.
 */
public class ReservedInstanceCoverageUpdate {

    /**
     * A summary metric for duration of a RI coverage update. The update will include RI coverage
     * validation and supplemental RI coverage analysis.
     */
    private static final DataMetricSummary COVERAGE_UPDATE_METRIC_SUMMARY =
            DataMetricSummary.builder()
                    .withName("cost_ri_coverage_update_duration_seconds")
                    .withHelp("Time for an RI coverage update. Includes coverage validation, supplemental analysis, and DB update.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private final AccountRIMappingStore accountRIMappingStore;

    private final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private final ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    private final ReservedInstanceCoverageValidatorFactory reservedInstanceCoverageValidatorFactory;

    private final SupplementalCoverageAnalysisFactory supplementalCoverageAnalysisFactory;

    private final CostNotificationSender costNotificationSender;

    /**
     * This cache saves the reserved instance mappings for all RIs used on per-entity basis.
     */
    private final Cache<Long, List<EntityRICoverageUpload>> riCoverageEntityCache;
    /**
     * This cache saves the reserved instance mappings for all RIs used by all accounts.
     */
    private final Cache<Long, List<AccountRICoverageUpload>> riCoverageAccountCache;

    public ReservedInstanceCoverageUpdate(
            @Nonnull final DSLContext dsl,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
            @Nonnull final AccountRIMappingStore accountRIMappingStore,
            @Nonnull final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore,
            @Nonnull final ReservedInstanceCoverageStore reservedInstanceCoverageStore,
            @Nonnull final ReservedInstanceCoverageValidatorFactory reservedInstanceCoverageValidatorFactory,
            @Nonnull final SupplementalCoverageAnalysisFactory supplementalCoverageAnalysisFactory,
            @Nonnull final CostNotificationSender costNotificationSender,
            final long riCoverageCacheExpireMinutes) {
        this.dsl = Objects.requireNonNull(dsl);
        this.accountRIMappingStore = Objects.requireNonNull(accountRIMappingStore);
        this.entityReservedInstanceMappingStore = Objects.requireNonNull(entityReservedInstanceMappingStore);
        this.reservedInstanceUtilizationStore = Objects.requireNonNull(reservedInstanceUtilizationStore);
        this.reservedInstanceCoverageStore = Objects.requireNonNull(reservedInstanceCoverageStore);
        this.reservedInstanceCoverageValidatorFactory =
                Objects.requireNonNull(reservedInstanceCoverageValidatorFactory);
        this.supplementalCoverageAnalysisFactory =
                Objects.requireNonNull(supplementalCoverageAnalysisFactory);
        this.costNotificationSender = Objects.requireNonNull(costNotificationSender);
        this.riCoverageEntityCache = CacheBuilder.newBuilder()
                .expireAfterAccess(riCoverageCacheExpireMinutes, TimeUnit.MINUTES)
                .build();
        // The passed expiry interval is configured as
        // riCoverageCacheExpireMinutes set to default of 120 mts.
        this.riCoverageAccountCache = CacheBuilder.newBuilder()
                .expireAfterAccess(riCoverageCacheExpireMinutes, TimeUnit.MINUTES)
                .build();
    }

    /**
     * Store a list {@link EntityRICoverageUpload} into {@link Cache}.
     *
     * @param topologyId the id of topology.
     * @param entityRICoverageList a list {@link EntityRICoverageUpload}.
     */
    public void storeEntityRICoverageOnlyIntoCache(
            @Nonnull final long topologyId,
            @Nonnull final List<EntityRICoverageUpload> entityRICoverageList) {
        riCoverageEntityCache.put(topologyId, entityRICoverageList);
    }

    /**
     * Store a list {@link EntityRICoverageUpload} into {@link Cache}. This will
     * cache the reserved instance mappings.
     *
     * @param topologyId the id of topology.
     * @param accountRICoverageList a list {@link EntityRICoverageUpload}.
     */
    public void cacheAccountRICoverageData(
            @Nonnull final long topologyId,
            @Nonnull final List<AccountRICoverageUpload> accountRICoverageList) {
        // Store the account RI coverage mappings for all accounts.
        riCoverageAccountCache.put(topologyId, accountRICoverageList);
        logger.debug("Cache updated for {}. Contents: {} ", topologyId, accountRICoverageList);
    }


    /**
     * Input a real time topology map. If there are matched {@link EntityRICoverageUpload} records
     * in the cache, it will store them into reserved instance coverage table. It uses the topology id
     * to match the input real time topology with cached {@link EntityRICoverageUpload}.
     *
     * @param topologyInfo The info for the topology.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     */
    public void updateAllEntityRICoverageIntoDB(@Nonnull TopologyInfo topologyInfo,
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {

        try (DataMetricTimer timer = COVERAGE_UPDATE_METRIC_SUMMARY.startTimer()) {
            final long topologyId = topologyInfo.getTopologyId();
            persistUndiscoveredAccountRiMappings(cloudTopology, topologyId);
            final List<EntityRICoverageUpload> entityRICoverageUploads =
                    getCoverageUploadsForTopology(topologyId, cloudTopology);

            final List<ServiceEntityReservedInstanceCoverageRecord> seRICoverageRecord =
                    createServiceEntityReservedInstanceCoverageRecords(entityRICoverageUploads,
                            cloudTopology);

            final Instant topologyCreationTime = Instant.ofEpochMilli(topologyInfo.getCreationTime());
            dsl.transaction(configuration -> {
                final DSLContext transactionContext = DSL.using(configuration);
                // need to update entity reserved instance mapping first, because reserved instance
                // utilization data will use them later.
                entityReservedInstanceMappingStore.updateEntityReservedInstanceMapping(transactionContext,
                        entityRICoverageUploads);
                reservedInstanceUtilizationStore.updateReservedInstanceUtilization(
                        transactionContext,
                        topologyCreationTime);
                reservedInstanceCoverageStore.updateReservedInstanceCoverageStore(
                        transactionContext,
                        topologyCreationTime,
                        seRICoverageRecord);
            });

            sendSourceEntityRICoverageNotification(topologyInfo, Status.SUCCESS);
            riCoverageEntityCache.invalidate(topologyId);
        } catch (Exception e) {
            logger.error("Error processing RI coverage update (Topology Context ID={}, Topology ID={})",
                    topologyInfo.getTopologyContextId(), topologyInfo.getTopologyId(), e);
            sendSourceEntityRICoverageNotification(topologyInfo, Status.FAIL);
        }
    }

    private void persistUndiscoveredAccountRiMappings(final CloudTopology<TopologyEntityDTO> cloudTopology,
            final long topologyId) {
        logger.debug("riCoverageAccountCache: {}", () -> riCoverageAccountCache.asMap().toString());
        final List<AccountRICoverageUpload> accountRICoverageList =
                riCoverageAccountCache.getIfPresent(topologyId);
        if (!CollectionUtils.isEmpty(accountRICoverageList)) {
            // Persist RI coverage mappings per account.
            // Store the account RI coverage mappings only for the undiscovered accounts.

            final List<TopologyEntityDTO> allAccounts = cloudTopology
                    .getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE);
            final Set<Long> discoveredAccounts = allAccounts.stream().filter(
                    acc -> acc.hasTypeSpecificInfo()
                            && acc.getTypeSpecificInfo().hasBusinessAccount()
                            && acc.getTypeSpecificInfo().getBusinessAccount().hasAssociatedTargetId())
                    .map(TopologyEntityDTO::getOid)
                    .collect(toSet());

            final List<AccountRICoverageUpload> undiscoveredAccountCoverageList =
                    accountRICoverageList.stream()
                            .filter(coverage -> !discoveredAccounts.contains(coverage.getAccountId()))
                            .collect(toList());
            logger.debug(
                    "allAccounts: {}, discoveredAccounts: {}, undiscoveredAccountCoverageList: {}",
                    () -> allAccounts, () -> discoveredAccounts,
                    () -> undiscoveredAccountCoverageList);

            if (CollectionUtils.isNotEmpty(undiscoveredAccountCoverageList)) {
                logger.info("Persisting RI coverage for undiscovered accounts {}...",
                        undiscoveredAccountCoverageList.stream()
                                .map(AccountRICoverageUpload::getAccountId)
                                .collect(toList()));
            }
            accountRIMappingStore.updateAccountRICoverageMappings(undiscoveredAccountCoverageList);
        } else {
            logger.warn("No per-Account RI Coverage mapping records found in cache for topology {}",
                    topologyId);
        }
    }

    public void skipCoverageUpdate(@Nonnull TopologyInfo topologyInfo) {
        sendSourceEntityRICoverageNotification(topologyInfo, Status.FAIL);
        riCoverageEntityCache.invalidate(topologyInfo.getTopologyId());
    }

    /**
     * Generate a list of {@link ServiceEntityReservedInstanceCoverageRecord}, it will be stored into
     * reserved instance coverage tables.
     *
     * @param entityRICoverageList a list of {@link EntityRICoverageUpload}.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     * @return a list of {@link ServiceEntityReservedInstanceCoverageRecord}.
     */
    @VisibleForTesting
    List<ServiceEntityReservedInstanceCoverageRecord>
            createServiceEntityReservedInstanceCoverageRecords(
                    @Nonnull List<EntityRICoverageUpload> entityRICoverageList,
                    @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final Map<Long, ServiceEntityReservedInstanceCoverageRecord> seRICoverageRecords =
                entityRICoverageList.stream()
                        .collect(Collectors.toMap(EntityRICoverageUpload::getEntityId,
                                entityRICoverage ->
                                        createRecordFromEntityRICoverage(entityRICoverage, cloudTopology)));
        return createAllEntityRICoverageRecord(seRICoverageRecords, cloudTopology);
    }

    /**
     * Generate a {@link ServiceEntityReservedInstanceCoverageRecord} based on input
     * {@link EntityRICoverageUpload} and {@link TopologyEntityCloudTopology}.
     *
     * @param entityRICoverage a {@link EntityRICoverageUpload}.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     * @return a  {@link ServiceEntityReservedInstanceCoverageRecord}.
     */
    private ServiceEntityReservedInstanceCoverageRecord
        createRecordFromEntityRICoverage(
                @Nonnull final EntityRICoverageUpload entityRICoverage,
                @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        return ServiceEntityReservedInstanceCoverageRecord.newBuilder()
                .setId(entityRICoverage.getEntityId())
                .setAvailabilityZoneId(
                        cloudTopology.getConnectedAvailabilityZone(entityRICoverage.getEntityId())
                                .map(TopologyEntityDTO::getOid)
                                .orElse(0L))
                .setRegionId(
                        cloudTopology.getConnectedRegion(entityRICoverage.getEntityId())
                                .map(TopologyEntityDTO::getOid)
                                .orElse(0L))
                .setBusinessAccountId(
                        cloudTopology.getOwner(entityRICoverage.getEntityId())
                                .map(TopologyEntityDTO::getOid)
                                .orElse(0L))
                .setTotalCoupons(entityRICoverage.getTotalCouponsRequired())
                .setUsedCoupons(entityRICoverage.getCoverageList().stream()
                        .mapToDouble(EntityRICoverageUpload.Coverage::getCoveredCoupons)
                        .sum())
                .build();
    }

    /**
     * Generate a list of {@link ServiceEntityReservedInstanceCoverageRecord} based on the input
     * real time topology, note that, for now, it only consider the VM entity.
     *
     *
     * @param seRICoverageRecords a Map which key is entity id, value is
     *                            {@link ServiceEntityReservedInstanceCoverageRecord} which created
     *                            by {@link EntityRICoverageUpload}.
     * @param cloudTopology The most recent {@link CloudTopology} received from the Topology Processor.
     * @return a list of {@link ServiceEntityReservedInstanceCoverageRecord}.
     */
    private List<ServiceEntityReservedInstanceCoverageRecord> createAllEntityRICoverageRecord(
            @Nonnull final Map<Long, ServiceEntityReservedInstanceCoverageRecord> seRICoverageRecords,
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {
        final List<ServiceEntityReservedInstanceCoverageRecord> allEntityRICoverageRecords =
                cloudTopology.getEntities().values().stream()
                        // only keep VM entity for now.
                         .filter(entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                         .filter(entity -> !seRICoverageRecords.containsKey(entity.getOid()))
                         .map(entity ->
                                 ServiceEntityReservedInstanceCoverageRecord.newBuilder()
                                     .setId(entity.getOid())
                                     .setAvailabilityZoneId(
                                             cloudTopology.getConnectedAvailabilityZone(entity.getOid())
                                                     .map(TopologyEntityDTO::getOid)
                                                     .orElse(0L))
                                     .setRegionId(
                                             cloudTopology.getConnectedRegion(entity.getOid())
                                                     .map(TopologyEntityDTO::getOid)
                                                     .orElse(0L))
                                     .setBusinessAccountId(
                                             cloudTopology.getOwner(entity.getOid())
                                                     .map(TopologyEntityDTO::getOid)
                                                     .orElse(0L))
                                     .setTotalCoupons(
                                             cloudTopology.getRICoverageCapacityForEntity(
                                                     entity.getOid()))
                                     .setUsedCoupons(0.0)
                                     .build())
                .collect(Collectors.toList());
        // add all records which come from entity reserved coverage data.
        allEntityRICoverageRecords.addAll(seRICoverageRecords.values());
        return allEntityRICoverageRecords;
    }

    /**
     * Based on <code>cloudTopology</code>, updates the total coupons of each coverage instance, matching
     * the capacity to the tier covering the references entity. Currently, only virtual machine ->
     * compute tier is the only supported relationship (all others will be ignored).
     *
     * @param cloudTopology An instance of {@link CloudTopology}. The topology is expected to contain
     *                      both the direct entities of each {@link EntityRICoverageUpload} and their
     *                      corresponding compute tiers
     * @param entityRICoverageList A {@link Collection} of {@link EntityRICoverageUpload} instances
     * @return A {@link List} of copied and modified {@link EntityRICoverageUpload} instances
     */
    private List<EntityRICoverageUpload> stitchCoverageCouponCapacityToTier(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull Collection<EntityRICoverageUpload> entityRICoverageList) {

        return entityRICoverageList.stream()
                .map(entityRICoverage -> EntityRICoverageUpload.newBuilder(entityRICoverage))
                .peek(entityRICoverageBuilder -> entityRICoverageBuilder.setTotalCouponsRequired(
                        cloudTopology.getRICoverageCapacityForEntity(
                                entityRICoverageBuilder.getEntityId())))
                .map(EntityRICoverageUpload.Builder::build)
                .collect(Collectors.toList());

    }

    /**
     * Resolves the {@link EntityRICoverageUpload} records to process for the {@code topologyId}.
     * First, resolves any cached billing records. If billing records exist, they will be validated
     * through the {@link ReservedInstanceCoverageValidator}. Subsequently, the
     * {@link SupplementalCoverageAnalysis} will be invoked to add any additional RI coverage
     * through our own internal analysis.
     *
     * @param topologyId The target topology ID
     * @param cloudTopology The {@link CloudTopology} associated with {@code topologyId}. This is used
     *                      in both validating the billing RI coverage and in the analysis for supplemental
     *                      coverage
     * @return An immutable list of {@link EntityRICoverageUpload} instances. The order is inconsequential.
     */
    private List<EntityRICoverageUpload> getCoverageUploadsForTopology(
            final long topologyId,
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {

        final List<EntityRICoverageUpload> entityRICoverageList =
                Optional.ofNullable(riCoverageEntityCache.getIfPresent(topologyId))
                        // If no uploads are cached for the specific topology ID, we carry forward
                        // previously stored mappings (validating & rerunning supplemental coverage
                        // analysis). It is assumed coverage updates are only processed for the
                        // realtime topology.
                        .orElseGet(() -> {
                                logger.info("Resolving coverage uploads from DB (TopologyId={})",
                                        topologyId);
                                return entityReservedInstanceMappingStore.getRICoverageByEntity()
                                        .entrySet()
                                        .stream()
                                        .map(coverageEntry ->
                                                // Skip setting coverage capacity. That will be set
                                                // through stitchCoverageCouponCapacityToTier()
                                                EntityRICoverageUpload.newBuilder()
                                                        .setEntityId(coverageEntry.getKey())
                                                        .addAllCoverage(coverageEntry.getValue())
                                                        .build())
                                        .collect(Collectors.toList());
                        });

        final List<EntityRICoverageUpload> validUploadedCoverageEntries;
        if (!entityRICoverageList.isEmpty()) {
            // Before storing the RI coverage, validate the entries, removing any that are deemed invalid.
            // Invalid entries may occur if the entity's state has recently changed (e.g. the tier has changed or
            // it's been shutdown, but not terminated), the RI's state has recently changed (switched from ISF to
            // non-isf), or the RI has expired.
            final ReservedInstanceCoverageValidator coverageValidator =
                    reservedInstanceCoverageValidatorFactory.newValidator(cloudTopology);
            validUploadedCoverageEntries =
                    coverageValidator.validateCoverageUploads(
                            // The validator requires accurate coupon capacity to be reflected
                            // in the EntityRICoverageUpload::totalCouponsRequired field. The
                            // topology-processor uploads the capacity received from the cloud
                            // provider. However, for Azure, this value will always be 0. For AWS,
                            // it may reflect stale data (e.g. if the VM has changed its compute tier)
                            stitchCoverageCouponCapacityToTier(cloudTopology, entityRICoverageList));
        } else {
            // The coverage validator returns an immutable list. Mirror the immutability here.
            validUploadedCoverageEntries =  Collections.EMPTY_LIST;
        }

        final SupplementalCoverageAnalysis supplementalRICoverageAnalysis =
                supplementalCoverageAnalysisFactory.createCoverageAnalysis(
                        cloudTopology,
                        validUploadedCoverageEntries);
        return supplementalRICoverageAnalysis.createCoverageRecordsFromSupplementalAllocation();
    }

    /**
     * Sends a notification on source RI coverage becoming available.
     * @param topologyInfo The topology info
     */
    private void sendSourceEntityRICoverageNotification(@Nonnull final TopologyInfo topologyInfo,
                                                        @Nonnull Status status) {
        // send notification on projected topology
        try {
            costNotificationSender.sendCostNotification(
                    CostNotification.newBuilder()
                            .setStatusUpdate(StatusUpdate.newBuilder()
                                    .setType(StatusUpdateType.SOURCE_RI_COVERAGE_UPDATE)
                                    .setTopologyContextId(topologyInfo.getTopologyContextId())
                                    .setTopologyId(topologyInfo.getTopologyId())
                                    .setTopologyCreationTime(topologyInfo.getCreationTime())
                                    .setStatus(status)
                                    .setTimestamp(Instant.now().toEpochMilli())
                                    .build())
                            .build());

            logger.debug("SOURCE_RI_COVERAGE_UPDATE status notification sent successfully " +
                            "(TopologyType={}, TopologyContextId={}, TopologyId={})",
                    topologyInfo::getTopologyType,
                    topologyInfo::getTopologyContextId,
                    topologyInfo::getTopologyId);
        } catch (CommunicationException|InterruptedException e) {
            logger.error("Error in sending source entity RI coverage notification", e);
        }
    }

    /**
     * Getter to return the cached accountCoverage for undiscovered accounts.
     * @return the cached accountCoverage for undiscovered accounts.
     */
    @Nonnull
    public Cache<Long, List<AccountRICoverageUpload>> getRiCoverageAccountCache() {
        return riCoverageAccountCache;
    }
}
