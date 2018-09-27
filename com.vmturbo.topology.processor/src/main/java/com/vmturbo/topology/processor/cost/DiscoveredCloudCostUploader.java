package com.vmturbo.topology.processor.cost;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.TierExpenses;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIAndExpenseDataRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIAndExpenseDataResponse;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData;
import com.vmturbo.platform.common.dto.NonMarketDTO.CostDataDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.CloudServiceData;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.CloudServiceData.BillingData;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * This class is responsible for extracting the cloud cost data and
 * sending it to the Cost Component.
 */
public class DiscoveredCloudCostUploader {
    private static final Logger logger = LogManager.getLogger();

    public static final long MILLIS_PER_YEAR = 31536000000L; // ms per year

    private static final Set<EntityType> CLOUD_ENTITY_TYPES = ImmutableSet.of(
            EntityType.CLOUD_SERVICE,
            EntityType.COMPUTE_TIER,
            EntityType.STORAGE_TIER,
            EntityType.DATABASE_TIER,
            EntityType.REGION,
            EntityType.AVAILABILITY_ZONE,
            EntityType.RESERVED_INSTANCE,
            EntityType.BUSINESS_ACCOUNT
    );

    // special string prefix for identifying fallback accounts.
    private static final String FALLBACK_ACCOUNT_PREFIX = "Default account for target ";
    private static final Long FALLBACK_ACCOUNT_DEFAULT_OID = 0L; // used when no fallback account is found

    private static final DataMetricSummary CLOUD_COST_UPLOAD_TIME = DataMetricSummary.builder()
            .withName("tp_cloud_cost_upload_seconds")
            .withHelp("Time taken to perform cloud cost upload (in seconds). This is separated into 'build' and 'upload' stages.")
            .withLabelNames("stage")
            .build()
            .register();

    private final RIAndExpenseUploadServiceBlockingStub costServiceClient;

    // track the last upload time. We want to space the uploads apart, to once-per-hour.
    private volatile LocalDateTime lastUploadTime = null;

    // the minimum time between uploads. In addition, we will avoid doing uploads twice within the
    // same hour of the day.
    private static final int MIN_UPLOAD_INTERVAL_MINS = 45;

    // hash code for the last cloud cost data upload we successfully made
    private int lastUploadHash = 0;

    // a cache of all the cloud service non-market entities and cost dto's discovered by cloud
    // probes. The concurrent map is probably overkill, but the idea is to support concurrent writes.
    // When we read during the upload stage, we will make a defensive copy anyways.
    private final Map<Long, TargetCostData> costDataByTargetId = new ConcurrentHashMap<>();

    public DiscoveredCloudCostUploader(RIAndExpenseUploadServiceBlockingStub costServiceClient) {
        this.costServiceClient = costServiceClient;
    }

    /**
     * This is called when a discovery completes.
     *
     * Set aside any cloud cost data contained in the discovery response for the given target.
     * We will use this data later, in the topology pipeline.
     *
     * @param targetId
     * @param discovery
     * @param nonMarketEntityDTOS
     */
    public void recordTargetCostData(long targetId,
                                     @Nonnull Discovery discovery,
                                     @Nonnull final List<NonMarketEntityDTO> nonMarketEntityDTOS,
                                     @Nonnull final List<CostDataDTO> costDataDTOS) {
        TargetCostData costData = new TargetCostData();
        costData.targetId = targetId;
        costData.discovery = discovery;
        costData.cloudServiceEntities = nonMarketEntityDTOS.stream()
            .filter(nme -> NonMarketEntityType.CLOUD_SERVICE == nme.getEntityType())
            .collect(Collectors.toList());
        costData.costDataDTOS = costDataDTOS;
        costDataByTargetId.put(targetId, costData);
    }

    /**
     * When a target is removed, we will remove any cached cloud cost data associated with it.
     *
     * @param targetId
     */
    public void targetRemoved(long targetId) {
        costDataByTargetId.remove(targetId);
    }

    /**
     * Get an immutable snapshot of the cost data map in it's current state
     *
     * @return an {@link ImmutableMap} of the cost data objects, by target id.
     */
    public Map<Long, TargetCostData> getCostDataByTargetIdSnapshot() {
        return ImmutableMap.copyOf(this.costDataByTargetId);
    }

    /**
     * Upload the cloud cost data.
     *
     * Called in the topology pipeline after the stitching context has been created, but before
     * it has been converted to a topology map. Ths is because a lot of the data we need is in the
     * raw cloud entity data, much of which we lose in the conversion to topology map.
     *
     * We will be cross-referencing data from the cost DTO's, non-market entities, and topology
     * entities (in stitching entity form), from the billing and discovery probes. So there may be
     * some sensitivity to discovery mismatches between billing and discovery probe data.
     *
     * @param topologyInfo
     * @param stitchingContext
     */
    public synchronized void uploadCostData(TopologyInfo topologyInfo, StitchingContext stitchingContext) {
        // check if we are within our minimum upload interval. We may actually want to remove this
        // throttle if we find that we are tracking data that changes frequently.
        // The upload interval is as follows:
        //  1) We will not upload twice within the same hour-of-the-day
        //  2) We will not upload within 45 minutes of our last known upload
        //  3) Because we are not persisting the "last upload" timestamp, we will upload on the
        //     first attempt after component startup. This may violate rules (1) and (2), but
        //     should be an unusual case since the component should generally not be starting up
        //     often.
        if (lastUploadTime != null) {
            // are we within the same hour?
            LocalDateTime now = LocalDateTime.now();
            if (now.getHour() == lastUploadTime.getHour()) {
                logger.info("Skipping upload of cost data, since we're still within same hour as"
                        +" last upload at {}", lastUploadTime.toString());
                return;
            }
            // are we within the the miminum upload interval of the last one?
            if (now.minusMinutes(MIN_UPLOAD_INTERVAL_MINS).isBefore(lastUploadTime)) {
                logger.info("Skipping cloud cost data upload since last upload was {} mins ago.",
                        lastUploadTime.until(now, ChronoUnit.MINUTES));
                return;
            }
        }
        // we've passed the throttling checks.

        // create a copy of the cost data map so we have a stable data set for this upload step.
        Map<Long, TargetCostData> costDataByTargetIdSnapshot = getCostDataByTargetIdSnapshot();

        // TODO: we are letting the currency amount use the default of USD. In the
        // future, we should set this dynamically.
        DataMetricTimer buildTimer = CLOUD_COST_UPLOAD_TIME.labels("build").startTimer();

        // build a map allowing easy translation from cloud service local id's to TP oids
        final Map<String, Long> cloudEntityOidByLocalId = createCloudLocalIdToOidMap(stitchingContext);

        // get the RI data
        RICostComponentData riCostComponentData = createRICostComponentData(stitchingContext,
                cloudEntityOidByLocalId, costDataByTargetIdSnapshot);
        logger.debug("Created RI cost component data.");

        // get the account expenses
        Map<Long,AccountExpenses.Builder> accountExpensesByOid = createAccountExpenses(cloudEntityOidByLocalId,
                stitchingContext, costDataByTargetIdSnapshot);
        logger.debug("Created {} AccountExpenses.", accountExpensesByOid.size());

        // assemble and execute the upload
        UploadRIAndExpenseDataRequest.Builder requestBuilder = UploadRIAndExpenseDataRequest.newBuilder();
        accountExpensesByOid.values().forEach(expenses -> {
            requestBuilder.addAccountExpenses(expenses.build());
        });

        requestBuilder.addAllReservedInstanceSpecs(riCostComponentData.riSpecs);

        riCostComponentData.riBoughtByLocalId.values().forEach(riBought -> {
            requestBuilder.addReservedInstanceBought(riBought.build());
        });

        riCostComponentData.riCoverages.forEach(riCoverage -> {
            requestBuilder.addReservedInstanceCoverage(riCoverage.build());
        });

        buildTimer.observe();
        logger.debug("Building cost component request took {} secs", buildTimer.getTimeElapsedSecs());

        // do we have anything to upload here?
        int requestHash = requestBuilder.hashCode();
        // TODO:  the hash calculation doesn't do what's supposed to do yet -- the hash is
        // too sensitive and always seems to report a new code, even given the same discovery data.
        // This may be due to the way the hash code works in the protobuf objects, where things like
        // default properties and order of properties may cause hashing differences even if the
        // values themselves would be the same.
        // In any case, I didn't have time to dig into it yet -- but in the future, this method
        // should either be replaced with a more appropriate check (e.g. a checksum on the output?)
        // or perhaps removed altogether.
        // it's arguable that we may want to broadcast even if the checksum has not changed in some
        // cases, since we are not guaranteed that processing of previous results was complete.
        if (requestHash != lastUploadHash) {
            // set the topology id after calculating the hash, so it doesn't affect the hash calc
            requestBuilder.setTopologyId(topologyInfo.getTopologyId());

            logger.debug("Request hash {} is different, will upload this request.", requestHash);
            try (DataMetricTimer uploadTimer = CLOUD_COST_UPLOAD_TIME.labels("upload").startTimer()) {
                // we should probably upload empty data too, if we are relying on this as a way to
                // "clear" data when all cloud targets are removed.
                UploadRIAndExpenseDataResponse response = costServiceClient.uploadRIAndExpenseData(requestBuilder.build());
                logger.debug("Cloud cost upload took {} secs", uploadTimer.getTimeElapsedSecs());
            }
            lastUploadHash = requestHash;
            lastUploadTime = LocalDateTime.now();
        } else {
            logger.info("Cloud cost upload step calculated same hash as the previous one -- will skip this upload.");
        }
    }

    /**
     * Create a mapping of cloud entity local id's to oids, to facilitate later lookups by local id.
     * @param stitchingContext containing the cloud entities to include in the map.
     * @return A mapping from cloud discovered entity local id -> stitching entity oid
     */
    @VisibleForTesting
    public Map<String,Long> createCloudLocalIdToOidMap(StitchingContext stitchingContext) {
        // build a map allowing easy translation from cloud service local id's to TP oids
        final Map<String, Long> cloudEntityOidByLocalId = new HashMap<>();
        CLOUD_ENTITY_TYPES.forEach(entityType -> {
            stitchingContext.getEntitiesOfType(entityType).forEach(entity -> {
                cloudEntityOidByLocalId.put(entity.getLocalId(), entity.getOid());
            });
        });

        // also add the fallback accounts
        addFallbackAccounts(stitchingContext, cloudEntityOidByLocalId);

        return cloudEntityOidByLocalId;
    }

    /**
     * Find the fallback accounts for each target id that has one.
     *
     * The "fallback account" is the account to assign ownership to, in the event that a proper
     * owner cannot be found. We are using this because the ownership is "most likely" the fallback
     * account anyways, and it's presumably better to default to this relationship than to have no
     * owner relationship at all. Note that if a target doesn't discover business accounts, it will
     * not have a fallback account, and any entities needing ownership assignments will probably
     * remain with an account oid of 0.
     *
     * the ideal fallback account:
     *  - is not owned by any other account
     *  - owns other accounts
     *
     * In the event of multiple accounts for a given target satisfying both criteria, the
     * account that owns the most other accounts will be the winner. if there is still a
     * tie, then one will be chosen from amongst the tied.
     *
     * @param stitchingContext
     * @param cloudEntityOidByLocalId
     */
    private void addFallbackAccounts(StitchingContext stitchingContext, Map<String, Long> cloudEntityOidByLocalId) {
        // we will be iterating the list of accounts more than once, which is not the most
        // efficient way to do this. Let's replace this with a faster algorithm later, if fallback
        // accounts remain necessary.
        //
        Set<Long> ownedAccounts = new HashSet<>(); // keep a set of the owned accounts
        // # of accounts owned per account oid
        Map<Long, AtomicInteger> entityAccountsOwned = new HashMap<>();
        // set the default account to 0 owned and into the "owned" set, so it is easily surpassed in
        // tests of domination.
        entityAccountsOwned.put(FALLBACK_ACCOUNT_DEFAULT_OID, new AtomicInteger(0));
        ownedAccounts.add(FALLBACK_ACCOUNT_DEFAULT_OID);
        stitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT).forEach(entity -> {
            AtomicInteger numOwned = entityAccountsOwned.getOrDefault(entity.getOid(), new AtomicInteger(0));
            if (entity.getConnectedToByType().containsKey(ConnectionType.OWNS_CONNECTION)) {
                // add any owned entities to the "owned accounts" set
                entity.getConnectedToByType().get(ConnectionType.OWNS_CONNECTION).forEach(owned -> {
                    if (owned.getEntityType() == EntityType.BUSINESS_ACCOUNT) {
                        logger.debug("Account {} owned by {}", owned.getOid(), entity.getOid());
                        ownedAccounts.add(owned.getOid());
                        numOwned.getAndIncrement();
                    }
                });
            }
            entityAccountsOwned.put(entity.getOid(), numOwned); // save # accounts owned
        });
        // now, iterate the accounts again and pick the fallback accounts
        stitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT).forEach(entity -> {
            // for all targets discovering this account, see if this account can claim dominion over
            // the target.
            entity.getDiscoveringTargetIds().forEach(targetId -> {
                // If our current account owns more or equal accounts than the current fallback
                // account for this target does, or the current fallback account itself is known
                // to be owned, then take over the fallback position.
                Long currentFallbackAccount = getFallbackAccountOid(targetId, cloudEntityOidByLocalId);
                if (entityAccountsOwned.get(entity.getOid()).intValue()
                        >= entityAccountsOwned.get(currentFallbackAccount).intValue()
                        || ownedAccounts.contains(currentFallbackAccount)) {
                    logger.debug("Setting fallback account for target {} to {} (owns {})",
                            targetId, entity.getOid(), entityAccountsOwned.get(entity.getOid()));
                    setFallbackAccountOid(targetId, entity.getOid(), cloudEntityOidByLocalId);
                }
            });
        });
    }

    /**
     * Get the fallback account oid for a given target. The "fallback account" is going to be the
     * first master account discovered by a target, or 0 if no fallback account was found.
     *
     * This is used when we need to determine the owner account for something, but may have
     * incomplete data. An example is an RI that is bought on the current day -- normally the
     * account ownership comes from the billing data, but that would not be available until the next
     * day. So between now and the next billing data, we won't have an explicit account owner
     * identified -- so we will use the default owner.
     *
     * Hopefully we can replace this with something more explicit in the future.
     *
     * @param targetId
     * @param cloudEntityOidByLocalId
     * @return
     */
    private long getFallbackAccountOid(long targetId, final Map<String, Long> cloudEntityOidByLocalId) {
        // default owners are stored in the cloud entity map using a special key of "Default Owner
        // for xxx", where xxx = the target id.
        String fallbackAccountKey = FALLBACK_ACCOUNT_PREFIX + targetId;
        return cloudEntityOidByLocalId.getOrDefault(fallbackAccountKey, FALLBACK_ACCOUNT_DEFAULT_OID);
    }

    /**
     * Sets the fallback account oid to use for the specified target id.
     *
     * @param targetId
     * @param fallbackAccountOid
     */
    private void setFallbackAccountOid(long targetId, Long fallbackAccountOid, final Map<String, Long> cloudEntityOidByLocalId) {
        String fallbackAccountKey = FALLBACK_ACCOUNT_PREFIX + targetId;
        logger.debug("Setting fallback account for target {} to {}", targetId, fallbackAccountOid);
        cloudEntityOidByLocalId.put(fallbackAccountKey, fallbackAccountOid);
    }

    /**
     * Given a {@link StitchingContext} and the set of cached cost data objects from the discoveries,
     * create a {@link RICostComponentData} containing the RI and service cost information we find.
     *
     * @param stitchingContext the current topology being broadcasted.
     * @param cloudEntityOidByLocalId a mapping of cloud entities by local id, for resolving cross references in the billing data.
     * @param costDataByTargetIdSnapshot A copy of latest discovered cost data.
     * @return a fresh {@link RICostComponentData} object brimming with data.
     */
    public RICostComponentData createRICostComponentData(StitchingContext stitchingContext,
                                                         Map<String, Long> cloudEntityOidByLocalId,
                                                         Map<Long, TargetCostData> costDataByTargetIdSnapshot) {
        RICostComponentData riCostComponentData = new RICostComponentData();

        // pull the RI's from the context and transform them into RI specs and bought info for the
        // cost component
        addRISpecsAndBoughtInfo(riCostComponentData, stitchingContext, cloudEntityOidByLocalId);

        // next, get the coverage information
        addRICoverageData(riCostComponentData, stitchingContext, costDataByTargetIdSnapshot, cloudEntityOidByLocalId);

        return riCostComponentData;
    }

    /**
     * Consume the RI entities in the stitching context, using the valuable data they contain to
     * create RI specs and RI Bought information, which will be added to the RICostComponentData
     * object.
     *
     * NOTE: This method also has the side effect of REMOVING RI entities from the stitching
     * context! This is because we have decided to not keep them around in the official topology,
     * they will exist primarily in the cost component.
     *
     * @param riCostComponentData The {@link RICostComponentData} to populate with RI spec and bought
     * @param stitchingContext the stitching context to consume the RI's from
     * @param cloudEntityOidByLocalId a map from cloud entity local id -> oid
     */
    private void addRISpecsAndBoughtInfo(@Nonnull RICostComponentData riCostComponentData,
                                         @Nonnull StitchingContext stitchingContext,
                                         @Nonnull Map<String, Long> cloudEntityOidByLocalId) {

        Map<ReservedInstanceSpecInfo, Integer> riSpecInfoToInternalId = new HashMap<>();
        // find all of the RI specs and RI bought info in the stitching context
        //List<ReservedInstanceSpecInfo> riSpecInfos = new ArrayList<>();
        // keep a map of RI local id -> RI Bought. We will use this to gradually assemble the
        // RIBought info, which comes from multiple sources.
        Map<String, ReservedInstanceBought.Builder> riBoughtByLocalId = new HashMap<>();

        // build a list of the RI's we have consumed so we can remove them from the stitching context
        // later
        List<TopologyStitchingEntity> riEntitiesToRemove = new ArrayList<>();
        stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE)
                .forEach(riStitchingEntity -> {
                    if (riStitchingEntity.getEntityBuilder() == null) {
                        logger.warn("No entity builder found for RI stitching entity {}", riStitchingEntity.getLocalId());
                        return;
                    }
                    // create an RI spec based on the details of the entity
                    ReservedInstanceData riData = riStitchingEntity.getEntityBuilder().getReservedInstanceData();
                    ReservedInstanceSpecInfo.Builder riSpecInfoBuilder = ReservedInstanceSpecInfo.newBuilder()
                            .setType(ReservedInstanceType.newBuilder()
                                    .setOfferingClass(OfferingClass.forNumber(riData.getOfferingClass().getNumber()))
                                    .setPaymentOption(PaymentOption.forNumber(riData.getOfferingType().getNumber()))
                                    // convert duration from millis to years. Can't guarantee precision here, so we will
                                    // round to the nearest number of years.
                                    .setTermYears((int)Math.round((double)riData.getDuration()/MILLIS_PER_YEAR)))
                            .setTenancy(Tenancy.forNumber(riData.getInstanceTenancy().getNumber()))
                            .setOs(CloudCostUtils.platformToOSType(riData.getPlatform()));
                    // haz region?
                    if (riData.hasRegion() && cloudEntityOidByLocalId.containsKey(riData.getRegion())) {
                        riSpecInfoBuilder.setRegionId(cloudEntityOidByLocalId.get(riData.getRegion()));
                    }
                    // haz tier?
                    if (riData.hasRelatedProfileId() && cloudEntityOidByLocalId.containsKey(riData.getRelatedProfileId())) {
                        riSpecInfoBuilder.setTierId(cloudEntityOidByLocalId.get(riData.getRelatedProfileId()));
                    }

                    ReservedInstanceSpecInfo riSpecInfo = riSpecInfoBuilder.build();

                    // if we haven't already saved this spec, add it to the list.
                    if (!riSpecInfoToInternalId.containsKey(riSpecInfo)) {
                        riSpecInfoToInternalId.put(riSpecInfo, riSpecInfoToInternalId.size());
                    }

                    // Now we will create an RIBought object.

                    // First, create the cost object for it.
                    ReservedInstanceBoughtCost.Builder riBoughtCost = ReservedInstanceBoughtCost.newBuilder();
                    if (riData.hasFixedCost()) {
                        riBoughtCost.setFixedCost(CurrencyAmount.newBuilder()
                                .setAmount(riData.getFixedCost()));
                    }
                    if (riData.hasUsageCost()) {
                        riBoughtCost.setUsageCostPerHour(CurrencyAmount.newBuilder()
                                .setAmount(riData.getUsageCost()));
                    }
                    if (riData.hasRecurringCost()) {
                        riBoughtCost.setRecurringCostPerHour(CurrencyAmount.newBuilder()
                                .setAmount(riData.getRecurringCost()));
                    }
                    // Create the RI Bought Coupons entry for it too
                    ReservedInstanceBoughtCoupons.Builder riBoughtCoupons = ReservedInstanceBoughtCoupons.newBuilder()
                            .setNumberOfCoupons(riData.getNumberOfCoupons())
                            // NOTE: num coupons used always seems to be 0 (from discovery) or -1 (from
                            // billing), so we will build this value up by totalling the individual
                            // "used" amounts in the coverage information related to this RI.
                            // So even though a "numberOfCouponsUsed" getter is available on
                            // ReservedInstanceData, we will not call it because if this value does
                            // end up getting filled in during discovery, it will cause us to
                            // double-count used coupons since our counting algorithm will still be
                            // running. If/when the probe sends "used" directly, we should remove
                            // our counting algorithm, or activate it only as a fallback for cases
                            // where used is not available.
                            //.setNumberOfCouponsUsed(riData.getNumberOfCouponsUsed())
                            .setNumberOfCouponsUsed(0);

                    // Finally, create the RI Bought instance itself
                    ReservedInstanceBought.Builder riBought = ReservedInstanceBought.newBuilder()
                            .setId(riStitchingEntity.getOid()) // using the oid from TP for this upload
                            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                                    // we are defaulting the business account owner to the fallback
                                    // account for this probe. This *should* get overwritten by an
                                    // actual owner when we process coverage information later.
                                    // However, not all RI's are accounted for in the coverage info,
                                    // so this helps ensure assignment of a plausible owner for
                                    // those cases.
                                    .setBusinessAccountId(getFallbackAccountOid(riStitchingEntity.getTargetId(),
                                            cloudEntityOidByLocalId))
                                    .setProbeReservedInstanceId(riStitchingEntity.getLocalId())
                                    .setStartTime(riData.getStartTime())
                                    .setNumBought(riData.getInstanceCount())
                                    .setReservedInstanceSpec(riSpecInfoToInternalId.get(riSpecInfo))
                                    .setReservedInstanceBoughtCost(riBoughtCost)
                                    .setReservedInstanceBoughtCoupons(riBoughtCoupons)
                            );
                    // set AZ, if it exists
                    if (riData.hasAvailabilityZone()
                            && cloudEntityOidByLocalId.containsKey(riData.getAvailabilityZone())) {
                        riBought.getReservedInstanceBoughtInfoBuilder()
                                .setAvailabilityZoneId(cloudEntityOidByLocalId.get(
                                        riData.getAvailabilityZone()));
                    }
                    riBoughtByLocalId.put(riStitchingEntity.getLocalId(), riBought);

                    // We are removing these RI entities from the topology.
                    riEntitiesToRemove.add(riStitchingEntity);
                });

        // SIDE EFFECT: remove the RI entities from the stitching context.
        riEntitiesToRemove.forEach(stitchingContext::removeEntity);

        // add the extracted data to the cost component data object.
        // create the RISpec objects, setting the spec ids to indices in the spec info list.
        List<ReservedInstanceSpec> riSpecs = new ArrayList<>(riSpecInfoToInternalId.size());
        riSpecInfoToInternalId.forEach((spec,id) -> {
            riSpecs.add(ReservedInstanceSpec.newBuilder()
                    .setId(id)
                    .setReservedInstanceSpecInfo(spec)
                    .build());
        });
        riCostComponentData.riSpecs = riSpecs;
        riCostComponentData.riBoughtByLocalId = riBoughtByLocalId;
    }

    /**
     * Find the RI coverage information in the cost data cache, and use it to populate the coverage
     * information in the {@link RICostComponentData} object we are assembling.
     *
     * This method requires that the {@link RICostComponentData} object is already partially
     * populated, with the RI specs and RI Bought data.
     *
     * @param riCostComponentData
     * @param costDataByTargetIdSnapshot
     * @param cloudEntityOidByLocalId
     */
    private void addRICoverageData(@Nonnull RICostComponentData riCostComponentData,
                                   @Nonnull StitchingContext stitchingContext,
                                   @Nonnull Map<Long, TargetCostData> costDataByTargetIdSnapshot,
                                   @Nonnull Map<String, Long> cloudEntityOidByLocalId) {
        // a little more null checking here since we do have a dependency on the data in the RI Cost
        // data so far.
        Objects.requireNonNull(riCostComponentData);
        Objects.requireNonNull(stitchingContext);
        Objects.requireNonNull(costDataByTargetIdSnapshot);
        Objects.requireNonNull(cloudEntityOidByLocalId);
        // ensure we have the data structures in place we need.
        if (riCostComponentData.riBoughtByLocalId == null || riCostComponentData.riSpecs == null) {
            throw new IllegalStateException("Missing RI Bought and/or RI Spec structures.");
        }

        // now comb through the cloud services, and extract the remaining RI-related information from them.
        List<EntityReservedInstanceCoverage.Builder> riCoverages = new ArrayList<>();
        // we will lazily-populate the map of VM local id -> oid mapping, since we should only need
        // it when RI's are in use.
        Map<String,Long> vmLocalIdToOid = new HashMap<>();
        costDataByTargetIdSnapshot.forEach((targetId, targetCostData) -> {
            targetCostData.cloudServiceEntities.stream()
                    .filter(NonMarketEntityDTO::hasCloudServiceData)
                    .forEach(nme -> {
                        if (!nme.getCloudServiceData().hasBillingData()) {
                            return;
                        }
                        CloudServiceData cloudServiceData = nme.getCloudServiceData();
                        BillingData billingData = nme.getCloudServiceData().getBillingData();
                        long accountOid = cloudEntityOidByLocalId.getOrDefault(cloudServiceData.getAccountId(),
                                getFallbackAccountOid(targetId, cloudEntityOidByLocalId));
                        if (!cloudEntityOidByLocalId.containsKey(cloudServiceData.getAccountId())) {
                            logger.warn("Account {} not found in stitching data, using fallback account {}.",
                                    cloudServiceData.getAccountId(),
                                    accountOid);
                        }
                        // assign this account id to any RI bought by this account
                        billingData.getReservedInstancesList().forEach(riEntity -> {
                            // get the RI object we've created that has the matching local id.
                            if (riCostComponentData.riBoughtByLocalId.containsKey(riEntity.getId())) {
                                logger.debug("Assigning RI {} to Account {}", riEntity.getId(),
                                        cloudServiceData.getAccountId());
                                riCostComponentData.riBoughtByLocalId.get(riEntity.getId())
                                        .getReservedInstanceBoughtInfoBuilder()
                                        .setBusinessAccountId(accountOid);
                            }
                        });
                        // map RI coverages on any VM's too.
                        billingData.getVirtualMachinesList().forEach(vmEntity -> {
                            // create an EntityRICoverage object if any coupons are traded by this VM
                            EntityReservedInstanceCoverage.Builder entityRIBought =
                                    vmEntity.getCommoditiesSoldList().stream()
                                            .filter(commSold -> CommodityType.COUPON.equals(commSold.getCommodityType()))
                                            .findFirst()
                                            .map(commSold -> {
                                                if (vmLocalIdToOid.isEmpty()) {
                                                    // lazily initialize the vm id map
                                                    logger.debug("Building VM local id -> oid map");
                                                    stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_MACHINE)
                                                            .forEach(vm -> {
                                                                vmLocalIdToOid.put(vm.getLocalId(), vm.getOid());
                                                            });
                                                }
                                                if (vmLocalIdToOid.containsKey(vmEntity.getId())) {
                                                    return EntityReservedInstanceCoverage.newBuilder()
                                                            .setEntityId(vmLocalIdToOid.get(vmEntity.getId()))
                                                            .setTotalCouponsRequired(commSold.getCapacity());
                                                }
                                                // skip this entity since the VM was not found
                                                logger.warn("VM not found with local id {} -- skipping RI coverage data",
                                                        vmEntity.getId());
                                                return null;
                                            })
                                            .orElse(null);
                            // if we have no coupons sold, exit now.
                            if (entityRIBought == null) {
                                return;
                            }
                            // add to the return list
                            riCoverages.add(entityRIBought);
                            // get the coverage info
                            vmEntity.getCommoditiesBoughtList().stream()
                                    .filter(commBought -> EntityType.RESERVED_INSTANCE.equals(commBought.getProviderType()))
                                    .forEach(commBought -> {
                                        commBought.getBoughtList().forEach(commodityDTO -> {
                                            if (CommodityType.COUPON.equals(commodityDTO.getCommodityType())) {
                                                logger.debug("Mapping RI {} to account {} vm {}",
                                                        commBought.getProviderId(),
                                                        cloudServiceData.getAccountId(), vmEntity.getId());
                                                long riOid = cloudEntityOidByLocalId.getOrDefault(commBought.getProviderId(), 0L);
                                                if (riOid == 0) {
                                                    logger.warn("Couldn't find RI oid for local id {}", commBought.getProviderId());
                                                }
                                                entityRIBought.addCoverage(Coverage.newBuilder()
                                                        .setProbeReservedInstanceId(commBought.getProviderId())
                                                        .setCoveredCoupons(commodityDTO.getUsed()));
                                                // increment the RI Bought coupons used by the used amount.
                                                ReservedInstanceBought.Builder rib = riCostComponentData.riBoughtByLocalId.get(commBought.getProviderId());
                                                if (rib != null) {
                                                    ReservedInstanceBoughtCoupons.Builder coupons =
                                                            rib.getReservedInstanceBoughtInfoBuilder().getReservedInstanceBoughtCouponsBuilder();
                                                    coupons.setNumberOfCouponsUsed(commodityDTO.getUsed() + coupons.getNumberOfCouponsUsed());
                                                }
                                            }
                                        });
                                    });

                        });
                    });
        });

        riCostComponentData.riCoverages = riCoverages;
    }


    /**
     * Create the set of account expenses to be uploaded.
     * @return a map of account expenses, keyed by business account oid
     */
    @VisibleForTesting
    public Map<Long,AccountExpenses.Builder> createAccountExpenses(
            Map<String, Long> cloudEntityOidByLocalId, StitchingContext stitchingContext,
            Map<Long, TargetCostData> costDataByTargetIdSnapshot) {

        // create the initial AccountExpenses objects w/receipt time based on target discovery time
        // in the future, this will be based on a billing time when that data is available.
        Map<Long,AccountExpenses.Builder> expensesByAccountOid = new HashMap<>();
        stitchingContext.getEntitiesOfType(EntityType.BUSINESS_ACCOUNT).forEach(stitchingEntity -> {
            // TODO: use the discovery time as the expense received time until we can find a better
            // expense-related time source. (e.g. the billing data itself). The time will be
            // converted from a local datetime to unix epoch millis.
            long discoveryTime = costDataByTargetIdSnapshot.get(stitchingEntity.getTargetId())
                    .discovery.getCompletionTime()
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            expensesByAccountOid.put(stitchingEntity.getOid(), AccountExpenses.newBuilder()
                    .setAssociatedAccountId(stitchingEntity.getOid())
                    .setExpenseReceivedTimestamp(discoveryTime));
        });

        // Find the service expenses from the cost data objects, and assign them to the
        // account expenses created above.
        costDataByTargetIdSnapshot.forEach((targetId, targetCostData) -> {
            targetCostData.costDataDTOS.forEach(costData -> {
                // find the expenses builder for the associated account.

                if (!costData.hasAccountId()) {
                    logger.warn("No account id set for costData object {} with cost {}",
                            costData.getId(), costData.getCost());
                    return;
                }
                Long accountOid = cloudEntityOidByLocalId.getOrDefault(costData.getAccountId(),
                            getFallbackAccountOid(targetId, cloudEntityOidByLocalId));
                if (!cloudEntityOidByLocalId.containsKey(costData.getAccountId())) {
                    logger.warn("Couldn't find biz account oid for local id {}, using fallback account {}.",
                            costData.getAccountId(),
                            accountOid);
                }
                if (!expensesByAccountOid.containsKey(accountOid)) {
                    logger.warn("No expense builder for account oid {}.", accountOid);
                    return;
                }

                AccountExpenses.Builder accountExpensesBuilder = expensesByAccountOid.get(accountOid);

                // create an expense entry for each cost object
                if (EntityType.CLOUD_SERVICE.equals(costData.getEntityType())) {
                    // create a ServiceExpenses for Cloud Services
                    ServiceExpenses.Builder serviceExpensesBuilder = ServiceExpenses.newBuilder();
                    // find the related cloud service entity from the topology map. We are creating
                    // one cloud service entity per service, rather than per account + service
                    // combination as the DTO represents.
                    // To do mapping from NME cloud service to TP cloud service, we will create an
                    // account-agnostic cloud service 'local id' by slicing the account id out of the
                    // cloud service's regular account-specific local id. We will use this modified
                    // local id to find the shared, account-agnostic cloud service topology entity.
                    String sharedCloudServiceLocalId = getCloudServiceLocalId(costData.getId(),
                            costData.getAccountId());
                    Long cloudServiceOid = cloudEntityOidByLocalId.get(sharedCloudServiceLocalId);
                    if (cloudServiceOid == null) {
                        logger.warn("Couldn't find a cloud service oid for service {}", sharedCloudServiceLocalId);
                        cloudServiceOid = 0L;
                    }
                    serviceExpensesBuilder.setAssociatedServiceId(cloudServiceOid);
                    serviceExpensesBuilder.setExpenses(CurrencyAmount.newBuilder()
                            .setAmount(costData.getCost()).build());
                    accountExpensesBuilder.getAccountExpensesInfoBuilder()
                            .addServiceExpenses(serviceExpensesBuilder);
                    logger.debug("Attached ServiceExpenses {} for service {} to account {}({})", costData.getId(),
                            serviceExpensesBuilder.getAssociatedServiceId(), costData.getAccountId(), accountOid);

                } else if (EntityType.VIRTUAL_MACHINE.equals(costData.getEntityType()) ||
                        EntityType.DATABASE_SERVER.equals(costData.getEntityType())) {
                    // Create TierExpenses for compute / database /storage tiers
                    TierExpenses.Builder tierExpensesBuilder = TierExpenses.newBuilder();
                    // find the compute tier matching our cost id
                    Long tierOid = cloudEntityOidByLocalId.get(costData.getId());
                    tierExpensesBuilder.setAssociatedTierId(tierOid);
                    tierExpensesBuilder.setExpenses(CurrencyAmount.newBuilder()
                            .setAmount(costData.getCost()).build());
                    accountExpensesBuilder.getAccountExpensesInfoBuilder()
                            .addTierExpenses(tierExpensesBuilder);
                    logger.debug("Attached TierExpenses {} for tier {} to account {}({})", costData.getId(),
                            tierExpensesBuilder.getAssociatedTierId(), costData.getAccountId(), accountOid);
                }

            });
        });

        return expensesByAccountOid;
    }

    /**
     * This function translates a full probe cloud serviceid to an account-agnostic format that
     * excludes the account id. e.g.:
     *
     * Input: "aws::192821421245::CS::AmazonS3" (account id 192821421245)
     * Output: "aws::CS::AmazonS3"
     *
     * @param accountSpecificCloudServiceId
     * @return
     */
    public String getCloudServiceLocalId(String accountSpecificCloudServiceId, String accountId) {
        return accountSpecificCloudServiceId.replace("::"+ accountId, "");
    }

    /**
     * Caches a target's cost data + discovery information.
     */
    public static class TargetCostData {
        public long targetId;

        public Discovery discovery;

        public List<NonMarketEntityDTO> cloudServiceEntities;

        public List<CostDataDTO> costDataDTOS;
    }

    /**
     * Holds the RI records that will be sent to the Cost component
     */
    public static class RICostComponentData {
        List<ReservedInstanceSpec> riSpecs;

        Map<String, ReservedInstanceBought.Builder> riBoughtByLocalId;

        List<EntityReservedInstanceCoverage.Builder> riCoverages;

    }

}
