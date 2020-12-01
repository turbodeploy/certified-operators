package com.vmturbo.topology.processor.cost;

import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_UPLOAD_TIME;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.MILLIS_PER_3_YEAR;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.MILLIS_PER_YEAR;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.RI_DATA_SECTION;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_BUILD_STAGE;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_UPLOAD_STAGE;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.GetRIDataChecksumRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.AccountRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage.RICoverageSource;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.mediation.hybrid.cloud.common.PropertyName;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ReservedInstanceData;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.CloudServiceData;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.CloudServiceData.BillingData;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.TargetCostData;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * RICostDataUploader collects Account expense data and Reserved Instance coverage and purchases
 * data from probe discoveries and sends it to the cost component.
 */
public class RICostDataUploader {
    private static final Logger logger = LogManager.getLogger();

    private final RIAndExpenseUploadServiceBlockingStub costServiceClient;

    // the minimum number of minutes to wait between RI data uploads. Must be >= 0.
    private final int minimumRIDataUploadIntervalMins;

    // used to get time stamps
    private final Clock clock;

    // track the last upload time. We want to space the uploads apart, to once-per-hour.
    private Instant lastUploadTime = null;

    private static final int DELTA_RI_DURATION_DAYS = 1;

    private final boolean discardRIsWithoutPurchasingAccountData;

    private final boolean riSupportInPartialCloudEnvironment;

    /**
     * Constructor.
     *
     * @param costServiceClient the {@link RIAndExpenseUploadServiceBlockingStub}
     * @param minimumRIDataUploadIntervalMins the minimum number of minutes to wait between
     *         RI data uploads
     * @param clock the {@link Clock}
     * @param fullAzureEARIDiscovery not discard reserved instances if the purchasing
     *         account information has not been discovered
     * @param riSupportInPartialCloudEnvironment OM-58310: Improve handling partially
     *         discovered cloud environment feature flag. Not discard reserved instances if the
     *         purchasing account information has not been discovered. Send entity level coverages
     *         for all discovered workloads covered by discovered and undiscovered reserved
     *         instances.
     */
    public RICostDataUploader(
            @Nonnull final RIAndExpenseUploadServiceBlockingStub costServiceClient,
            final int minimumRIDataUploadIntervalMins, @Nonnull final Clock clock,
            final boolean fullAzureEARIDiscovery,
            final boolean riSupportInPartialCloudEnvironment) {
        if (minimumRIDataUploadIntervalMins < 0) {
            throw new IllegalArgumentException(
                    "minimumRIDataUploadIntervalMins cannot be less than 0.");
        }
        this.costServiceClient = costServiceClient;
        this.minimumRIDataUploadIntervalMins = minimumRIDataUploadIntervalMins;
        this.clock = clock;
        this.riSupportInPartialCloudEnvironment = riSupportInPartialCloudEnvironment;
        this.discardRIsWithoutPurchasingAccountData =
                !fullAzureEARIDiscovery && !riSupportInPartialCloudEnvironment;
    }

    /**
     * Upload the RI data to the cost component.
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
    public synchronized void uploadRIData(Map<Long, TargetCostData> costDataByTarget,
                                            TopologyInfo topologyInfo,
                                            StitchingContext stitchingContext,
                                            CloudEntitiesMap cloudEntitiesMap) {
        // check if we are within our minimum upload interval. Note that we are not persisting the
        // last upload time stamp, and starting a new set of upload interval checks every time the
        // component starts. Component restarts should be rare, so we don't expect this to be much
        // of an issue, but if this does become a problem we can add persistence of the last upload
        // time.
        if (lastUploadTime != null) {
            if (clock.instant().minus(minimumRIDataUploadIntervalMins, ChronoUnit.MINUTES)
                    .isBefore(lastUploadTime)) {
                // we are within the minimum upload interval -- we need to skip this upload.
                logger.info("Skipping upload of RI data, since we're within minimum upload " +
                        "interval since the last upload at {}", lastUploadTime.toString());
                return;
            }
        }
        // we've passed the throttling checks.

        DataMetricTimer buildTimer = CLOUD_COST_UPLOAD_TIME.labels(RI_DATA_SECTION,
                UPLOAD_REQUEST_BUILD_STAGE).startTimer();

        // get the RI data
        RICostComponentData riCostComponentData = createRICostComponentData(stitchingContext,
                cloudEntitiesMap, costDataByTarget);
        logger.debug("Created RI cost component data.");

        // assemble and execute the upload
        UploadRIDataRequest.Builder requestBuilder = UploadRIDataRequest.newBuilder();
        requestBuilder.addAllReservedInstanceSpecs(riCostComponentData.riSpecs);

        riCostComponentData.riBoughtByLocalId.values().forEach(
                requestBuilder::addReservedInstanceBought);
        riCostComponentData.entityLevelReservedInstanceCoverages.forEach(
                requestBuilder::addReservedInstanceCoverage);
        riCostComponentData.accountLevelReservedInstanceCoverages.forEach(
                requestBuilder::addAccountLevelReservedInstanceCoverage);

        buildTimer.observe();
        logger.debug("Building cost component request took {} secs", buildTimer.getTimeElapsedSecs());

        // check the last processed hash to see if we should upload again
        long lastProcessedHash = costServiceClient.getRIDataChecksum(
                GetRIDataChecksumRequest.getDefaultInstance()).getChecksum();

        // generate a hash of the intermediate build - we won't add topology id in yet because that
        // is expected to change each iteration.
        long requestHash = requestBuilder.build().hashCode();
        if (requestHash != lastProcessedHash) {
            // set the topology ID after calculating the hash, so it doesn't affect the hash calc
            requestBuilder.setTopologyContextId(topologyInfo.getTopologyId());
            requestBuilder.setChecksum(requestHash);
            requestBuilder.setCreatedTime(System.currentTimeMillis());

            logger.debug("Request hash [{}] is different from last processed hash [{}], will upload this request.",
                    Long.toUnsignedString(requestHash), Long.toUnsignedString(lastProcessedHash));
            try (DataMetricTimer uploadTimer = CLOUD_COST_UPLOAD_TIME.labels(
                    RI_DATA_SECTION, UPLOAD_REQUEST_UPLOAD_STAGE).startTimer()) {
                // we should probably upload empty data too, if we are relying on this as a way to
                // "clear" data when all cloud targets are removed.
                UploadRIDataRequest request = requestBuilder.build();
                logger.info("Uploading RI cost data ({} bytes)", request.getSerializedSize());
                costServiceClient.uploadRIData(requestBuilder.build());
                logger.info("Cloud cost upload took {} secs", uploadTimer.getTimeElapsedSecs());
                lastUploadTime = clock.instant();
            } catch (Exception e) {
                logger.error("Error uploading cloud expenses", e);
            }
        } else {
            logger.info("RI / Expense upload step calculated same hash as the last processed hash -- will skip this upload.");
        }
    }

    /**
     * Given a {@link StitchingContext} and the set of cached cost data objects from the discoveries,
     * create a {@link RICostComponentData} containing the RI and service cost information we find.
     *
     * @param stitchingContext the current topology being broadcasted.
     * @param cloudEntitiesMap a mapping of cloud entities by local id, for resolving cross references in the billing data.
     * @param costDataByTargetIdSnapshot A copy of latest discovered cost data.
     * @return a fresh {@link RICostComponentData} object brimming with data.
     */
    RICostComponentData createRICostComponentData(StitchingContext stitchingContext,
                                                         CloudEntitiesMap cloudEntitiesMap,
                                                         Map<Long, TargetCostData> costDataByTargetIdSnapshot) {
        final RICostComponentData riCostComponentData = new RICostComponentData();
        // pull the RI's from the context and transform them into RI specs and bought info for the
        // cost component
        addRISpecsAndBoughtInfo(riCostComponentData, stitchingContext, cloudEntitiesMap);

        // next, get the coverage information
        addRICoverageData(riCostComponentData, stitchingContext, costDataByTargetIdSnapshot,
                cloudEntitiesMap);

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
     * @param cloudEntitiesMap a map from cloud entity local id -> oid
     */
    private void addRISpecsAndBoughtInfo(@Nonnull final RICostComponentData riCostComponentData,
            @Nonnull final StitchingContext stitchingContext,
            @Nonnull final CloudEntitiesMap cloudEntitiesMap) {
        // find all of the RI specs and RI bought info in the stitching context
        // keep a map of RI local id -> RI Bought. We will use this to gradually assemble the
        // RIBought info, which comes from multiple sources.
        final Map<String, ReservedInstanceBought.Builder> riBoughtByLocalId = new HashMap<>();

        // Map of reserved instance spec to generated id.
        // When topology processor uploading reserved instance bought data to cost component,
        // topology processor will create a local id of this reserved instance spec,
        // and cost component needs to change local id to a real id.
        final Map<ReservedInstanceSpecInfo, Integer> riSpecInfoToInternalId = new HashMap<>();

        final Set<Long> accountsWithDiscoveredData = stitchingContext
                .getEntitiesOfType(EntityType.BUSINESS_ACCOUNT)
                .filter(ba -> ba.getEntityBuilder().getBusinessAccountData().getDataDiscovered())
                .map(TopologyStitchingEntity::getOid)
                .collect(Collectors.toSet());

        stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE).forEach(ri -> {
            final ReservedInstanceData riData = ri.getEntityBuilder().getReservedInstanceData();
            Long purchasingAccountOId = null;
            if (riData.hasPurchasingAccountId()) {
                purchasingAccountOId = cloudEntitiesMap.get(riData.getPurchasingAccountId());
                if (purchasingAccountOId != null) {
                    if (!accountsWithDiscoveredData.contains(purchasingAccountOId)
                            && discardRIsWithoutPurchasingAccountData) {
                        logger.info(
                                "Ignoring RI {} because the data for purchasing account {} has not been discovered.",
                                ri.getLocalId(), purchasingAccountOId);
                        return;
                    }
                } else {
                    purchasingAccountOId = cloudEntitiesMap.getFallbackAccountOid(ri.getTargetId());
                    logger.warn(
                            "Could not retrieve purchasing account oid by id {}"
                                    + " {}. Using the fall back account : {} from target: {}.",
                            riData.getPurchasingAccountId(), ri.getLocalId(),
                            purchasingAccountOId, ri.getTargetId());
                }
            } else {
                purchasingAccountOId = cloudEntitiesMap.getFallbackAccountOid(ri.getTargetId());
                logger.warn("RI {} does not have a purchasing account in the"
                                + " ReservedInstanceData. Using the fall back account : {} from target: {}.",
                        ri.getLocalId(), purchasingAccountOId, ri.getTargetId());
            }

            // We log partial term Reserved Instances ie RI's whose term is not 1 or 3 years
            if (!isRIDurationStandard(riData)) {
                final long durationInDays = Duration.ofMillis(riData.getDuration()).abs().toDays();
                logger.info("Partial term RI with ID {} and duration {} days.", ri.getLocalId(), durationInDays);
            }

            final ReservedInstanceSpecInfo.Builder reservedInstanceSpecInfo =
                    ReservedInstanceSpecInfo.newBuilder();
            if (riData.hasRegion()) {
                final Long regionId = cloudEntitiesMap.get(riData.getRegion());
                if (regionId != null) {
                    reservedInstanceSpecInfo.setRegionId(regionId);
                } else {
                    logger.info("Ignoring RI {} because the region oid by id {} was not found.",
                            ri.getLocalId(), riData.getRegion());
                    return;
                }
            }
            if (riData.hasRelatedProfileId()) {
                final Long tierId = cloudEntitiesMap.get(riData.getRelatedProfileId());
                if (tierId != null) {
                    reservedInstanceSpecInfo.setTierId(tierId);
                } else {
                    logger.info(
                            "Ignoring RI {} because the tier oid by related profile id {} was not found.",
                            ri.getLocalId(), riData.getRelatedProfileId());
                    return;
                }
            }

            final ReservedInstanceBoughtInfo.Builder reservedInstanceBoughtInfo =
                    ReservedInstanceBoughtInfo.newBuilder();
            if (riData.hasAvailabilityZone()) {
                final Long availabilityZoneId = cloudEntitiesMap.get(riData.getAvailabilityZone());
                if (availabilityZoneId != null) {
                    reservedInstanceBoughtInfo.setAvailabilityZoneId(availabilityZoneId);
                } else {
                    logger.info(
                            "Ignoring RI {} because the availability zone oid by id {} was not found.",
                            ri.getLocalId(), riData.getAvailabilityZone());
                    return;
                }
            }

            final ReservedInstanceType.Builder reservedInstanceType =
                    ReservedInstanceType.newBuilder()
                            .setOfferingClass(
                                    OfferingClass.forNumber(riData.getOfferingClass().getNumber()))
                            .setPaymentOption(
                                    PaymentOption.forNumber(riData.getOfferingType().getNumber()));
            // Convert duration from millis to years.
            // Precision can't be guaranteed here, rounding to the nearest number of years.
            reservedInstanceType.setTermYears(
                    (int)Math.round((double)riData.getDuration() / MILLIS_PER_YEAR));

            reservedInstanceSpecInfo.setType(reservedInstanceType)
                    .setTenancy(Tenancy.forNumber(riData.getInstanceTenancy().getNumber()))
                    .setOs(CloudCostUtils.platformToOSType(riData.getPlatform()));

            if (riData.hasInstanceSizeFlexible()) {
                reservedInstanceSpecInfo.setSizeFlexible(riData.getInstanceSizeFlexible());
            }
            reservedInstanceSpecInfo.setPlatformFlexible(riData.getPlatformFlexible());

            final ReservedInstanceSpecInfo riSpecInfo = reservedInstanceSpecInfo.build();
            if (!riSpecInfoToInternalId.containsKey(riSpecInfo)) {
                // Generate reserved instance spec id.
                // The sign of increasing map size is used as the emitter of the new id.
                riSpecInfoToInternalId.put(riSpecInfo, riSpecInfoToInternalId.size());
            }

            final ReservedInstanceBoughtCost.Builder riBoughtCost =
                    ReservedInstanceBoughtCost.newBuilder();
            if (riData.hasFixedCost()) {
                riBoughtCost.setFixedCost(
                        CurrencyAmount.newBuilder().setAmount(riData.getFixedCost()));
            }
            if (riData.hasUsageCost()) {
                riBoughtCost.setUsageCostPerHour(
                        CurrencyAmount.newBuilder().setAmount(riData.getUsageCost()));
            }
            if (riData.hasRecurringCost()) {
                riBoughtCost.setRecurringCostPerHour(
                        CurrencyAmount.newBuilder().setAmount(riData.getRecurringCost()));
            }

            // NOTE: num coupons used always seems to be 0 (from discovery) or -1 (from billing),
            // so we will build this value up by totalling the individual "used" amounts in the coverage
            // information related to this RI. So even though a "numberOfCouponsUsed" getter is available
            // on ReservedInstanceData, we will not call it because if this value does end up getting
            // filled in during discovery, it will cause us to double-count used coupons since our
            // counting algorithm will still be running. If/when the probe sends "used" directly,
            // we should remove our counting algorithm, or activate it only as a fallback for cases
            // where used is not available.
            int numberOfCoupons = riData.getNumberOfCoupons();
            if (numberOfCoupons == 0) {
                numberOfCoupons = getNumberOfCouponsFromInstanceFamily(riData.getRelatedProfileId(),
                        stitchingContext);
                logger.info("Number of coupons not set for RI {},"
                                + " setting as max ({}) from Instance family",
                        ri.getLocalId(), numberOfCoupons);
            }
            final ReservedInstanceBoughtCoupons.Builder riBoughtCoupons =
                    ReservedInstanceBoughtCoupons.newBuilder()
                            .setNumberOfCoupons(numberOfCoupons)
                            .setNumberOfCouponsUsed(0);


            reservedInstanceBoughtInfo.setProbeReservedInstanceId(ri.getLocalId())
                            .setReservedInstanceSpec(riSpecInfoToInternalId.get(riSpecInfo))
                            .setReservedInstanceBoughtCost(riBoughtCost)
                            .setReservedInstanceBoughtCoupons(riBoughtCoupons);

            if (riData.hasStartTime()) {
                reservedInstanceBoughtInfo.setStartTime(riData.getStartTime());
            }

            if (riData.hasInstanceCount()) {
                reservedInstanceBoughtInfo.setNumBought(riData.getInstanceCount());
            }
            // Fall back if purchasingAccountOId comes through as null.
            if (purchasingAccountOId != null) {
                // If purchasing account is set in RI data then get it from there.
                reservedInstanceBoughtInfo.setBusinessAccountId(purchasingAccountOId);
            } else {
                // Otherwise use fallback target account.
                reservedInstanceBoughtInfo.setBusinessAccountId(
                        cloudEntitiesMap.getFallbackAccountOid(ri.getTargetId()));
                logger.warn("Using fallback account for reserved instance {}", ri.getLocalId());
            }

            if (ri.getEntityBuilder().hasDisplayName()) {
                reservedInstanceBoughtInfo.setDisplayName(ri.getDisplayName());
            }

            if (riData.hasEndTime()) {
                reservedInstanceBoughtInfo.setEndTime(riData.getEndTime());
            }

            if (riData.hasReservationOrderId()) {
                reservedInstanceBoughtInfo.setReservationOrderId(riData.getReservationOrderId());
            }

            final ReservedInstanceScopeInfo.Builder scopeInfo =
                    ReservedInstanceScopeInfo.newBuilder();
            if (riData.hasAppliedScope()) {
                switch (riData.getAppliedScope().getAppliedScopeTypeCase()) {
                    case SHARED_RESERVED_INSTANCE_SCOPE:
                        scopeInfo.setShared(true);
                        break;
                    case MULTIPLE_ACCOUNTS_RESERVED_INSTANCE_SCOPE:
                        scopeInfo.setShared(false);
                        for (final String accountId : riData.getAppliedScope()
                                .getMultipleAccountsReservedInstanceScope()
                                .getAccountIdList()) {
                            final Long accountOid = cloudEntitiesMap.get(accountId);
                            if (accountOid != null) {
                                scopeInfo.addApplicableBusinessAccountId(accountOid);
                            } else {
                                logger.error(
                                        "Cannot find account from RI applied scopes. RI: {}, account: {}",
                                        ri.getLocalId(), accountId);
                            }
                        }
                        break;
                    case APPLIEDSCOPETYPE_NOT_SET:
                    default:
                        logger.debug("RI {} has unknown applied scope", ri.getLocalId());
                        break;
                }
            }
            reservedInstanceBoughtInfo.setReservedInstanceScopeInfo(scopeInfo);

            final ReservedInstanceBought.Builder riBought = ReservedInstanceBought.newBuilder()
                    .setId(ri.getOid())
                    .setReservedInstanceBoughtInfo(reservedInstanceBoughtInfo);

            riBoughtByLocalId.put(ri.getLocalId(), riBought);
        });

        // add the extracted data to the cost component data object.
        // create the RISpec objects, setting the spec ids to indices in the spec info list.
        final List<ReservedInstanceSpec> riSpecs = new ArrayList<>(riSpecInfoToInternalId.size());
        riSpecInfoToInternalId.forEach((spec, id) -> riSpecs.add(ReservedInstanceSpec.newBuilder()
                .setId(id)
                .setReservedInstanceSpecInfo(spec)
                .build()));
        riCostComponentData.riSpecs = riSpecs;
        riCostComponentData.riBoughtByLocalId = riBoughtByLocalId;
    }

    private int getNumberOfCouponsFromInstanceFamily(final String instanceType,
                                                     final StitchingContext stitchingContext) {
        return stitchingContext.getEntitiesOfType(EntityType.COMPUTE_TIER)
                .filter(tier -> tier.getEntityBuilder().getId().equals(instanceType))
                .filter(tier -> tier.getEntityBuilder().hasComputeTierData())
                .findFirst()
                .map(tier -> tier.getEntityBuilder().getComputeTierData().getNumCoupons())
                .orElse(0);
    }

    /**
     * The RI duration should be within 1 day of 1*365 days (1 year RI term) or 3*365 days (3 year RI term).
     * @param riData RI spec based on the details of the entity
     * @return true if a standard RI duration.
     */
    private boolean isRIDurationStandard(final ReservedInstanceData riData) {
        return (Duration.ofMillis(riData.getDuration()).minusMillis(MILLIS_PER_YEAR).abs().toDays()
                < DELTA_RI_DURATION_DAYS)
                || (Duration.ofMillis(riData.getDuration()).minusMillis(MILLIS_PER_3_YEAR).abs().toDays()
                < DELTA_RI_DURATION_DAYS);
    }

    /**
     * Find the RI coverage information in the cost data cache, and use it to populate the coverage
     * information in the {@link RICostComponentData} object we are assembling.
     *
     * <p>This method requires that the {@link RICostComponentData} object is already partially
     * populated, with the RI specs and RI Bought data.<p/>
     *
     * @param riCostComponentData the {@link RICostComponentData}
     * @param stitchingContext the {@link StitchingContext}
     * @param costDataByTargetIdSnapshot mapping from target oid to the {@link TargetCostData}
     * @param cloudEntitiesMap the {@link CloudEntitiesMap}
     */
    private void addRICoverageData(@Nonnull final RICostComponentData riCostComponentData,
            @Nonnull final StitchingContext stitchingContext,
            @Nonnull final Map<Long, TargetCostData> costDataByTargetIdSnapshot,
            @Nonnull final CloudEntitiesMap cloudEntitiesMap) {
        Objects.requireNonNull(riCostComponentData);
        Objects.requireNonNull(stitchingContext);
        Objects.requireNonNull(costDataByTargetIdSnapshot);
        Objects.requireNonNull(cloudEntitiesMap);
        Objects.requireNonNull(riCostComponentData.riBoughtByLocalId);
        Objects.requireNonNull(riCostComponentData.riSpecs);

        // we will lazily-populate the map of VM local id -> oid mapping,
        // since we should only need it when RI's are in use.
        final Map<String, Long> vmLocalIdToOid = new HashMap<>();
        riCostComponentData.entityLevelReservedInstanceCoverages = new ArrayList<>();
        riCostComponentData.accountLevelReservedInstanceCoverages = new ArrayList<>();
        for (final Entry<Long, TargetCostData> entry : costDataByTargetIdSnapshot.entrySet()) {
            final TargetCostData costData = entry.getValue();
            for (final NonMarketEntityDTO nme : costData.cloudServiceEntities) {
                if (!nme.hasCloudServiceData()) {
                    logger.debug(
                            "Skipping non market entity processing {} because no cloud service data specified.",
                            nme.getId());
                    continue;
                }
                final CloudServiceData csd = nme.getCloudServiceData();
                if (!csd.hasBillingData()) {
                    logger.debug(
                            "Skipping non market entity processing {} because no billing data specified.",
                            nme.getId());
                    continue;
                }
                Long accountOid = null;
                if (csd.hasAccountId()) {
                    accountOid = cloudEntitiesMap.get(csd.getAccountId());
                } else {
                    logger.warn("No account specified for non market entity {}.", nme.getId());
                }

                if (riSupportInPartialCloudEnvironment && accountOid == null) {
                    logger.warn(
                            "Account oid not found for non market entity {}, account level coverage by reserved instances will not be uploaded.",
                            nme.getId());
                }
                final Map<String, Double> accountCoverageByRIs = new HashMap<>();
                final List<EntityRICoverageUpload.Builder> entityRICoverages = new ArrayList<>();
                final BillingData billingData = csd.getBillingData();
                if (!billingData.getVirtualMachinesList().isEmpty() && vmLocalIdToOid.isEmpty()) {
                    logger.debug("Building VM local id -> oid map");
                    stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_MACHINE)
                            .forEach(vm -> {
                                vmLocalIdToOid.put(getBillingId(vm),
                                        vm.getOid());
                            });
                }
                for (final EntityDTO vm : billingData.getVirtualMachinesList()) {
                    final Long entityOid = vmLocalIdToOid.get(vm.getId());
                    EntityRICoverageUpload.Builder entityRICoverageUpload = null;
                    if (entityOid != null) {
                        final Optional<Double> totalCouponsRequired =
                                vm.getCommoditiesSoldList().stream().filter(
                                        c -> c.getCommodityType() == CommodityType.COUPON).map(
                                        CommodityDTO::getCapacity).reduce(Double::sum);
                        if (totalCouponsRequired.isPresent()) {
                            entityRICoverageUpload = EntityRICoverageUpload.newBuilder()
                                    .setEntityId(entityOid)
                                    .setTotalCouponsRequired(totalCouponsRequired.get());
                        }
                    } else {
                        logger.debug(
                                "Could not find virtual machine oid by id {}, entity level coverage by reserved instances will not be uploaded.",
                                vm.getId());
                    }

                    for (final CommodityBought commodityBought : vm.getCommoditiesBoughtList()) {
                        if (commodityBought.getProviderType() != EntityType.RESERVED_INSTANCE) {
                            continue;
                        }
                        for (final CommodityDTO commodity : commodityBought.getBoughtList()) {
                            if (commodity.getCommodityType() != CommodityType.COUPON) {
                                continue;
                            }
                            final String reservedInstanceId = commodityBought.getProviderId();
                            final Long reservedInstanceOid = cloudEntitiesMap.get(
                                    reservedInstanceId);
                            if (reservedInstanceOid == null) {
                                logger.warn("Could not find reserved instance oid by id {}",
                                        reservedInstanceId);
                            }
                            final ReservedInstanceBought.Builder reservedInstanceBought =
                                    riCostComponentData.riBoughtByLocalId.get(reservedInstanceId);
                            if (reservedInstanceBought != null) {
                                final ReservedInstanceBoughtCoupons.Builder
                                        reservedInstanceBoughtCoupons =
                                        reservedInstanceBought
                                                .getReservedInstanceBoughtInfoBuilder()
                                                .getReservedInstanceBoughtCouponsBuilder();
                                reservedInstanceBoughtCoupons
                                        .setNumberOfCouponsUsed(commodity.getUsed()
                                                + reservedInstanceBoughtCoupons.getNumberOfCouponsUsed());
                            }

                            if (entityOid != null) {
                                if (entityRICoverageUpload == null) {
                                    entityRICoverageUpload = EntityRICoverageUpload.newBuilder()
                                            .setEntityId(entityOid);
                                }
                                final Coverage.Builder coverage = Coverage.newBuilder()
                                        .setProbeReservedInstanceId(reservedInstanceId)
                                        .setCoveredCoupons(commodity.getUsed())
                                        .setRiCoverageSource(RICoverageSource.BILLING);
                                setCoverageUsageTimestamps(coverage, billingData);
                                if (reservedInstanceOid != null) {
                                    coverage.setReservedInstanceId(reservedInstanceOid);
                                }
                                entityRICoverageUpload.addCoverage(coverage);
                                logger.debug("Mapping RI {} to account {} vm {}",
                                        reservedInstanceId, csd.getAccountId(), vm.getId());
                            }
                            if (riSupportInPartialCloudEnvironment && accountOid != null) {
                                accountCoverageByRIs.compute(reservedInstanceId,
                                        (probeReservedInstanceId, coveredCoupons) ->
                                                coveredCoupons != null ? coveredCoupons
                                                        + commodity.getUsed()
                                                        : commodity.getUsed());
                                logger.debug("Mapping RI {} to account {}", reservedInstanceId,
                                        csd.getAccountId());
                            }
                        }
                    }
                    if (entityRICoverageUpload != null) {
                        entityRICoverages.add(entityRICoverageUpload);
                    }
                }
                riCostComponentData.entityLevelReservedInstanceCoverages.addAll(entityRICoverages);
                if (accountOid != null && !accountCoverageByRIs.isEmpty()) {
                    final AccountRICoverageUpload.Builder accountRICoverageUpload =
                            AccountRICoverageUpload.newBuilder().setAccountId(accountOid);
                    accountCoverageByRIs.forEach((probeReservedInstanceId, coveredCoupons) -> {
                        final Coverage.Builder coverage =
                                Coverage.newBuilder()
                                        .setProbeReservedInstanceId(probeReservedInstanceId)
                                        .setCoveredCoupons(coveredCoupons)
                                        .setRiCoverageSource(RICoverageSource.BILLING);
                        setCoverageUsageTimestamps(coverage, billingData);
                        final Long reservedInstanceOid = cloudEntitiesMap.get(
                                probeReservedInstanceId);
                        if (reservedInstanceOid != null) {
                            coverage.setReservedInstanceId(reservedInstanceOid);
                        } else {
                            logger.warn("Could not find reserved instance oid by id {}",
                                    probeReservedInstanceId);
                        }
                        accountRICoverageUpload.addCoverage(coverage);
                    });
                    riCostComponentData.accountLevelReservedInstanceCoverages.add(
                            accountRICoverageUpload);
                }
            }
        }
    }

    private static void setCoverageUsageTimestamps(@Nonnull final Coverage.Builder coverage,
            @Nonnull final BillingData billingData) {
        if (billingData.hasBillingWindowStart()) {
            coverage.setUsageStartTimestamp(billingData.getBillingWindowStart());
        }
        if (billingData.hasBillingWindowEnd()) {
            coverage.setUsageEndTimestamp(billingData.getBillingWindowEnd());
        }
    }

    /**
     * Gets the {@link PropertyName#BILLING_ID} from {@link EntityDTO#getEntityPropertiesList()} of
     * {@code vm}, if this property is available, otherwise returns the {@link EntityDTO#getId()}.
     * NOTE: Hack for Azure Subscription and Azure EA probe, which sends different id for the same
     * virtual machine.
     *
     * @param vm the {@link TopologyStitchingEntity}
     * @return the billing id of virtual machine.
     */
    private static String getBillingId(final TopologyStitchingEntity vm) {
        return vm.getEntityBuilder()
            .getEntityPropertiesList().stream()
            .filter(property -> property.getName().equals(PropertyName.BILLING_ID))
            .map(CommonDTO.EntityDTO.EntityProperty::getValue)
            .findAny()
            .orElse(vm.getLocalId());
    }

    /**
     * Holds the RI records that will be sent to the cost component.
     */
    public static class RICostComponentData {
        List<ReservedInstanceSpec> riSpecs;
        Map<String, ReservedInstanceBought.Builder> riBoughtByLocalId;
        List<EntityRICoverageUpload.Builder> entityLevelReservedInstanceCoverages;
        List<AccountRICoverageUpload.Builder> accountLevelReservedInstanceCoverages;
    }
}
