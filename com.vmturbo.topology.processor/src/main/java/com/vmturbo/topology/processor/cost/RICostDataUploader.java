package com.vmturbo.topology.processor.cost;

import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.CLOUD_COST_UPLOAD_TIME;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.MILLIS_PER_YEAR;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.RI_DATA_SECTION;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_BUILD_STAGE;
import static com.vmturbo.topology.processor.cost.DiscoveredCloudCostUploader.UPLOAD_REQUEST_UPLOAD_STAGE;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
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
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.mediation.hybrid.cloud.common.PropertyName;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
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

    private final boolean fullAzureEARIDiscovery;

    public RICostDataUploader(RIAndExpenseUploadServiceBlockingStub costServiceClient,
                              int minimumRIDataUploadIntervalMins, Clock clock,
                              boolean fullAzureEARIDiscovery) {
        this.costServiceClient = costServiceClient;
        if (minimumRIDataUploadIntervalMins < 0) {
            throw new IllegalArgumentException("minimumRIDataUploadIntervalMins cannot be less than 0.");
        }
        this.minimumRIDataUploadIntervalMins = minimumRIDataUploadIntervalMins;
        this.clock = clock;
        this.fullAzureEARIDiscovery = fullAzureEARIDiscovery;
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

        riCostComponentData.riBoughtByLocalId.values().forEach(riBought -> {
            requestBuilder.addReservedInstanceBought(riBought.build());
        });

        riCostComponentData.riCoverages.forEach(riCoverage -> {
            requestBuilder.addReservedInstanceCoverage(riCoverage.build());
        });

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
            requestBuilder.setTopologyId(topologyInfo.getTopologyId());
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
        RICostComponentData riCostComponentData = new RICostComponentData();

        // pull the RI's from the context and transform them into RI specs and bought info for the
        // cost component
        addRISpecsAndBoughtInfo(riCostComponentData, stitchingContext, cloudEntitiesMap);

        // next, get the coverage information
        addRICoverageData(riCostComponentData, stitchingContext, costDataByTargetIdSnapshot, cloudEntitiesMap);

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
    private void addRISpecsAndBoughtInfo(@Nonnull RICostComponentData riCostComponentData,
                                         @Nonnull StitchingContext stitchingContext,
                                         @Nonnull CloudEntitiesMap cloudEntitiesMap) {

        Map<ReservedInstanceSpecInfo, Integer> riSpecInfoToInternalId = new HashMap<>();
        // find all of the RI specs and RI bought info in the stitching context
        // keep a map of RI local id -> RI Bought. We will use this to gradually assemble the
        // RIBought info, which comes from multiple sources.
        Map<String, ReservedInstanceBought.Builder> riBoughtByLocalId = new HashMap<>();

        final Map<Long, TopologyStitchingEntity> businessAccountById = stitchingContext
                .getEntitiesOfType(EntityType.BUSINESS_ACCOUNT)
                .collect(Collectors.toMap(TopologyStitchingEntity::getOid, Function.identity()));

        stitchingContext.getEntitiesOfType(EntityType.RESERVED_INSTANCE)
                .forEach(riStitchingEntity -> {
                    if (riStitchingEntity.getEntityBuilder() == null) {
                        logger.warn("No entity builder found for RI stitching entity {}", riStitchingEntity.getLocalId());
                        return;
                    }

                    final Long baId = cloudEntitiesMap.get(riStitchingEntity.getEntityBuilder()
                            .getReservedInstanceData().getPurchasingAccountId());
                    TopologyStitchingEntity entity = businessAccountById.get(baId);
                    if (entity != null) {
                        boolean dataDiscovered = entity.getEntityBuilder().getBusinessAccountData()
                                .getDataDiscovered();

                        if (!dataDiscovered && !fullAzureEARIDiscovery) {
                            logger.info("Ignoring RI : {} because the associated account information" +
                                    " has not been discovered.", riStitchingEntity.getEntityBuilder()
                                    .getReservedInstanceData().getReservationOrderId());
                            return;
                        }
                    }

                    // create an RI spec based on the details of the entity
                    ReservedInstanceData riData = riStitchingEntity.getEntityBuilder().getReservedInstanceData();
                    double riDuration = (double)riData.getDuration() / MILLIS_PER_YEAR;
                    // We skip partial term Reserved Instances ie RI's whose term is not 1 or 3 years
                    if (riDuration == 1.0 || riDuration == 3.0) {
                    ReservedInstanceSpecInfo.Builder riSpecInfoBuilder = ReservedInstanceSpecInfo.newBuilder()
                            .setType(ReservedInstanceType.newBuilder()
                                    .setOfferingClass(OfferingClass.forNumber(riData.getOfferingClass().getNumber()))
                                    .setPaymentOption(PaymentOption.forNumber(riData.getOfferingType().getNumber()))
                                    // convert duration from millis to years. Can't guarantee precision here, so we will
                                    // round to the nearest number of years.
                                    .setTermYears((int)Math.round((double)riData.getDuration()/MILLIS_PER_YEAR)))
                            .setTenancy(Tenancy.forNumber(riData.getInstanceTenancy().getNumber()))
                            .setOs(CloudCostUtils.platformToOSType(riData.getPlatform()))
                            .setPlatformFlexible(riData.getPlatformFlexible());
                    // haz region?
                    if (riData.hasRegion() && cloudEntitiesMap.containsKey(riData.getRegion())) {
                        riSpecInfoBuilder.setRegionId(cloudEntitiesMap.get(riData.getRegion()));
                    }
                    // haz tier?
                    if (riData.hasRelatedProfileId() && cloudEntitiesMap.containsKey(riData.getRelatedProfileId())) {
                        riSpecInfoBuilder.setTierId(cloudEntitiesMap.get(riData.getRelatedProfileId()));
                    }

                    if (riData.hasInstanceSizeFlexible()) {
                        riSpecInfoBuilder.setSizeFlexible(riData.getInstanceSizeFlexible());
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
                                    .setBusinessAccountId(getOwnerAccountOid(riStitchingEntity, cloudEntitiesMap))
                                    .setProbeReservedInstanceId(riStitchingEntity.getLocalId())
                                    .setStartTime(riData.getStartTime())
                                    .setNumBought(riData.getInstanceCount())
                                    .setReservedInstanceSpec(riSpecInfoToInternalId.get(riSpecInfo))
                                    .setReservedInstanceBoughtCost(riBoughtCost)
                                    .setReservedInstanceBoughtCoupons(riBoughtCoupons)
                            );

                    // Set display name is it is provided in EntityDTO
                    if (riStitchingEntity.getEntityBuilder().hasDisplayName()) {
                        riBought.getReservedInstanceBoughtInfoBuilder().setDisplayName(
                            riStitchingEntity.getDisplayName());
                    }

                    // Set reservation order ID is it is provided in EntityDTO
                    if (riData.hasReservationOrderId()) {
                        riBought.getReservedInstanceBoughtInfoBuilder().setReservationOrderId(
                            riData.getReservationOrderId());
                    }

                    // set AZ, if it exists
                    if (riData.hasAvailabilityZone()
                            && cloudEntitiesMap.containsKey(riData.getAvailabilityZone())) {
                        riBought.getReservedInstanceBoughtInfoBuilder()
                                .setAvailabilityZoneId(cloudEntitiesMap.get(
                                        riData.getAvailabilityZone()));
                    }

                    final ReservedInstanceScopeInfo.Builder scopeInfo = ReservedInstanceScopeInfo
                        .newBuilder();
                    if (riData.hasShared()) {
                        scopeInfo.setShared(riData.getShared());
                        if (riData.getShared() && riData.getAppliedScopesCount() > 0) {
                            logger.warn("Shared RI has {} applied scopes. RI ID is {}.",
                                riData.getAppliedScopesCount(), riStitchingEntity.getLocalId());
                        }
                    }
                    for (final String accountId : riData.getAppliedScopesList()) {
                        final Long accountOid = cloudEntitiesMap.get(accountId);
                        if (accountOid != null) {
                            scopeInfo.addApplicableBusinessAccountId(accountOid);
                        } else {
                            logger.error("Cannot find account from RI applied scopes." +
                                    " RI: {}, Account: {}", riStitchingEntity.getOid(), accountId);
                        }
                    }
                    riBought.getReservedInstanceBoughtInfoBuilder().setReservedInstanceScopeInfo(
                        scopeInfo);

                    riBoughtByLocalId.put(riStitchingEntity.getLocalId(), riBought);
                    }
                });

        // add the extracted data to the cost component data object.
        // create the RISpec objects, setting the spec ids to indices in the spec info list.
        List<ReservedInstanceSpec> riSpecs = new ArrayList<>(riSpecInfoToInternalId.size());
        riSpecInfoToInternalId.forEach((spec, id) ->
            riSpecs.add(ReservedInstanceSpec.newBuilder()
                    .setId(id)
                    .setReservedInstanceSpecInfo(spec)
                    .build())
        );
        riCostComponentData.riSpecs = riSpecs;
        riCostComponentData.riBoughtByLocalId = riBoughtByLocalId;
    }

    private static long getOwnerAccountOid(
            @Nonnull TopologyStitchingEntity riStitchingEntity,
            @Nonnull CloudEntitiesMap cloudEntitiesMap) {
        final ReservedInstanceData riData = riStitchingEntity.getEntityBuilder()
                .getReservedInstanceData();
        // If purchasing account is set in RI data (Azure EA case) then get it from there
        if (riData.hasPurchasingAccountId()) {
            final Long accountId = cloudEntitiesMap.get(riData.getPurchasingAccountId());
            if (accountId != null) {
                return accountId;
            }
        }
        // Otherwise use fallback target account
        return cloudEntitiesMap.getFallbackAccountOid(riStitchingEntity.getTargetId());
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
     * @param cloudEntitiesMap
     */
    private void addRICoverageData(@Nonnull RICostComponentData riCostComponentData,
                                   @Nonnull StitchingContext stitchingContext,
                                   @Nonnull Map<Long, TargetCostData> costDataByTargetIdSnapshot,
                                   @Nonnull CloudEntitiesMap cloudEntitiesMap) {
        // a little more null checking here since we do have a dependency on the data in the RI Cost
        // data so far.
        Objects.requireNonNull(riCostComponentData);
        Objects.requireNonNull(stitchingContext);
        Objects.requireNonNull(costDataByTargetIdSnapshot);
        Objects.requireNonNull(cloudEntitiesMap);
        // ensure we have the data structures in place we need.
        if (riCostComponentData.riBoughtByLocalId == null || riCostComponentData.riSpecs == null) {
            throw new IllegalStateException("Missing RI Bought and/or RI Spec structures.");
        }

        // now comb through the cloud services, and extract the remaining RI-related information from them.
        List<EntityRICoverageUpload.Builder> riCoverages = new ArrayList<>();
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
                        long accountOid = cloudEntitiesMap.getOrDefault(cloudServiceData.getAccountId(),
                                cloudEntitiesMap.getFallbackAccountOid(targetId));
                        if (!cloudEntitiesMap.containsKey(cloudServiceData.getAccountId())) {
                            logger.warn("Account {} not found in stitching data, using fallback account {}.",
                                    cloudServiceData.getAccountId(),
                                    accountOid);
                        }
                        // map RI coverages on any VM's too.
                        billingData.getVirtualMachinesList().forEach(vmEntity -> {
                            // create an EntityRICoverage object if any coupons are traded by this VM
                            EntityRICoverageUpload.Builder entityRIBought =
                                    vmEntity.getCommoditiesSoldList().stream()
                                            .filter(commSold -> CommodityType.COUPON.equals(commSold.getCommodityType()))
                                            .findFirst()
                                            .map(commSold -> {
                                                if (vmLocalIdToOid.isEmpty()) {
                                                    // lazily initialize the vm id map
                                                    logger.debug("Building VM local id -> oid map");
                                                    stitchingContext.getEntitiesOfType(EntityType.VIRTUAL_MACHINE)
                                                            .forEach(vm -> {
                                                                vmLocalIdToOid.put(getBillingId(vm),
                                                                    vm.getOid());
                                                            });
                                                }
                                                if (vmLocalIdToOid.containsKey(vmEntity.getId())) {
                                                    return EntityRICoverageUpload.newBuilder()
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
                                                long riOid = cloudEntitiesMap.getOrDefault(commBought.getProviderId(), 0L);
                                                if (riOid == 0) {
                                                    logger.warn("Couldn't find RI oid for local id {}", commBought.getProviderId());
                                                }
                                                entityRIBought.addCoverage(EntityRICoverageUpload.Coverage.newBuilder()
                                                        .setProbeReservedInstanceId(commBought.getProviderId())
                                                        .setCoveredCoupons(commodityDTO.getUsed())
                                                        .setRiCoverageSource(EntityRICoverageUpload.Coverage.RICoverageSource.BILLING));
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

    private static String getBillingId(TopologyStitchingEntity vm) {
        return vm.getEntityBuilder()
            .getEntityPropertiesList().stream()
            .filter(property -> property.getName().equals(PropertyName.BILLING_ID))
            .map(CommonDTO.EntityDTO.EntityProperty::getValue)
            .findAny()
            .orElse(vm.getLocalId());
    }

    /**
     * Holds the RI records that will be sent to the Cost component
     */
    public static class RICostComponentData {
        List<ReservedInstanceSpec> riSpecs;

        Map<String, ReservedInstanceBought.Builder> riBoughtByLocalId;

        List<EntityRICoverageUpload.Builder> riCoverages;

    }

}
