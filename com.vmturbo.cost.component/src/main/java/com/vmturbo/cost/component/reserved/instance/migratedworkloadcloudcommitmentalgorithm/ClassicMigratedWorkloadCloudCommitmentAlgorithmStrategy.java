package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceDerivedCost;
import com.vmturbo.common.protobuf.cost.Pricing;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RIProviderSetting;
import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.component.db.tables.pojos.ActionContextRiBuy;
import com.vmturbo.cost.component.db.tables.pojos.BuyReservedInstance;
import com.vmturbo.cost.component.db.tables.pojos.PlanProjectedEntityToReservedInstanceMapping;
import com.vmturbo.cost.component.db.tables.pojos.PlanReservedInstanceBought;
import com.vmturbo.cost.component.db.tables.records.ActionContextRiBuyRecord;
import com.vmturbo.cost.component.db.tables.records.BuyReservedInstanceRecord;
import com.vmturbo.cost.component.db.tables.records.PlanProjectedEntityToReservedInstanceMappingRecord;
import com.vmturbo.cost.component.db.tables.records.PlanReservedInstanceBoughtRecord;
import com.vmturbo.cost.component.history.HistoricalStatsService;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanActionContextRiBuyStore;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanBuyReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanProjectedEntityToReservedInstanceMappingStore;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.repository.PlanReservedInstanceSpecStore;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO;

/**
 * MigratedWorkloadCloudCommitmentAlgorithmStrategy that replicates the strategy used in the classic OpsManager.
 */
@Service
public class ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy implements MigratedWorkloadCloudCommitmentAlgorithmStrategy {
    private static final Logger logger = LogManager.getLogger(ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy.class);

    /**
     * The historical service, from which we retrieve historical VM vCPU usage.
     */
    private HistoricalStatsService historicalStatsService;

    /**
     * Provides access to the price table, which has on-demand and reserved instance prices. We use this information when
     * calculating the savings in our actions.
     */
    private PriceTableStore priceTableStore;

    /**
     * Provides access to the business account price table key store. We use this information to retrieve the price table
     * key for the specified business account OID.
     */
    private BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore;

    /**
     * Provides access to the reserved instance spec table. We use this repository to look up the RI spec for each workload
     * for which we want to create a Buy RI action.
     */
    private PlanReservedInstanceSpecStore planReservedInstanceSpecStore;

    /**
     * Provides access to the buy_reserved_instance database table. We use this to insert a new BuyReservedInstance
     * record for each Buy RI action that we create.
     */
    private PlanBuyReservedInstanceStore planBuyReservedInstanceStore;

    /**
     * Provides access to the action_context_buy_ri database table.
     */
    private PlanActionContextRiBuyStore planActionContextRiBuyStore;

    /**
     * Provides access to the plan_projected_entity_to_reserved_instance_mapping database table.
     */
    private PlanProjectedEntityToReservedInstanceMappingStore planProjectedEntityToReservedInstanceMappingStore;

    private PlanReservedInstanceBoughtStore planReservedInstanceBoughtStore;

    /**
     * Create a new ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy.
     *
     * @param historicalStatsService            The historical status service that will be used to retrieve historical VCPU metrics
     * @param priceTableStore                   The price table store, from which we retrieve on-demand and reserved instance prices
     * @param businessAccountPriceTableKeyStore Used to map the business account to a price table key
     * @param planBuyReservedInstanceStore      Used to create a record in the buy_reserved_instance database table
     * @param planReservedInstanceSpecStore     Used to match a reserved instance specification to its reserved instance spec record
     * @param planActionContextRiBuyStore       Used to add a record to the action_context_ri_buy database table
     * @param planProjectedEntityToReservedInstanceMappingStore Used to add a record to the plan_projected_entity_to_reserved_instance_mapping table
     * @param planReservedInstanceBoughtStore   Used to add a record to the plan_reserved_instance_bought table
     */
    public ClassicMigratedWorkloadCloudCommitmentAlgorithmStrategy(HistoricalStatsService historicalStatsService,
                                                                   PriceTableStore priceTableStore,
                                                                   BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore,
                                                                   PlanBuyReservedInstanceStore planBuyReservedInstanceStore,
                                                                   PlanReservedInstanceSpecStore planReservedInstanceSpecStore,
                                                                   PlanActionContextRiBuyStore planActionContextRiBuyStore,
                                                                   PlanProjectedEntityToReservedInstanceMappingStore planProjectedEntityToReservedInstanceMappingStore,
                                                                   PlanReservedInstanceBoughtStore planReservedInstanceBoughtStore) {
        this.historicalStatsService = historicalStatsService;
        this.priceTableStore = priceTableStore;
        this.planBuyReservedInstanceStore = planBuyReservedInstanceStore;
        this.businessAccountPriceTableKeyStore = businessAccountPriceTableKeyStore;
        this.planReservedInstanceSpecStore = planReservedInstanceSpecStore;
        this.planActionContextRiBuyStore = planActionContextRiBuyStore;
        this.planProjectedEntityToReservedInstanceMappingStore = planProjectedEntityToReservedInstanceMappingStore;
        this.planReservedInstanceBoughtStore = planReservedInstanceBoughtStore;
    }

    /**
     * The number of historical days to analyze VM usage.
     */
    @Value("${migratedWorkflowCloudCommitmentAnalysis.numberOfHistoricalDays:21}")
    private int numberOfHistoricalDays;

    /**
     * The vCPU usage threshold: the VM is considered "active" for a day if its vCPU -> Used -> Max is greater than
     * this threshold. The default value is 20%.
     */
    @Value("${migratedWorkflowCloudCommitmentAnalysis.commodityThreshold:20}")
    private int commodityThreshold;

    /**
     * The percentage of time that this VM must be "active" in the specified number of historical days for us to
     * recommend buying an RI. The default value is 80%.
     */
    @Value("${migratedWorkflowCloudCommitmentAnalysis.activeDaysThreshold:80}")
    private int activeDaysThreshold;

    /**
     * Performs the analysis of our input data and generates Buy RI recommendations.
     *
     * @param migratedWorkloads The workloads that are being migrated as part of a migrate to cloud plan
     * @return A list of Buy RI actions for these workloads
     */
    @Override
    public List<ActionDTO.Action> analyze(List<MigratedWorkloadPlacement> migratedWorkloads,
                                          Long masterBusinessAccountOid,
                                          CloudType cloudType,
                                          RIProviderSetting riProviderSetting,
                                          Long topologyContextId) {
        logger.info("Starting Buy RI Analysis for plan {}", topologyContextId);

        // Extract the list of OIDs to analyze; TODO: filter out VMs already using an RI
        List<Long> oids = migratedWorkloads.stream()
                .map(workload -> workload.getVirtualMachine().getOid())
                .collect(Collectors.toList());

        // Retrieve historical statistics for our VMs
        List<Stats.EntityStats> stats = historicalStatsService.getHistoricalStats(oids, Arrays.asList("VCPU"), numberOfHistoricalDays);

        // Analyze the historical statistics to determine the OIDs for which to buy RIs
        List<Long> oidsForWhichToBuyRIs = analyzeStatistics(stats);
        logger.info("Buy RIs for OIDs: {}", oidsForWhichToBuyRIs);

        try {
            // Create actions
            List<ActionDTO.Action> actions = createActions(oidsForWhichToBuyRIs,
                    migratedWorkloads,
                    masterBusinessAccountOid,
                    cloudType,
                    riProviderSetting,
                    topologyContextId);

            logger.info("Buy RI Analysis completed successfully for plan {}", topologyContextId);

            // Return the list of actions
            return actions;
        } catch (MigratedWorkloadCloudCommitmentAlgorithmException e) {
            return new ArrayList<>();
        }
    }

    /**
     * Analyzes the list of EntityStats to determine for which OIDs we should buy RIs.
     *
     * @param stats A list of EntityStats, from the historical stats service
     * @return A list of OIDs for which we should buy RIs
     */
    @VisibleForTesting
    private List<Long> analyzeStatistics(List<Stats.EntityStats> stats) {
        // Determine the minimum number of days that a VM must be active
        int minimumNumberOfDaysActive = (int)((double)activeDaysThreshold / 100 * numberOfHistoricalDays);

        // Capture the OIDs of the VMs for which we want to buy an RI
        List<Long> oidsForWhichToBuyRIs = new ArrayList<>();

        // Iterate over the stats for each VM
        for (Stats.EntityStats stat : stats) {
            // Count the number of "active" days for this VM
            int activeDays = 0;

            // Each EntityStats will have one snapshot for each day from the history service
            for (Stats.StatSnapshot snapshot : stat.getStatSnapshotsList()) {
                List<Stats.StatSnapshot.StatRecord> records = snapshot.getStatRecordsList();
                if (CollectionUtils.isNotEmpty(records)) {
                    // Each snapshot should have a single StatRecord
                    Stats.StatSnapshot.StatRecord.StatValue usedValue = records.get(0).getUsed();
                    if (usedValue != null) {
                        // Compare the used -> max value to our commodity threshold
                        if (usedValue.getMax() > commodityThreshold) {
                            activeDays++;
                        }
                    }
                }
            }

            // If the number of active days is above our minimum then add it to our list
            logger.info("{} - number of days active: {}, minimum days required: {}", stat.getOid(), activeDays, minimumNumberOfDaysActive);
            if (activeDays >= minimumNumberOfDaysActive) {
                oidsForWhichToBuyRIs.add(stat.getOid());
            }
        }

        // Return the list of OIDs for which to buy RIs
        return oidsForWhichToBuyRIs;
    }

    /**
     * Creates a list of actions for the specified OIDs from the list of migrated workload placements.
     *
     * @param oidsForWhichToBuyRIs     A list of the OIDs for which to create a Buy RI action
     * @param migratedWorkloads        The list of migrated workloads that contain these OIDs
     * @param masterBusinessAccountOid The business account for which to buy RIs
     * @param cloudType                The cloud service provider
     * @param riProviderSetting        The RI provider settings to use when buying RIs
     * @param topologyContextId        The topology context ID with which to associate the actions
     * @return A list of Buy RI actions
     * @throws MigratedWorkloadCloudCommitmentAlgorithmException If the actions could not be created
     */
    @VisibleForTesting
    private List<ActionDTO.Action> createActions(List<Long> oidsForWhichToBuyRIs,
                                                 List<MigratedWorkloadPlacement> migratedWorkloads,
                                                 Long masterBusinessAccountOid,
                                                 CloudType cloudType,
                                                 RIProviderSetting riProviderSetting,
                                                 Long topologyContextId) throws MigratedWorkloadCloudCommitmentAlgorithmException {
        // Retrieve our price tables
        Long priceTableKey = getPriceTableKey(masterBusinessAccountOid).orElseThrow(MigratedWorkloadCloudCommitmentAlgorithmException::new);
        Pricing.PriceTable priceTable = getPriceTable(priceTableKey).orElseThrow(MigratedWorkloadCloudCommitmentAlgorithmException::new);
        Pricing.ReservedInstancePriceTable riPriceTable = getRIPriceTable(priceTableKey).orElseThrow(MigratedWorkloadCloudCommitmentAlgorithmException::new);

        // Create actions
        List<ActionDTO.Action> actions = new ArrayList<>();
        oidsForWhichToBuyRIs.forEach(oid -> {
            // Find the workload with the specified OID
            Optional<MigratedWorkloadPlacement> migratedWorkloadPlacement = migratedWorkloads.stream()
                    .filter(workload -> workload.getVirtualMachine().getOid() == oid)
                    .findFirst();

            if (!migratedWorkloadPlacement.isPresent()) {
                // This should never happen because we're extracting the OID from the migrated workload list, but
                // just in case, log a warning message
                logger.warn("Could not find migrated workload placement for VM with OID {} when trying to create a Buy RI action", oid);
            }

            // Add an action for that workload
            migratedWorkloadPlacement.ifPresent(placement -> {
                logger.info("Buy RI for VM {} (oid={}) - Compute Tier: {}, Region: {}",
                        placement.getVirtualMachine().getDisplayName(),
                        placement.getVirtualMachine().getOid(),
                        placement.getComputeTier().getDisplayName(),
                        placement.getRegion().getDisplayName());

                try {
                    // Add a new action to our list
                    createAction(placement, masterBusinessAccountOid, priceTable, riPriceTable, cloudType, riProviderSetting, topologyContextId).ifPresent(actions::add);
                } catch (MigratedWorkloadCloudCommitmentAlgorithmException e) {
                    logger.warn("Unable to create a Buy RI action for VM: {} to compute tier: {} in region {}",
                            placement.getVirtualMachine().getOid(), placement.getComputeTier().getOid(), placement.getRegion().getOid());
                }
            });
        });

        // Return our actions
        return actions;
    }

    /**
     * Returns the price table key for the specified business account.
     *
     * @param masterBusinessAccountOid The business account for which to retrieve the price table key.
     * @return The price table key for the specified business account
     */
    private Optional<Long> getPriceTableKey(Long masterBusinessAccountOid) {
        // Lookup the on-demand price table key from the business account price table key store
        Map<Long, Long> priceTableKeys = businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(ImmutableSet.of(masterBusinessAccountOid));
        if (!priceTableKeys.containsKey(masterBusinessAccountOid)) {
            logger.warn("Unable to find the price table key for business account: {}. Cannot generate Buy RI actions.", masterBusinessAccountOid);
            return Optional.empty();
        }

        // Return the price table key
        return Optional.of(priceTableKeys.get(masterBusinessAccountOid));
    }

    /**
     * Returns the price table with the specified price table key.
     *
     * @param priceTableKey The key for which to retrieve the price table
     * @return The price table for the specified business account.
     */
    private Optional<Pricing.PriceTable> getPriceTable(Long priceTableKey) {
        // Get the price table associated with this key
        Map<Long, Pricing.PriceTable> priceTables = priceTableStore.getPriceTables(Arrays.asList(priceTableKey));
        if (!priceTables.containsKey(priceTableKey)) {
            logger.warn("Unable to find the price table record for price table key: {}. Cannot generate Buy RI actions.",
                    priceTableKey);
            return Optional.empty();
        }

        // Return the price table
        return Optional.of(priceTables.get(priceTableKey));
    }

    /**
     * Returns the reserved instance price table with the specified price table key.
     *
     * @param priceTableKey The key for which to retrieve the reserved instance price table
     * @return The reserved instance price table for the specified business account.
     */
    private Optional<Pricing.ReservedInstancePriceTable> getRIPriceTable(Long priceTableKey) {
        // Lookup the RI price table
        Map<Long, Pricing.ReservedInstancePriceTable> riPriceTables = priceTableStore.getRiPriceTables(Arrays.asList(priceTableKey));
        if (!riPriceTables.containsKey(priceTableKey)) {
            logger.warn("Unable to find the reserved instance price table for price table key: {}. Cannot generate Buy RI actions.",
                    priceTableKey);
            return Optional.empty();
        }

        // Return the reserved instance price table
        return Optional.of(riPriceTables.get(priceTableKey));
    }

    /**
     * Creates a Buy RI action for the specified migrated workload placement.
     *
     * @param placement                The migrated workload placement for which to create a Buy RI action
     * @param masterBusinessAccountOid The master business account for which to buy the RI
     * @param priceTable               The price table that contains on-demand prices
     * @param riPriceTable             The reserved instance price table that contains RI prices
     * @param cloudType                The cloud service provider
     * @param riProviderSetting        The RI provider settings to use to create the Buy RI actions
     * @param topologyContextId        The topology context ID with which to associate the actions
     * @return A Buy RI action
     * @throws MigratedWorkloadCloudCommitmentAlgorithmException If the action could not be created
     */
    @VisibleForTesting
    private Optional<ActionDTO.Action> createAction(MigratedWorkloadPlacement placement,
                                                    Long masterBusinessAccountOid,
                                                    Pricing.PriceTable priceTable,
                                                    Pricing.ReservedInstancePriceTable riPriceTable,
                                                    CloudType cloudType,
                                                    RIProviderSetting riProviderSetting,
                                                    Long topologyContextId) throws MigratedWorkloadCloudCommitmentAlgorithmException {
        // Retrieve the costs for migrating this workload: on-demand and various RI costs
        CostRecord costRecord = getCosts(placement, priceTable, riPriceTable, cloudType, riProviderSetting).orElseThrow(MigratedWorkloadCloudCommitmentAlgorithmException::new);

        // Build our BuyRI action info
        ActionDTO.BuyRI buyRI = ActionDTO.BuyRI.newBuilder()
                .setBuyRiId(IdentityGenerator.next())  // TODO: determine what to put here
                .setComputeTier(ActionDTO.ActionEntity.newBuilder()
                        .setId(placement.getComputeTier().getOid())
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .setType(placement.getComputeTier().getEntityType())
                        .build())
                .setCount(1)
                .setRegion(ActionDTO.ActionEntity.newBuilder()
                        .setId(placement.getRegion().getOid())
                        .setType(CommonDTO.EntityDTO.EntityType.REGION_VALUE)
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build())
                .setMasterAccount(ActionDTO.ActionEntity.newBuilder()
                        .setId(masterBusinessAccountOid)
                        .setType(CommonDTO.EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE)
                        .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                        .build())
                .build();

        // Build our explanation
        ActionDTO.Explanation explanation = ActionDTO.Explanation.newBuilder()
                .setBuyRI(ActionDTO.Explanation.BuyRIExplanation.newBuilder()
                        .setCoveredAverageDemand(100f)
                        .setTotalAverageDemand(100f)
                        // Shown as the estimated on-demand cost per term
                        .setEstimatedOnDemandCost((float)costRecord.calculateOnDemandCostForTerm())
                        .build())
                .build();

        // Create the action
        ActionDTO.Action action = ActionDTO.Action.newBuilder()
                .setId(IdentityGenerator.next())
                .setInfo(ActionDTO.ActionInfo.newBuilder()
                        .setBuyRi(buyRI)
                        .build())
                .setExplanation(explanation)
                .setDeprecatedImportance(0)
                .setSupportingLevel(ActionDTO.Action.SupportLevel.SHOW_ONLY)
                .setSavingsPerHour(CloudCostDTO.CurrencyAmount.newBuilder()
                        .setAmount(costRecord.calculateSavingsPerHour())
                        .build())
                .setExecutable(false)
                .build();

        // Create the BuyReservationInstance in the database
        createBuyReservedInstanceDbRecord(action, costRecord, topologyContextId, masterBusinessAccountOid, placement.getVirtualMachine().getOid());

        // Create the ActionContextRiBuy record in the database
        createActionContextRiBuy(placement, action, topologyContextId);

        // Create plan reserved instance bought
        long riBoughtId = IdentityGenerator.next();
        createPlanReservedInstanceBought(
                riBoughtId,
                topologyContextId,
                costRecord.getRiSpecId(),
                ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                                .setNumberOfCoupons(costRecord.getNumberOfCoupons())
                                .setNumberOfCouponsUsed(costRecord.getNumberOfCoupons())
                                .build())
                        .setReservedInstanceDerivedCost(ReservedInstanceDerivedCost.newBuilder()
                                .setOnDemandRatePerHour(CurrencyAmount.newBuilder()
                                        .setAmount(costRecord.getOnDemandPrice())
                                        .build())
                                .setAmortizedCostPerHour(CurrencyAmount.newBuilder()
                                        .setAmount(costRecord.getAmortizedHourlyCost())
                                        .build())
                                .build())
                        .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                                .setRecurringCostPerHour(CurrencyAmount.newBuilder()
                                        .setAmount(costRecord.getOnDemandPrice())
                                        .build())
                                .setUsageCostPerHour(CurrencyAmount.newBuilder()
                                        .setAmount(costRecord.getRecurringPrice())
                                        .build())
                                .setFixedCost(CurrencyAmount.newBuilder()
                                        .setAmount(costRecord.getUpFrontPrice())
                                        .build())
                                .build())
                        .setBusinessAccountId(masterBusinessAccountOid)
                        .setNumBought(1)
                        .setReservedInstanceSpec(costRecord.getRiSpecId())
                        .setToBuy(true)
                        .build(),
                1,
                costRecord.getUpFrontPrice(),
                costRecord.getRecurringPrice(),
                costRecord.getAmortizedHourlyCost());

        // Create the PlanProjectedEntityToReservedInstanceMapping record in the database
        createPlanProjectedEntityToRIMappingRecord(placement.getVirtualMachine().getOid(), topologyContextId, riBoughtId, costRecord.getNumberOfCoupons());

        // Return the constructed action
        return Optional.of(action);
    }

    private void createActionContextRiBuy(MigratedWorkloadPlacement placement,
                                          ActionDTO.Action action,
                                          Long topologyContextId) {
        ActionContextRiBuy actionContextRiBuy = new ActionContextRiBuy();
        actionContextRiBuy.setActionId(action.getId());
        actionContextRiBuy.setPlanId(topologyContextId);
        actionContextRiBuy.setTemplateType(placement.getComputeTier().getDisplayName());
        actionContextRiBuy.setTemplateFamily(placement.getComputeTier().getTypeSpecificInfo().getComputeTier().getFamily());

        ActionContextRiBuyRecord actionContextRiBuyRecord = planActionContextRiBuyStore.save(actionContextRiBuy);
        logger.debug("Created ActionContextRiBuyRecord: {}", actionContextRiBuyRecord);
    }

    /**
     * Creates a BuyReservedInstance and inserts it into the database.
     *
     * @param action                   The action for which to create the BuyReservedInstance record
     * @param costRecord               The cost record that contains the various on-demand and RI costs
     * @param topologyContextId        The topologyContextId with which the BuyReservedInstance record should be associated
     * @param masterBusinessAccountOid The business account with which the BuyReservedInstance should be associated
     * @param vmId                     The VM for which we are recommending buying this RI
     */
    private void createBuyReservedInstanceDbRecord(ActionDTO.Action action, CostRecord costRecord, Long topologyContextId, Long masterBusinessAccountOid, Long vmId) {
        // Create the BuyReservationInstance in the database
        BuyReservedInstance buyReservedInstance = new BuyReservedInstance(
                action.getInfo().getBuyRi().getBuyRiId(),
                topologyContextId,
                masterBusinessAccountOid,
                action.getInfo().getBuyRi().getRegion().getId(),
                costRecord.getRiSpecId(),
                1, // Count
                Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceSpec(costRecord.getRiSpecId())
                        .setNumBought(1)
                        .setReservedInstanceBoughtCoupons(Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons.newBuilder()
                                .setNumberOfCoupons(costRecord.getNumberOfCoupons())
                                .setNumberOfCouponsUsed(costRecord.getNumberOfCoupons())
                                .build()) // Assume we're using all of the coupons.
                        .setReservedInstanceDerivedCost(Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceDerivedCost.newBuilder()
                                .setAmortizedCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder()
                                        .setAmount(costRecord.getAmortizedHourlyCost())
                                        .build())
                                .setOnDemandRatePerHour(CloudCostDTO.CurrencyAmount.newBuilder()
                                        .setAmount(costRecord.getOnDemandPrice())
                                        .build())
                                .build())
                        .setBusinessAccountId(masterBusinessAccountOid)
                        .setReservedInstanceBoughtCost(Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost.newBuilder()
                                .setFixedCost(CloudCostDTO.CurrencyAmount.newBuilder()
                                        .setAmount(costRecord.getUpFrontPrice())
                                        .build())
                                .setUsageCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder()
                                        .setAmount(costRecord.getUsagePrice())
                                        .build())
                                .setRecurringCostPerHour(CloudCostDTO.CurrencyAmount.newBuilder()
                                        .setAmount(costRecord.getRecurringPrice())
                                        .build())
                                .build())
                        .setToBuy(true)
                        .build(),
                costRecord.getUpFrontPrice(),
                costRecord.getRecurringPrice(),
                costRecord.getAmortizedHourlyCost());

        // Save the BuyReservedInstance to the database
        BuyReservedInstanceRecord buyReservedInstanceRecord = planBuyReservedInstanceStore.save(buyReservedInstance);
        logger.debug("Created BuyReservedInstanceRecord: {}", buyReservedInstanceRecord);
    }

    /**
     * Creates a record in the plan_reserved_instance_bought table.
     *
     * @param id                                The ID of this record
     * @param planId                            The plan for which this reserved instance was bought
     * @param reservedInstanceSpecId            The spec ID of this reserved instance
     * @param reservedInstanceBoughtInfo        Information about the RI that was bought
     * @param count                             The number of RIs that were purchased
     * @param perInstanceFixedCost              The upfront cost of the RI
     * @param perInstanceRecurringCostHourly    The recurring hourly cost of this RI
     * @param perInstanceAmortizedCostHourly    The hourly cost of running this RI amortized over the term
     */
    private void createPlanReservedInstanceBought(long id,
                                                  long planId,
                                                  long reservedInstanceSpecId,
                                                  ReservedInstanceBoughtInfo reservedInstanceBoughtInfo,
                                                  int count,
                                                  double perInstanceFixedCost,
                                                  double perInstanceRecurringCostHourly,
                                                  double perInstanceAmortizedCostHourly) {
        PlanReservedInstanceBought planReservedInstanceBought = new PlanReservedInstanceBought(id, planId, reservedInstanceSpecId, reservedInstanceBoughtInfo, count,
                perInstanceFixedCost, perInstanceRecurringCostHourly, perInstanceAmortizedCostHourly);

        PlanReservedInstanceBoughtRecord record = planReservedInstanceBoughtStore.save(planReservedInstanceBought);
        logger.debug("Created PlanReservedInstanceBoughtRecord: {}", record);
    }

    /**
     * Creates a new PlanProjectedEntityToReservedInstanceMappingRecord in the database.
     *
     * @param entityId           The VM entity OID
     * @param planId             The plan OID
     * @param reservedInstanceId The reserved instance OID it is buying
     * @param usedCoupons        The number of coupons it is using
     */
    private void createPlanProjectedEntityToRIMappingRecord(long entityId, long planId, long reservedInstanceId, double usedCoupons) {
        PlanProjectedEntityToReservedInstanceMapping planProjectedEntityToReservedInstanceMapping =
                new PlanProjectedEntityToReservedInstanceMapping(entityId, planId, reservedInstanceId, usedCoupons);

        PlanProjectedEntityToReservedInstanceMappingRecord record = planProjectedEntityToReservedInstanceMappingStore.save(planProjectedEntityToReservedInstanceMapping);
        logger.debug("Created PlanProjectedEntityToReservedInstanceMappingRecord: {}", record);
    }

    /**
     * Retrieves the cost information for the specified placement, using the provided price table, RI price table,
     * and migration profile.
     *
     * @param placement         The workload placement for which to retrieve cost information
     * @param priceTable        The on demand price table
     * @param riPriceTable      The RI price table
     * @param cloudType         The cloud service provider
     * @param riProviderSetting The RI provider settings, specifying the type of RI to buy
     * @return A CostRecord that contains all of the relevant fields
     */
    private Optional<CostRecord> getCosts(MigratedWorkloadPlacement placement,
                                          Pricing.PriceTable priceTable,
                                          Pricing.ReservedInstancePriceTable riPriceTable,
                                          CloudType cloudType,
                                          RIProviderSetting riProviderSetting) {
        // Create a cost record to hold the cost information we discover
        CostRecord costRecord = new CostRecord();

        // Get the virtual machine operating system
        costRecord.setOsType(placement.getVirtualMachine().getTypeSpecificInfo().getVirtualMachine().getGuestOsInfo().getGuestOsType());

        // Lookup costs. TODO: add null checks
        Pricing.OnDemandPriceTable onDemandPriceTable = priceTable.getOnDemandPriceByRegionIdMap().get(placement.getRegion().getOid());
        PricingDTO.ComputeTierPriceList computeTierPriceList = onDemandPriceTable.getComputePricesByTierIdMap().get(placement.getComputeTier().getOid());
        costRecord.setOnDemandPrice(computeTierPriceList.getBasePrice().getPricesList().get(0).getPriceAmount().getAmount());
        costRecord.setNumberOfCoupons(placement.getComputeTier().getTypeSpecificInfo().getComputeTier().getNumCoupons());

        // TODO: Get on-demand license cost, not presently in the price table for this compute tier

        // Find the reserved instance spec for the RI we want to buy
        List<Cost.ReservedInstanceSpec> riSpecs = new ArrayList<>();
        if (cloudType == CloudType.AWS) {
            riSpecs = planReservedInstanceSpecStore.getReservedInstanceSpecs(
                    placement.getRegion().getOid(),
                    placement.getComputeTier().getOid(),
                    riProviderSetting.getPreferredOfferingClass(),
                    riProviderSetting.getPreferredPaymentOption(),
                    riProviderSetting.getPreferredTerm(),
                    CloudCostDTO.Tenancy.DEFAULT,
                    costRecord.getOsType());
        } else if (cloudType == CloudType.AZURE) {
            riSpecs = planReservedInstanceSpecStore.getReservedInstanceSpecs(
                    placement.getRegion().getOid(),
                    placement.getComputeTier().getOid(),
                    riProviderSetting.getPreferredTerm());
        } else {
            logger.error("Unknown cloud type: {}, cannot lookup RI specification for which to buy RIs", cloudType);
            return Optional.empty();
        }

        if (riSpecs.size() == 0) {
            logger.warn("Could not find RI for: region={}, compute tier={}, offering class={}, payment option={}, term={}, tenancy={}, os={}",
                    placement.getRegion().getOid(),
                    placement.getComputeTier().getOid(),
                    riProviderSetting.getPreferredOfferingClass(),
                    riProviderSetting.getPreferredPaymentOption(),
                    riProviderSetting.getPreferredTerm(),
                    CloudCostDTO.Tenancy.DEFAULT,
                    costRecord.getOsType());
            return Optional.empty();
        }
        costRecord.setRiSpecId(riSpecs.get(0).getId());
        costRecord.setTerm(riProviderSetting.getPreferredTerm());

        // Get RI costs
        PricingDTO.ReservedInstancePrice reservedInstancePrice = riPriceTable.getRiPricesBySpecIdMap().get(costRecord.getRiSpecId());
        costRecord.setUpFrontPrice(reservedInstancePrice.getUpfrontPrice().getPriceAmount().getAmount());
        costRecord.setRecurringPrice(reservedInstancePrice.getRecurringPrice().getPriceAmount().getAmount());
        costRecord.setUsagePrice(reservedInstancePrice.getUsagePrice().getPriceAmount().getAmount());

        // Compute the amortized hourly cost
        costRecord.setAmortizedHourlyCost(costRecord.getUpFrontPrice() / (riProviderSetting.getPreferredTerm() * 365 * 24) + costRecord.getRecurringPrice());

        // Return the cost record
        return Optional.of(costRecord);
    }

    /**
     * Helper class that wraps cost information.
     */
    private class CostRecord {
        private CloudCostDTO.OSType osType;
        private double onDemandPrice;
        private double onDemandLicencePrice;
        private int numberOfCoupons;
        private double upFrontPrice;
        private double recurringPrice;
        private double usagePrice;
        private double amortizedHourlyCost;
        private int term;
        private long riSpecId;

        CostRecord() {
        }

        public CloudCostDTO.OSType getOsType() {
            return osType;
        }

        public CostRecord setOsType(CloudCostDTO.OSType osType) {
            this.osType = osType;
            return this;
        }

        public double getOnDemandPrice() {
            return onDemandPrice;
        }

        public void setOnDemandPrice(double onDemandPrice) {
            this.onDemandPrice = onDemandPrice;
        }

        public double getOnDemandLicencePrice() {
            return onDemandLicencePrice;
        }

        public void setOnDemandLicencePrice(double onDemandLicencePrice) {
            this.onDemandLicencePrice = onDemandLicencePrice;
        }

        public int getNumberOfCoupons() {
            return numberOfCoupons;
        }

        public void setNumberOfCoupons(int numberOfCoupons) {
            this.numberOfCoupons = numberOfCoupons;
        }

        public double getUpFrontPrice() {
            return upFrontPrice;
        }

        public void setUpFrontPrice(double upFrontPrice) {
            this.upFrontPrice = upFrontPrice;
        }

        public double getRecurringPrice() {
            return recurringPrice;
        }

        public void setRecurringPrice(double recurringPrice) {
            this.recurringPrice = recurringPrice;
        }

        public double getUsagePrice() {
            return usagePrice;
        }

        public void setUsagePrice(double usagePrice) {
            this.usagePrice = usagePrice;
        }

        public double getAmortizedHourlyCost() {
            return amortizedHourlyCost;
        }

        public void setAmortizedHourlyCost(double amortizedHourlyCost) {
            this.amortizedHourlyCost = amortizedHourlyCost;
        }

        public int getTerm() {
            return term;
        }

        public void setTerm(int term) {
            this.term = term;
        }

        public long getRiSpecId() {
            return riSpecId;
        }

        public void setRiSpecId(long riSpecId) {
            this.riSpecId = riSpecId;
        }

        public double calculateSavingsPerHour() {
            return onDemandPrice - amortizedHourlyCost;
        }

        public double calculateOnDemandCostForTerm() {
            return onDemandPrice * 24 * 365 * term;
        }
    }
}
