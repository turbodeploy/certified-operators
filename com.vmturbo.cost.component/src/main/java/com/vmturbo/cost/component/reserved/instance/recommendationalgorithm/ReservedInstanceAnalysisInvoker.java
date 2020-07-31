package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RIProviderSetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.setting.CategoryPathConstants;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredTerm;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.group.api.SettingMessages.SettingNotification;
import com.vmturbo.group.api.SettingsListener;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Invokes the RI Instance Analysis.
 * Called:
 * - when the cost component is started and later on after regular intervals.
 * - when the RI inventory changes.
 * - when RI Buy Settings change.
 * - when RI or On-Demand template prices change.
 */
public class ReservedInstanceAnalysisInvoker implements SettingsListener {
    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceAnalyzer reservedInstanceAnalyzer;

    private final RepositoryServiceBlockingStub repositoryClient;

    private final SettingServiceBlockingStub settingsServiceClient;

    private final long realtimeTopologyContextId;

    // The inventory of RIs that have already been purchased
    private final ReservedInstanceBoughtStore riBoughtStore;

    private final BusinessAccountPriceTableKeyStore prTabKeyStore;
    private final PriceTableStore prTabStore;

    private static List<String> riSettingNames = new ArrayList<>();

    private boolean enableRIBuyAfterPricingChange;

    private boolean disableRealtimeRIBuyAnalysis;


    /**
     * List of Business Accounts with associated pricing information.
     */
    private final Set<BizAccPriceRecord> businessAccountsWithCost = new HashSet<>();

    private Optional<CloudTopology<TopologyEntityDTO>> cloudTopology = Optional.empty();

    private boolean runBuyRIOnNextBroadcast = false;

    static  {
        for (GlobalSettingSpecs globalSettingSpecs : GlobalSettingSpecs.values()) {
            if (globalSettingSpecs.getCategoryPaths().contains(CategoryPathConstants.RI)) {
                riSettingNames.add(globalSettingSpecs.getSettingName());
            }
        }
    }

    public ReservedInstanceAnalysisInvoker(@Nonnull ReservedInstanceAnalyzer reservedInstanceAnalyzer,
                                           @Nonnull RepositoryServiceBlockingStub repositoryClient,
                                           @Nonnull SettingServiceBlockingStub settingsServiceClient,
                                           @Nonnull ReservedInstanceBoughtStore riBoughtStore,
                                           @Nonnull BusinessAccountPriceTableKeyStore prTabKeyStore,
                                           @Nonnull PriceTableStore prTabStore,
                                           long realtimeTopologyContextId,
                                           boolean enableRIBuyAfterPricingChange,
                                           boolean disableRealtimeRIBuyAnalysis) {
        this.reservedInstanceAnalyzer = reservedInstanceAnalyzer;
        this.repositoryClient = repositoryClient;
        this.settingsServiceClient = settingsServiceClient;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.riBoughtStore = riBoughtStore;
        this.prTabKeyStore = prTabKeyStore;
        this.prTabStore = prTabStore;
        this.riBoughtStore.onInventoryChange(this::onRIInventoryUpdated);
        this.enableRIBuyAfterPricingChange = enableRIBuyAfterPricingChange;
        this.disableRealtimeRIBuyAnalysis = disableRealtimeRIBuyAnalysis;
    }

    /**
     * Invokes RI Buy Analysis.
     *
     * @param cloudTopology Cloud Topology from Live Topology Listener.
     * @param allBusinessAccounts BAs.
     */
    public void invokeBuyRIAnalysis(final CloudTopology<TopologyEntityDTO> cloudTopology,
            final Set<ImmutablePair<Long, String>> allBusinessAccounts) {
        Objects.requireNonNull(cloudTopology, "CloudTopology is not initialized.");
        this.cloudTopology = Optional.of(cloudTopology);

        // We don't want to run Buy RI twice (once for reason A, another for reason B).
        boolean hasRIBuyRun = false;
        hasRIBuyRun = invokeRIBuyIfBusinessAccountsUpdated(allBusinessAccounts);
        if (!hasRIBuyRun && enableRIBuyAfterPricingChange) {
            hasRIBuyRun = invokeRIBuyIfPriceTablesChanged(allBusinessAccounts);
        }
        if (!hasRIBuyRun && isRunBuyRIOnNextBroadcast()) {
            StartBuyRIAnalysisRequest buyRiRequest = getStartBuyRIAnalysisRequest();
            if (buyRiRequest.getAccountsList().isEmpty()) {
                logger.warn("invokeBuyRIAnalysis: No BAs found. Trigger RI Buy Analysis on"
                        + " next next topology broadcast.");
            } else {
                // Reset only if we have non-empty topology with BAs.
                setRunBuyRIOnNextBroadcast(false);
            }
            invokeBuyRIAnalysis(buyRiRequest);
        }
    }

    /**
     * Invoke the Buy RI Algorithm.
     *
     * @param buyRiRequest StartBuyRIAnalysisRequest.
     */
    public synchronized void invokeBuyRIAnalysis(StartBuyRIAnalysisRequest buyRiRequest) {
        if (cloudTopology.isPresent() && !disableRealtimeRIBuyAnalysis) {
            logger.info("Started BuyRIAnalysis with accounts: {}, regions: {}, platforms: {},"
                    + " tenancies: {} and profile: {}",
                    buyRiRequest.getAccountsList(),
                    buyRiRequest.getRegionsList(),
                    buyRiRequest.getPlatformsList(),
                    buyRiRequest.getTenanciesList(),
                    buyRiRequest.getPurchaseProfileByCloudtypeMap());
            ReservedInstanceAnalysisScope reservedInstanceAnalysisScope =
                    new ReservedInstanceAnalysisScope(buyRiRequest);
            try {
                reservedInstanceAnalyzer.runRIAnalysisAndSendActions(realtimeTopologyContextId,
                        cloudTopology.get(), reservedInstanceAnalysisScope,
                        ReservedInstanceHistoricalDemandDataType.CONSUMPTION);
            } catch (InterruptedException e) {
                logger.error("Interrupted publishing of Buy RI actions", e);
                Thread.currentThread().interrupt();
            } catch (CommunicationException e) {
                logger.error("Exception while publishing Buy RI actions", e);
            }
        } else {
            if (!disableRealtimeRIBuyAnalysis) {
                logger.warn("No cloud topology defined in the Cost. Trigger Buy RI on next topology broadcast.");
                // Set flag to run RI Buy on next topology broadcast.
                setRunBuyRIOnNextBroadcast(true);
            } else {
                logger.warn("Realtime RI buy analysis is disabled. Skipping");
                reservedInstanceAnalyzer.clearRealtimeRIBuyActions();
            }
        }
    }

    /**
     * Invokes the RI Buy Algorithm when RI Buy Settings are updated.
     */
    @Override
    public void onSettingsUpdated(SettingNotification notification) {
        StartBuyRIAnalysisRequest buyRiRequest = getStartBuyRIAnalysisRequest();
        if (buyRiRequest.getAccountsList().isEmpty()) {
            logger.warn("onSettingsUpdated: No BAs found. Trigger RI Buy Analysis on"
                    + " next inventory/price table udpate or scheduled interval.");
            return;
        }
        if (riSettingNames.contains(notification.getGlobal().getSetting().getSettingSpecName())) {
            logger.info("RI Buy Settings were updated. Triggering RI Buy Analysis.");
            invokeBuyRIAnalysis(buyRiRequest);
        }
     }

    /**
     * Invokes RI Buy Analysis if the RI Inventory is updated.
     */
    @VisibleForTesting
    protected void onRIInventoryUpdated() {
        StartBuyRIAnalysisRequest buyRiRequest = getStartBuyRIAnalysisRequest();
        if (buyRiRequest.getAccountsList().isEmpty()) {
            logger.warn("onRIInventoryUpdated: No BAs found. Trigger RI Buy Analysis on"
                    + " next inventory/price table udpate or scheduled interval.");
            return;
        }
        logger.info("RI Inventory has been changed. Triggering RI Buy Analysis.");
        invokeBuyRIAnalysis(buyRiRequest);
    }

    /**
     * Invoke RI Buy Analysis if the number of BAs has changed.
     *
     * @param allBusinessAccounts OIDs of all BAs present.
     *
     * @return true if BuyRI Analysis was invoked or if there are no targets/BAs to process,
     *         false otherwise.
     */
    public boolean invokeRIBuyIfBusinessAccountsUpdated(Set<ImmutablePair<Long, String>> allBusinessAccounts) {
        boolean runRiBuy = false;
        final StartBuyRIAnalysisRequest buyRiRequest = getStartBuyRIAnalysisRequest();
        logger.info("Business accounts received in topology broadcast: {}", allBusinessAccounts);
        final int newAccountsCount = addNewBAsWithCost(allBusinessAccounts);
        if (newAccountsCount > 0) {
            logger.info("{} new account(s) with Cost found - invoking RI Buy Analysis...", newAccountsCount);
            runRiBuy = true;
        }
        if (rmObsoleteBAs(allBusinessAccounts)) {
            logger.info("Invoke RI Buy Analysis as a BA was removed.");
            runRiBuy = true;
        }
        if (runRiBuy) {
            invokeBuyRIAnalysis(buyRiRequest);
        }

        return runRiBuy;
    }

    /**
     * Invoke RI Buy Analysis if any price table has changed.
     *
     * @param allBusinessAccounts OIDs of all BAs present.
     *
     * @return true if BuyRI Analysis was invoked, false otherwise.
     */
    public boolean invokeRIBuyIfPriceTablesChanged(Set<ImmutablePair<Long, String>> allBusinessAccounts) {
        StartBuyRIAnalysisRequest buyRiRequest = getStartBuyRIAnalysisRequest();
        logger.info("Business accounts received in topology broadcast: {}", allBusinessAccounts);
        if (buyRiRequest.getAccountsList().isEmpty()) {
            logger.warn("invokeRIBuyIfPriceTablesChanged: No BAs found. Trigger RI Buy Analysis"
                    + " next inventory/price table udpate or scheduled interval.");
            return false;
        }

        if (checkAndUpdatePricing(allBusinessAccounts)) {
            logger.info("Invoke RI Buy Analysis as pricing changed for some of the discovered BAs.");
            invokeBuyRIAnalysis(buyRiRequest);
            return true;
        }

        return false;
    }

    /**
     * Get a StartBuyRIAnalysisRequest for a real time topology.
     *
     * @return StartBuyRIAnalysisRequest for a real time topology.
     */
    public StartBuyRIAnalysisRequest getStartBuyRIAnalysisRequest() {
        List<TopologyEntityDTO> entities = RepositoryDTOUtil.topologyEntityStream(
                repositoryClient.retrieveTopologyEntities(
                        RetrieveTopologyEntitiesRequest.newBuilder()
                                .setTopologyContextId(realtimeTopologyContextId)
                                .setReturnType(Type.FULL)
                                .setTopologyType(TopologyType.SOURCE)
                                .build()))
                .map(PartialEntity::getFullEntity)
                .filter(a -> a.getEnvironmentType() == EnvironmentType.CLOUD)
                .filter(a -> a.getEntityType() == EntityType.REGION_VALUE
                        || a.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                .collect(Collectors.toList());

        // Gets all Business Account Ids.
        final Set<Long> baIds = entities.stream()
                .filter(entity -> entity.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                .map(entity -> entity.getOid())
                .collect(Collectors.toSet());

        // Gets all Cloud Region Ids.
        final Set<Long> regionIds = entities.stream()
                .filter(a -> a.getEntityType() == EntityType.REGION_VALUE)
                .filter(a -> a.getEnvironmentType() == EnvironmentType.CLOUD)
                .map(a -> a.getOid())
                .collect(Collectors.toSet());

        RISetting riSetting = getRIBuySettings(settingsServiceClient);
        Map<String, RIPurchaseProfile> riPurchaseProfileMap = Maps.newHashMap();
        // Convert the RISettings to RIPurchaseProfile, if a Setting value is not present, use the default.
        riSetting.getRiSettingByCloudtypeMap().entrySet().forEach(riSettingEntry -> {
                    String cloudType = riSettingEntry.getKey();
                    RIProviderSetting riProviderSetting = riSettingEntry.getValue();
                    ReservedInstanceType.OfferingClass defaultOffering = cloudType.equals(CloudType.AZURE.name()) ?
                            ReservedInstanceType.OfferingClass.CONVERTIBLE : ReservedInstanceType.OfferingClass.STANDARD;
                    ReservedInstanceType.PaymentOption defaultPayment = ReservedInstanceType.PaymentOption.ALL_UPFRONT;
                    int defaultTerm = 1;
                    RIPurchaseProfile riPurchaseProfile = RIPurchaseProfile.newBuilder()
                            .setRiType(ReservedInstanceType.newBuilder().setOfferingClass(
                                    riProviderSetting.hasPreferredOfferingClass() ?
                                            riProviderSetting.getPreferredOfferingClass() : defaultOffering)
                                    .setPaymentOption(riProviderSetting.hasPreferredPaymentOption() ?
                                            riProviderSetting.getPreferredPaymentOption() : defaultPayment)
                                    .setTermYears(riProviderSetting.hasPreferredTerm() ?
                                            riProviderSetting.getPreferredTerm() : defaultTerm)
                            ).build();
                    riPurchaseProfileMap.put(cloudType, riPurchaseProfile);
                }
        );

        final StartBuyRIAnalysisRequest.Builder buyRiRequest = StartBuyRIAnalysisRequest
                .newBuilder()
                .addAllPlatforms(ImmutableSet.copyOf(OSType.values()))
                .addAllTenancies(ImmutableSet.copyOf(Tenancy.values()))
                .putAllPurchaseProfileByCloudtype(riPurchaseProfileMap);

        if (!regionIds.isEmpty()) {
            buyRiRequest.addAllRegions(regionIds);
        }
        if (!baIds.isEmpty()) {
            buyRiRequest.addAllAccounts(baIds);
        }

        return buyRiRequest.build();
    }

    /**
     * Gets the current RI Buy Settings.
     * @param settingsServiceClient The Settings Service Client.
     * @return RISetting The setting with which the RI Buy Algorithm is going to run.
     */
    protected RISetting getRIBuySettings(SettingServiceBlockingStub settingsServiceClient) {
        final Map<String, Setting> settings = new HashMap<>();
        settingsServiceClient.getMultipleGlobalSettings(
                GetMultipleGlobalSettingsRequest.newBuilder().build().newBuilder()
                        .addAllSettingSpecName(riSettingNames)
                        .build())
                .forEachRemaining( setting -> {
                    settings.put(setting.getSettingSpecName(), setting);
                });

        final RIProviderSetting riAWSSetting = RIProviderSetting.newBuilder()
                .setPreferredOfferingClass(OfferingClass.valueOf(settings
                        .get(GlobalSettingSpecs.AWSPreferredOfferingClass.getSettingName())
                        .getEnumSettingValue().getValue()))
                .setPreferredPaymentOption(PaymentOption.valueOf(settings
                        .get(GlobalSettingSpecs.AWSPreferredPaymentOption.getSettingName())
                        .getEnumSettingValue().getValue()))
                .setPreferredTerm(PreferredTerm.valueOf(settings
                        .get(GlobalSettingSpecs.AWSPreferredTerm.getSettingName())
                        .getEnumSettingValue().getValue()).getYears())
                .build();

        final RIProviderSetting riAzureSetting = RIProviderSetting.newBuilder()
                .setPreferredTerm(PreferredTerm.valueOf(settings
                        .get(GlobalSettingSpecs.AzurePreferredTerm.getSettingName())
                        .getEnumSettingValue().getValue()).getYears())
                .build();

        final RISetting riSetting = RISetting.newBuilder()
                .putRiSettingByCloudtype(CloudType.AWS.name(), riAWSSetting)
                .putRiSettingByCloudtype(CloudType.AZURE.name(), riAzureSetting)
                .build();

        return riSetting;
    }

    /**
     * Checks, and updates if needed, price record data for BAs since last discovery/topology broadcast.
     *
     * @param allBusinessAccounts All Business Accounts in topology.
     *
     * @return whether pricing record for any BAs was updated since last topology broadcast.
     */
    protected boolean checkAndUpdatePricing(Set<ImmutablePair<Long, String>> allBusinessAccounts) {
        boolean priceChanged = false;

        // Get all BAs which have cost (includes old and new).
        final Map<Long, Long> allBAToPriceTableOid = prTabKeyStore
                .fetchPriceTableKeyOidsByBusinessAccount(allBusinessAccounts.stream().map(p -> p.left)
                        .collect(Collectors.toSet()));

        Set<BizAccPriceRecord> lastDiscoveredBAs = collectBAsCostRecords(allBusinessAccounts,
                allBAToPriceTableOid);

        logger.info("Business Accounts w/cost: {}", businessAccountsWithCost);
        logger.info("Last Discovered BAs: {}", lastDiscoveredBAs);

        // Run through the list of BAs with price info as was known prior to the last discovery
        // and check if the price info changed for any.
        // In other words - for each BA:
        //     Compare pricing from past discovery to pricing from current discovery.
        //     If different, update record to reflect new price table oid and checksum
        //     and,
        //     Flag the change in pricing to trigger the Buy RI in the parent.
        for (BizAccPriceRecord storedBaRec : businessAccountsWithCost) {
            for (BizAccPriceRecord lastDiscBaRec : lastDiscoveredBAs) {
                // Same BA? Check if the pricing has changed since last discovery.
                if (lastDiscBaRec.getBusinessAccountOid().equals(storedBaRec.getBusinessAccountOid())) {
                    if (!lastDiscBaRec.equals(storedBaRec)) {
                        logger.info("Pricing changed for BA: {}. BA's old price {}",
                                lastDiscBaRec, storedBaRec);
                        priceChanged = true;
                        // Make the BA pricing record up-to-date in the member "businessAccountsWithCost"
                        // collection.
                        storedBaRec.setPrTabChecksum(lastDiscBaRec.getPrTabChecksum());
                        storedBaRec.setPrTabKeyOid(lastDiscBaRec.getPrTabKeyOid());
                        // Note that, even if we detected price change in this particular account pricing,
                        // (i.e enough to know to decide to trigger Buy RI) we still have to run though ALL
                        // entries in both sets (businessAccountsWithCost, lastDiscoveredBAs) as we also have
                        // to update all current prices for all accounts in businessAccountsWithCost.
                        // --------------
                        // So, do not be tempted to
                        // break;
                        // at this moment.
                    }
                }
            }
        }

        return priceChanged;
    }

    protected Set<BizAccPriceRecord> collectBAsCostRecords(
            final Set<ImmutablePair<Long, String>> businessAccounts,
            final Map<Long, Long> baToPriceTableOidMap) {
        Set<BizAccPriceRecord> lastDiscoveredBAs = new HashSet<>();
        // Find which of the new BAs have cost.
        for (Long businessAccountId : baToPriceTableOidMap.keySet()) {
            Long priceTableKeyOid = baToPriceTableOidMap.get(businessAccountId);
            Map<Long, PriceTableKey> prTabOidToTabKey = prTabStore.getPriceTableKeys(
                    Collections.singletonList(priceTableKeyOid));
            PriceTableKey prTabKey = prTabOidToTabKey.get(priceTableKeyOid);
            if (prTabKey != null) {
                // Get price table checksum by price table key.
                Map<PriceTableKey, Long> prTabkeyToChkSum = prTabStore.getChecksumByPriceTableKeys(
                        Collections.singletonList(prTabKey));
                // Get checksum of a price table.
                Long tabChecksum = prTabkeyToChkSum.get(prTabKey);
                if (tabChecksum != null) {
                    // Add record of BA with its price table key oid and price table checksum.
                    lastDiscoveredBAs.add(new BizAccPriceRecord(businessAccountId,
                            getAccountNameByOid(businessAccounts, businessAccountId),
                            priceTableKeyOid, tabChecksum));
                }
            }
        }

        return lastDiscoveredBAs;
    }

    /**
     * Adds newly discovered BAs having cost info since last discovery/topology broadcast.
     *
     * @param allBusinessAccounts All Business Accounts in topology.
     *
     * @return count of new BAs with cost that were discovered since last topology broadcast.
     */
    protected int addNewBAsWithCost(Set<ImmutablePair<Long, String>> allBusinessAccounts) {
        Set<BizAccPriceRecord> newBusinessAccountsWithCost = getNewBusinessAccountsWithCost(allBusinessAccounts);
        if (newBusinessAccountsWithCost.size() > 0) {
            businessAccountsWithCost.addAll(newBusinessAccountsWithCost);
            logger.info("Detected new Business Account(s): {}", newBusinessAccountsWithCost.stream()
                    .map(rec -> rec.getBusinessAccountOid()).collect(Collectors.toSet()));
            logger.info("Size of the updated collection 'Business Accounts With Cost' = {}",
                    businessAccountsWithCost.size());
        }
        return newBusinessAccountsWithCost.size();
    }

    /**
     * Removes obsolete BAs (which were not present in results of recent discovery and last topology broadcast).
     *
     * @param allBusinessAccounts All Business Accounts in topology.
     *
     * @return whether an existing BA was deleted since last topology broadcast or if BA accounts collection
     *         parameter does not have any BAs.
     */
    protected boolean rmObsoleteBAs(Set<ImmutablePair<Long, String>> allBusinessAccounts) {
        if (CollectionUtils.isEmpty(allBusinessAccounts)) {
            return true;
        }

        Set<Long> deletedBusinessAccounts = Sets.difference(
                businessAccountsWithCost.stream()
                        .map(baRec -> baRec.getBusinessAccountOid()).collect(Collectors.toSet()),
                allBusinessAccounts.stream().map(p -> p.left).collect(Collectors.toSet()))
                .immutableCopy();
        if (deletedBusinessAccounts.size() > 0) {
            Set<BizAccPriceRecord> removeTheseBAs = new HashSet<>();
            for (Long baOid : deletedBusinessAccounts) {
                for (BizAccPriceRecord baRec : businessAccountsWithCost) {
                    if (baRec.getBusinessAccountOid() == baOid) {
                        removeTheseBAs.add(baRec);
                    }
                }
            }
            businessAccountsWithCost.removeAll(removeTheseBAs);
            logger.info("Detected deleted Business Account(s): {}."
                    + " Size of the updated collection 'Business Accounts With Cost' = {}",
                    removeTheseBAs, businessAccountsWithCost.size());
        }
        return (deletedBusinessAccounts.size() > 0);
    }

    /**
     * Returns a collection of BAs' pricing records which didn't have cost till last topology broadcast.
     *
     * @param allBusinessAccounts All Business Accounts in topology.
     *
     * @return collection of BAs which didn't have cost till last topology broadcast.
     */
    @VisibleForTesting
    public Set<BizAccPriceRecord> getNewBusinessAccountsWithCost(Set<ImmutablePair<Long, String>> allBusinessAccounts) {
        // Get all new discovered BAs since last broadcast.
        final ImmutableSet<Long> newBusinessAccounts = Sets.difference(
                allBusinessAccounts.stream().map(p -> p.left).collect(Collectors.toSet()),
                businessAccountsWithCost.stream().map(baRec -> baRec.getBusinessAccountOid())
                        .collect(Collectors.toSet())).immutableCopy();

        logger.debug("getNewBusinessAccountsWithCost:\n"
                + "  - allBusinessAccounts {},\n"
                + "  - businessAccountsWithCost {},\n"
                + "  - newBusinessAccounts {}",
                allBusinessAccounts, businessAccountsWithCost, newBusinessAccounts);
        if (CollectionUtils.isEmpty(newBusinessAccounts)) {
            return new HashSet<>();
        }

        // Get all BAs which have cost (includes old and new).
        final Map<Long, Long> newBAToPriceTableOid = prTabKeyStore
                .fetchPriceTableKeyOidsByBusinessAccount(newBusinessAccounts);
        logger.debug("getNewBusinessAccountsWithCost:\n  - newBAToPriceTableOid {}", newBAToPriceTableOid);

        return collectBAsCostRecords(allBusinessAccounts, newBAToPriceTableOid);
    }

    @Nonnull
    private String getAccountNameByOid(@Nonnull Set<ImmutablePair<Long, String>> allBusinessAccounts,
            @Nonnull Long businessAccountId) {
        String accName = "unknown";
        for (ImmutablePair<Long, String> acc : allBusinessAccounts) {
            if (acc.left.equals(businessAccountId)) {
                accName = acc.right;
                break;
            }
        }

        return accName;
    }

    /**
     * Get "Run Buy RI analysis on next topology broadcast" flag.
     *
     * @return "Run Buy RI analysis on next topology broadcast" flag.
     */
    public boolean isRunBuyRIOnNextBroadcast() {
        return runBuyRIOnNextBroadcast;
    }

    /**
     * Set "Run Buy RI analysis on next topology broadcast" flag.
     *
     * @param runBuyRIOnNextBroadcast "Run Buy RI analysis on next topology broadcast" flag.
     */
    public void setRunBuyRIOnNextBroadcast(boolean runBuyRIOnNextBroadcast) {
        this.runBuyRIOnNextBroadcast = runBuyRIOnNextBroadcast;
    }

    /**
     * Gets BA price table key store.
     *
     * @return BA price table key store.
     */
    public BusinessAccountPriceTableKeyStore getPrTabKeyStore() {
        return prTabKeyStore;
    }

    /**
     * Gets BA price table store.
     *
     * @return BA price table store.
     */
    public PriceTableStore getPrTabStore() {
        return prTabStore;
    }

    /**
     * Class representing record of essential information related to pricing for a business account.
     */
    protected class BizAccPriceRecord {
        private Long businessAccountOid;
        private String baDisplayName;
        private Long prTabKeyOid;
        private Long prTabChecksum;

        BizAccPriceRecord(Long businessAccountOid, String baDisplayName, Long prTabKeyOid, Long prTabChecksum) {
            super();
            this.businessAccountOid = businessAccountOid;
            this.baDisplayName = baDisplayName;
            this.prTabKeyOid = prTabKeyOid;
            this.prTabChecksum = prTabChecksum;
        }

        Long getBusinessAccountOid() {
            return businessAccountOid;
        }

        Long getPrTabKeyOid() {
            return prTabKeyOid;
        }

        void setPrTabKeyOid(Long prTabKeyOid) {
            this.prTabKeyOid = prTabKeyOid;
        }

        Long getPrTabChecksum() {
            return prTabChecksum;
        }

        void setPrTabChecksum(Long prTabChecksum) {
            this.prTabChecksum = prTabChecksum;
        }

        public String getBaDisplayName() {
            return baDisplayName;
        }

        private ReservedInstanceAnalysisInvoker getOuterType() {
            return ReservedInstanceAnalysisInvoker.this;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((businessAccountOid == null) ? 0 : businessAccountOid.hashCode());
            result = prime * result + ((prTabChecksum == null) ? 0 : prTabChecksum.hashCode());
            result = prime * result + ((prTabKeyOid == null) ? 0 : prTabKeyOid.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            BizAccPriceRecord other = (BizAccPriceRecord) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (businessAccountOid == null) {
                if (other.businessAccountOid != null)
                    return false;
            } else if (!businessAccountOid.equals(other.businessAccountOid))
                return false;
            if (prTabChecksum == null) {
                if (other.prTabChecksum != null)
                    return false;
            } else if (!prTabChecksum.equals(other.prTabChecksum))
                return false;
            if (prTabKeyOid == null) {
                if (other.prTabKeyOid != null)
                    return false;
            } else if (!prTabKeyOid.equals(other.prTabKeyOid))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "BizAccPriceRecord [baOid=" + businessAccountOid + ", baName=" + baDisplayName
                    + ", prTabKeyOid=" + prTabKeyOid + ", prTabChecksum=" + prTabChecksum + "]";
        }
    }
}
