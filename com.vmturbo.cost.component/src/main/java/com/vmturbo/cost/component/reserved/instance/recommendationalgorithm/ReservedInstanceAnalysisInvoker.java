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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
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
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
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
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;

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

    private boolean runRIBuyOnNewRequest;

    private final ExecutorService executorService;

    private final AtomicReference<Future<Void>> currentRunningRIBuy = new AtomicReference<>();

    /**
     * Map of Business Accounts by OID with associated pricing information.
     */
    private final Map<Long, BusinessAccountPriceData> businessAccountsWithCost = new ConcurrentHashMap<>();

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
                                           boolean disableRealtimeRIBuyAnalysis,
                                           boolean runRIBuyOnNewRequest) {
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
        this.runRIBuyOnNewRequest = runRIBuyOnNewRequest;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("Realtime RI Buy Analysis").build();
        this.executorService = Executors.newSingleThreadExecutor(namedThreadFactory);
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
        hasRIBuyRun = invokeRIBuyIfBusinessAccountsUpdated(allBusinessAccounts, cloudTopology);
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
    public void invokeBuyRIAnalysis(StartBuyRIAnalysisRequest buyRiRequest) {
        if (areAllActionsDisabled()) {
            logger.warn("All Actions are disabled. Hence RI Buy will not trigger. Enable all actions to run RI Buy.");
            return;
        }

        if (cloudTopology.isPresent() && !disableRealtimeRIBuyAnalysis) {
            currentRunningRIBuy.updateAndGet((currentFuture) -> {
                if (currentFuture == null || runRIBuyOnNewRequest || currentFuture.isDone()) {
                    if (currentFuture != null && !currentFuture.isDone()) {
                        logger.info(
                                "RI Buy is already running, but will be discarded. A new RI buy round"
                                        + "of analysis will be run for topology id {}",
                                buyRiRequest.getTopologyInfo().getTopologyId());
                        currentFuture.cancel(true);
                    }
                    return runRIBuyAsync(buyRiRequest);
                } else {
                    logger.warn("RI buy already running. Wil not be run again for topology id {}",
                            buyRiRequest.getTopologyInfo().getTopologyId());
                    return currentFuture;
                }
            });
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

    private boolean areAllActionsDisabled() {
        final GetGlobalSettingResponse globalSettingResponse = settingsServiceClient
                        .getGlobalSetting(GetSingleGlobalSettingRequest.newBuilder()
                                        .setSettingSpecName(GlobalSettingSpecs.DisableAllActions
                                                        .getSettingName()).build());
        return globalSettingResponse.getSetting().getBooleanSettingValue().getValue();
    }

    private synchronized void startRIBuyRun(StartBuyRIAnalysisRequest buyRiRequest) {
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
        } catch (Exception e) {
            logger.error("Error executing RI Buy analysis", e);
        }
    }

    /**
     * Executes the RI buy in a separate thread.
     *
     * @param buyRiRequest The buyRIRequest.
     *
     * @return A future containing the running RI buy reference.
     */
    private synchronized Future<Void> runRIBuyAsync(StartBuyRIAnalysisRequest buyRiRequest) {
        return executorService.submit(() -> {
            startRIBuyRun(buyRiRequest);
            return null;
        });
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
     * @param cloudTopology The given cloud topology.
     *
     * @return true if BuyRI Analysis was invoked or if there are no targets/BAs to process,
     *         false otherwise.
     */
    public boolean invokeRIBuyIfBusinessAccountsUpdated(Set<ImmutablePair<Long, String>> allBusinessAccounts,
            CloudTopology<TopologyEntityDTO> cloudTopology) {
        boolean runRiBuy = false;
        final StartBuyRIAnalysisRequest buyRiRequest = getStartBuyRIAnalysisRequest(cloudTopology);
        logger.debug("Business accounts received in topology broadcast: {}", allBusinessAccounts);
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
        logger.debug("Business accounts received in topology broadcast: {}", allBusinessAccounts);
        if (buyRiRequest.getAccountsList().isEmpty()) {
            logger.warn("invokeRIBuyIfPriceTablesChanged: No BAs found. Trigger RI Buy Analysis"
                    + " next inventory/price table update or scheduled interval.");
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

        return constructRIBuyAnalysisRequest(regionIds, baIds);
    }

    /**
     * Given a cloud topology, constructs the RI buy request.
     *
     * @param cloudTopology The cloud topology.
     *
     * @return The constructed RI buy request.
     */
    public StartBuyRIAnalysisRequest getStartBuyRIAnalysisRequest(CloudTopology<TopologyEntityDTO> cloudTopology) {

        // Gets all Business Account Ids.
        final Set<Long> baIds = cloudTopology.getAllEntitiesOfType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .stream().map(s -> s.getOid()).collect(Collectors.toSet());

        // Gets all Cloud Region Ids.
        final Set<Long> regionIds = cloudTopology.getAllEntitiesOfType(EntityType.REGION_VALUE).stream()
                .map(s -> s.getOid()).collect(Collectors.toSet());

        return constructRIBuyAnalysisRequest(regionIds, baIds);
    }

    private StartBuyRIAnalysisRequest constructRIBuyAnalysisRequest(Set<Long> regionIds, Set<Long> baIds) {
        RISetting riSetting = getRIBuySettings(settingsServiceClient);
        Map<String, RIPurchaseProfile> riPurchaseProfileMap = Maps.newHashMap();
        // Convert the RISettings to RIPurchaseProfile, if a Setting value is not present, use the default.
        riSetting.getRiSettingByCloudtypeMap().entrySet().forEach(riSettingEntry -> {
                    String cloudType = riSettingEntry.getKey();
                    RIProviderSetting riProviderSetting = riSettingEntry.getValue();
                    ReservedInstanceType.OfferingClass defaultOffering = cloudType.equals(CloudType.AZURE.name())
                            ? ReservedInstanceType.OfferingClass.CONVERTIBLE : ReservedInstanceType.OfferingClass.STANDARD;
                    PaymentOption defaultPayment = PaymentOption.ALL_UPFRONT;
                    int defaultTerm = 1;
                    RIPurchaseProfile riPurchaseProfile = RIPurchaseProfile.newBuilder()
                            .setRiType(ReservedInstanceType.newBuilder().setOfferingClass(
                                    riProviderSetting.hasPreferredOfferingClass()
                                            ? riProviderSetting.getPreferredOfferingClass() : defaultOffering)
                                    .setPaymentOption(riProviderSetting.hasPreferredPaymentOption()
                                            ? riProviderSetting.getPreferredPaymentOption() : defaultPayment)
                                    .setTermYears(riProviderSetting.hasPreferredTerm()
                                            ? riProviderSetting.getPreferredTerm() : defaultTerm)
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

        final Map<Long, BusinessAccountPriceData> lastDiscoveredBAs =
                collectBAsCostRecords(allBusinessAccounts, allBAToPriceTableOid)
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(
                                BusinessAccountPriceData::businessAccountOid,
                                Function.identity()));

        logger.debug("Business Accounts w/cost: {}", businessAccountsWithCost);
        logger.debug("Last Discovered BAs: {}", lastDiscoveredBAs);

        // Run through the list of BAs with price info as was known prior to the last discovery
        // and check if the price info changed for any.
        // In other words - for each BA:
        //     Compare pricing from past discovery to pricing from current discovery.
        //     If different, update record to reflect new price table oid and checksum
        //     and,
        //     Flag the change in pricing to trigger the Buy RI in the parent.
        for (Map.Entry<Long, BusinessAccountPriceData> storedBaRec : businessAccountsWithCost.entrySet()) {

            long storedAccountOid = storedBaRec.getKey();
            if (lastDiscoveredBAs.containsKey(storedAccountOid)) {
                final BusinessAccountPriceData storedPriceData = storedBaRec.getValue();
                final BusinessAccountPriceData lastAccountPriceData = lastDiscoveredBAs.get(storedAccountOid);

                if (!lastAccountPriceData.equals(storedPriceData)) {
                    logger.info("Pricing changed for BA: {}. BA's old price {}",
                            lastAccountPriceData, storedPriceData);
                    priceChanged = true;

                    // update the businessAccountsWithCost map
                    businessAccountsWithCost.put(storedAccountOid, lastAccountPriceData);
                }
            }
        }

        return priceChanged;
    }

    protected Set<BusinessAccountPriceData> collectBAsCostRecords(
            final Set<ImmutablePair<Long, String>> businessAccounts,
            final Map<Long, Long> baToPriceTableOidMap) {

        final Set<Long> uniquePriceTableKeyOids = ImmutableSet.copyOf(baToPriceTableOidMap.values());
        final Map<Long, PriceTableKey> priceTableKeyOidMap = prTabStore.getPriceTableKeys(uniquePriceTableKeyOids);
        final Set<PriceTableKey> uniquePriceTableKeys = ImmutableSet.copyOf(priceTableKeyOidMap.values());
        final Map<PriceTableKey, Long> priceTableKeyChecksumMap = prTabStore.getChecksumByPriceTableKeys(uniquePriceTableKeys);

        final ImmutableSet.Builder<BusinessAccountPriceData> lastDiscoveredBAs = ImmutableSet.builder();
        // Find which of the new BAs have cost.
        for (long businessAccountId : baToPriceTableOidMap.keySet()) {

            final long  priceTableKeyOid = baToPriceTableOidMap.get(businessAccountId);
            if (priceTableKeyOidMap.containsKey(priceTableKeyOid)) {
                final PriceTableKey priceTableKey = priceTableKeyOidMap.get(priceTableKeyOid);

                if (priceTableKey != null && priceTableKeyChecksumMap.containsKey(priceTableKey)) {
                    final Long priceTableChecksum = priceTableKeyChecksumMap.get(priceTableKey);
                    if (priceTableChecksum != null) {
                        lastDiscoveredBAs.add(BusinessAccountPriceData.builder()
                                .businessAccountOid(businessAccountId)
                                .accountDisplayName(getAccountNameByOid(businessAccounts, businessAccountId))
                                .priceTableKeyOid(priceTableKeyOid)
                                .priceTableChecksum(priceTableChecksum)
                                .build());
                    }

                }
            }
        }

        return lastDiscoveredBAs.build();
    }

    /**
     * Adds newly discovered BAs having cost info since last discovery/topology broadcast.
     *
     * @param allBusinessAccounts All Business Accounts in topology.
     *
     * @return count of new BAs with cost that were discovered since last topology broadcast.
     */
    protected int addNewBAsWithCost(Set<ImmutablePair<Long, String>> allBusinessAccounts) {
        Set<BusinessAccountPriceData> newBusinessAccountsWithCost = getNewBusinessAccountsWithCost(allBusinessAccounts);
        if (newBusinessAccountsWithCost.size() > 0) {
            newBusinessAccountsWithCost.forEach(accountPriceData ->
                    businessAccountsWithCost.put(accountPriceData.businessAccountOid(), accountPriceData));
            logger.info("Detected new Business Account(s): {}", newBusinessAccountsWithCost.stream()
                    .map(rec -> rec.businessAccountOid()).collect(Collectors.toSet()));
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

        final Set<Long> deletedBusinessAccounts = Sets.difference(
                businessAccountsWithCost.keySet(),
                allBusinessAccounts.stream().map(p -> p.left).collect(Collectors.toSet()))
                .immutableCopy();
        if (deletedBusinessAccounts.size() > 0) {
            final Set<BusinessAccountPriceData> removedAccountPriceData = deletedBusinessAccounts.stream()
                    .map(businessAccountsWithCost::remove)
                    .filter(Objects::nonNull)
                    .collect(ImmutableSet.toImmutableSet());
            logger.info("Detected deleted Business Account(s): {}."
                    + " Size of the updated collection 'Business Accounts With Cost' = {}",
                    removedAccountPriceData, businessAccountsWithCost.size());
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
    public Set<BusinessAccountPriceData> getNewBusinessAccountsWithCost(
            Set<ImmutablePair<Long, String>> allBusinessAccounts) {

        // Get all new discovered BAs since last broadcast.
        final ImmutableSet<Long> newBusinessAccounts = Sets.difference(
                allBusinessAccounts.stream().map(p -> p.left).collect(Collectors.toSet()),
                businessAccountsWithCost.keySet()).immutableCopy();

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

    @HiddenImmutableImplementation
    @Immutable
    interface BusinessAccountPriceData {

        long businessAccountOid();

        @Auxiliary
        String accountDisplayName();

        long priceTableKeyOid();

        long priceTableChecksum();

        static Builder builder() {
            return new Builder();
        }

        class Builder extends ImmutableBusinessAccountPriceData.Builder {}
    }
}
