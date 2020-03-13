package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
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
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore.ReservedInstanceBoughtChangeType;
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
 * Called
 * - when the cost component is started and later on after regular intervals.
 * - when the RI inventory changes.
 * - when RI Buy Settings change.
 */
public class ReservedInstanceAnalysisInvoker implements SettingsListener {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceAnalyzer reservedInstanceAnalyzer;

    private final RepositoryServiceBlockingStub repositoryClient;

    private final SettingServiceBlockingStub settingsServiceClient;

    private final long realtimeTopologyContextId;

    // The inventory of RIs that have already been purchased
    private final ReservedInstanceBoughtStore riBoughtStore;

    private final BusinessAccountPriceTableKeyStore keyStore;

    private static List<String> riSettingNames = new ArrayList<>();

    private final Set<Long> businessAccountsWithCost = new HashSet<>();

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
                                           @Nonnull BusinessAccountPriceTableKeyStore keyStore,
                                           long realtimeTopologyContextId) {
        this.reservedInstanceAnalyzer = reservedInstanceAnalyzer;
        this.repositoryClient = repositoryClient;
        this.settingsServiceClient = settingsServiceClient;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.riBoughtStore = riBoughtStore;
        this.keyStore = keyStore;
        this.riBoughtStore.getUpdateEventStream()
                .filter(event -> event == ReservedInstanceBoughtChangeType.UPDATED)
                .subscribe(this::onRIInventoryUpdated);
    }

    /**
     * Invoke the Buy RI Algorithm.
     *
     * @param buyRiRequest StartBuyRIAnalysisRequest.
     */
    public synchronized void invokeBuyRIAnalysis(StartBuyRIAnalysisRequest buyRiRequest) {
        ReservedInstanceAnalysisScope reservedInstanceAnalysisScope =
                new ReservedInstanceAnalysisScope(buyRiRequest);
        try {
            reservedInstanceAnalyzer.runRIAnalysisAndSendActions(realtimeTopologyContextId,
                    reservedInstanceAnalysisScope, ReservedInstanceHistoricalDemandDataType.CONSUMPTION);
        } catch (InterruptedException e) {
            logger.error("Interrupted publishing of Buy RI actions", e);
            Thread.currentThread().interrupt();
        } catch (CommunicationException e) {
            logger.error("Exception while publishing Buy RI actions", e);
        }
    }

    /**
     * Invokes the RI Buy Algorithm when RI Buy Settings are updated.
     */
    @Override
    public void onSettingsUpdated(SettingNotification notification) {
        StartBuyRIAnalysisRequest buyRiRequest = getStartBuyRIAnalysisRequest();
        if (buyRiRequest.getAccountsList().isEmpty()) {
            logger.warn("ReservedInstanceAnalysisInvoker::onSettingsUpdated." +
                    "No BA's found in repository. Reattempt to trigger RI Buy Analysis will be " +
                    "done during next settings update.");
            return;
        }
        if (riSettingNames.contains(notification.getGlobal().getSetting().getSettingSpecName())) {
            logger.info("RI Buy Settings were updated. Triggering RI Buy Analysis.");
            invokeBuyRIAnalysis(buyRiRequest);
        }
     }

    /**
     * Invokes RI Buy Analysis if the RI Inventory is updated.
     *
     * @param type ReservedInstanceBoughtChangeType
     */
    @VisibleForTesting
    protected void onRIInventoryUpdated(final ReservedInstanceBoughtChangeType type) {
        StartBuyRIAnalysisRequest buyRiRequest = getStartBuyRIAnalysisRequest();
        if (buyRiRequest.getAccountsList().isEmpty()) {
            logger.warn("ReservedInstanceAnalysisInvoker::onRIInventoryUpdated." +
                    "No BA's found in repository. Reattempt to trigger RI Buy Analysis will be " +
                    "done during next RI Inventory update.");
            return;
        }
        logger.info("RI Inventory has been changed. Triggering RI Buy Analysis.");
        invokeBuyRIAnalysis(buyRiRequest);
    }

    /**
     * Invoke RI Buy Analysis if the number of BA's are updated.
     *
     * @param allBusinessAccounts OID's of all BA's present.
     */
    public void invokeRIBuyIfBusinessAccountsUpdated(Set<Long> allBusinessAccounts) {
        StartBuyRIAnalysisRequest buyRiRequest = getStartBuyRIAnalysisRequest();
        if (buyRiRequest.getAccountsList().isEmpty()) {
            logger.warn("ReservedInstanceAnalysisInvoker::invokeRIBuyIfBusinessAccountsUpdated." +
                    "No BA's found in repository. Reattempt to trigger RI Buy Analysis will be " +
                    "done during next live topology broadcast.");
            return;
        }
        if (isNewBusinessAccountWithCostFound(allBusinessAccounts) || isBusinessAccountDeleted(allBusinessAccounts)) {
            logger.info("Invoking RI Buy Analysis because either a new BA with Cost was found" +
                    " or a BA was deleted.");
            invokeBuyRIAnalysis(buyRiRequest);
        }
    }

    /**
     * Returns a StartBuyRIAnalysisRequest for a real time topology.
     * @return
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
                .filter(a -> a.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                .map(a -> a.getOid())
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
                    ReservedInstanceType.OfferingClass defaultOffering = cloudType == CloudType.AZURE.name() ?
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
     * Returns whether a new BA with cost was found since last topology broadcast.
     *
     * @param allBusinessAccounts All Business Accounts in topology.
     * @return whether a new BA with cost was found since last topology broadcast.
     */
    protected boolean isNewBusinessAccountWithCostFound(Set<Long> allBusinessAccounts) {
        Set<Long> newBusinessAccountsWithCost = getNewBusinessAccountsWithCost(allBusinessAccounts);
        addToBusinessAccountsWithCost(newBusinessAccountsWithCost);
        return (newBusinessAccountsWithCost.size() > 0);
    }

    /**
     * Returns whether a BA was deleted since last topology broadcast.
     *
     * @param allBusinessAccounts All Business Accounts in topology.
     * @return whether a BA was deleted since last topology broadcast.
     */
    private boolean isBusinessAccountDeleted(Set<Long> allBusinessAccounts) {
        Set<Long> deletedBusinessAccounts = Sets.difference(businessAccountsWithCost, allBusinessAccounts)
                .immutableCopy();
        removeFromBusinessAccountsWithCost(deletedBusinessAccounts);
        return (deletedBusinessAccounts.size() > 0);
    }

    /**
     * Returns a collection of BA's which didn't have cost till last topology broadcast.
     *
     * @param allBusinessAccounts All Business Accounts in topology.
     * @return collection of BA's which didn't have cost till last topology broadcast.
     */
    @VisibleForTesting
    public Set<Long> getNewBusinessAccountsWithCost(Set<Long> allBusinessAccounts) {
        // Get all new discovered BA's since last broadcast.
        final ImmutableSet<Long> newBusinessAccounts = Sets
                .difference(allBusinessAccounts, businessAccountsWithCost).immutableCopy();

        // Get all BA's which have cost(includes old and new) .
        final Map<Long, Long> allBusinessAccountsWithCost = keyStore
                .fetchPriceTableKeyOidsByBusinessAccount(newBusinessAccounts);

        Set<Long> newBusinessAccountsWithCost = new HashSet<>();
        // Find which of the new BA's have cost.
        for (Long businessAccountId : newBusinessAccounts) {
            if (allBusinessAccountsWithCost.containsKey(businessAccountId)) {
                newBusinessAccountsWithCost.add(businessAccountId);
            }
        }
        return newBusinessAccountsWithCost;
    }

    /**
     * Adds the new Business Accounts With Cost to current Business Accounts With Cost.
     *
     * @param newBusinessAccountsWithCost a collection of BA's which didn't have cost till last
     *                                    topology broadcast.
     */
    private void addToBusinessAccountsWithCost(Set<Long> newBusinessAccountsWithCost) {
        businessAccountsWithCost.addAll(newBusinessAccountsWithCost);
    }

    /**
     * Removes the deleted Business Accounts With Cost from current Business Accounts With Cost.
     *
     * @param deletedBusinessAccounts a collection of BA's which have been deleted.
     */
    private void removeFromBusinessAccountsWithCost(Set<Long> deletedBusinessAccounts) {
        businessAccountsWithCost.removeAll(deletedBusinessAccounts);
    }
}
