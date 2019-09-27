package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.RISettingsEnum.PreferredTerm;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore.ReservedInstanceBoughtChangeType;
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
public class ReservedInstanceAnalysisInvoker {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceAnalyzer reservedInstanceAnalyzer;

    private final RepositoryServiceBlockingStub repositoryClient;

    private final SettingServiceBlockingStub settingsServiceClient;

    private final long realtimeTopologyContextId;

    // The inventory of RIs that have already been purchased
    private final ReservedInstanceBoughtStore riBoughtStore;

    public ReservedInstanceAnalysisInvoker(ReservedInstanceAnalyzer reservedInstanceAnalyzer,
                                    RepositoryServiceBlockingStub repositoryClient,
                                    SettingServiceBlockingStub settingsServiceClient,
                                    ReservedInstanceBoughtStore riBoughtStore,
                                    long realtimeTopologyContextId) {
        this.reservedInstanceAnalyzer = reservedInstanceAnalyzer;
        this.repositoryClient = repositoryClient;
        this.settingsServiceClient = settingsServiceClient;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.riBoughtStore = riBoughtStore;
        this.riBoughtStore.getUpdateEventStream()
                .filter(event -> event == ReservedInstanceBoughtChangeType.UPDATED)
                .subscribe(this::onRIInventoryUpdated);
    }

    /**
     * Invoke the Buy RI Algorithm.
     */
    public synchronized void invokeBuyRIAnalysis() {
        StartBuyRIAnalysisRequest buyRiRequest = getStartBuyRIAnalysisRequest();
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


    private void onRIInventoryUpdated(final ReservedInstanceBoughtChangeType type) {
        logger.info("RI Inventory has been changed. Triggering RI Buy Analysis.");
        invokeBuyRIAnalysis();
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

        final StartBuyRIAnalysisRequest.Builder buyRiRequest = StartBuyRIAnalysisRequest
                .newBuilder()
                .addAllPlatforms(ImmutableSet.copyOf(OSType.values()))
                .addAllTenancies(ImmutableSet.copyOf(Tenancy.values()))
                .setPurchaseProfile(RIPurchaseProfile.newBuilder()
                        .setRiType(ReservedInstanceType.newBuilder()
                                .setOfferingClass(riSetting.hasPreferredOfferingClass() ?
                                        riSetting.getPreferredOfferingClass() : ReservedInstanceType
                                        .OfferingClass.STANDARD)
                                .setPaymentOption(riSetting.hasPreferredPaymentOption() ?
                                        riSetting.getPreferredPaymentOption()
                                        : ReservedInstanceType.PaymentOption.ALL_UPFRONT)
                                .setTermYears(riSetting.hasPreferredTerm() ? riSetting.getPreferredTerm() : 1))
                        .setPurchaseDate(riSetting.hasPurchaseDate() ? riSetting.getPurchaseDate()
                                : new Date().getTime()));

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
     * @return RISetting.
     */
    private RISetting getRIBuySettings(SettingServiceBlockingStub settingsServiceClient) {
        final List<String> settingNames =
                Arrays.asList(GlobalSettingSpecs.AWSPreferredOfferingClass.getSettingName(),
                        GlobalSettingSpecs.AWSPreferredPaymentOption.getSettingName(),
                        GlobalSettingSpecs.AWSPreferredTerm.getSettingName());

        final Map<String, Setting> settings = new HashMap<>();
        settingsServiceClient.getMultipleGlobalSettings(
                GetMultipleGlobalSettingsRequest.newBuilder().build().newBuilder()
                        .addAllSettingSpecName(settingNames)
                        .build())
                .forEachRemaining( setting -> {
                    settings.put(setting.getSettingSpecName(), setting);
                });

        final RISetting riSetting = RISetting.newBuilder()
                .setPreferredOfferingClass(OfferingClass.valueOf(settings
                        .get(GlobalSettingSpecs.AWSPreferredOfferingClass.getSettingName())
                        .getEnumSettingValue().getValue()))
                .setPreferredPaymentOption(PaymentOption.valueOf(settings
                        .get(GlobalSettingSpecs.AWSPreferredPaymentOption.getSettingName())
                        .getEnumSettingValue().getValue()))
                .setPreferredTerm(PreferredTerm.valueOf(settings
                        .get(GlobalSettingSpecs.AWSPreferredTerm.getSettingName())
                        .getEnumSettingValue().getValue()).getYears())
                .setPurchaseDate(0)
                .build();

        return riSetting;
    }
}
