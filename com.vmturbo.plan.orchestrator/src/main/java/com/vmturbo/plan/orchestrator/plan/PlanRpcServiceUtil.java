package com.vmturbo.plan.orchestrator.plan;

import static com.vmturbo.common.protobuf.utils.StringConstants.CLOUD_MIGRATION_PLAN__ALLOCATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.CLOUD_MIGRATION_PLAN__CONSUMPTION;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersResponse;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RIProviderSetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.setting.ConfigurableActionSettings;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DemandType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;

/**
 * This class provides support for PlanRpcService.
 */
public class PlanRpcServiceUtil {

    /**
     * Create StartBuyRIAnalysisRequest.
     *
     * @param scenarioInfo the scenarioInfo of plan instance
     * @param riScenario the scenario change related with RI
     * @param planId the plan id
     * @param groupServiceClient group service client
     * @param repositoryServiceClient repository service client
     * @return StartBuyRIAnalysisRequest
     */
    public static StartBuyRIAnalysisRequest createBuyRIRequest(@Nonnull ScenarioInfo scenarioInfo,
                                          @Nonnull ScenarioChange riScenario, long planId,
                                          @Nonnull final GroupServiceBlockingStub groupServiceClient,
                                          @Nonnull final RepositoryServiceBlockingStub repositoryServiceClient) {

        // Create the PurchaseProfileByCloudtype map based on the Scenario RI settings
        RISetting riSetting = riScenario.getRiSetting();
        Map<String, RIPurchaseProfile> riPurchaseProfileMap = Maps.newHashMap();
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

        /*
         * Because OCP currently does not allow User scoping by OSType or Tenancy, not setting
         * platforms or tenancies here in the requests, results in null and the RI buy algorithm will
         * fill in the appropriate OSTypes and Tenancies.
         */
        final StartBuyRIAnalysisRequest.Builder buyRiRequest = StartBuyRIAnalysisRequest
                .newBuilder().setTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(planId)
                        .setTopologyType(TopologyType.PLAN)
                        .setPlanInfo(PlanTopologyInfo.newBuilder()
                                .setPlanType(scenarioInfo.getType())
                                .setPlanSubType(getCloudPlanSubType(scenarioInfo)))
                                .build())
                .putAllPurchaseProfileByCloudtype(riPurchaseProfileMap);
        final Map<Integer, Set<Long>> scopeObjectByClass = getClassNameToOids(scenarioInfo.getScope()
                        .getScopeEntriesList(), groupServiceClient, repositoryServiceClient);
        final Set<Long> regionIds = scopeObjectByClass.get(EntityType.REGION.getNumber());
        if (regionIds != null && !regionIds.isEmpty()) {
            buyRiRequest.addAllRegions(regionIds);
        }
        final Set<Long> baIds = scopeObjectByClass.get(EntityType.BUSINESS_ACCOUNT.getNumber());
        if (baIds != null && !baIds.isEmpty()) {
            buyRiRequest.addAllAccounts(baIds);
        }
        if (isScalingEnabled(scenarioInfo)) {
            buyRiRequest.setDemandType(DemandType.CONSUMPTION);
        } else {
            buyRiRequest.setDemandType(DemandType.ALLOCATION);
        }
        return buyRiRequest.build();
    }

    private static Map<Integer, Set<Long>> getClassNameToOids(@Nonnull List<PlanScopeEntry> planScopeEntries,
                                                    @Nonnull GroupServiceBlockingStub groupServiceClient,
                                                    @Nonnull final RepositoryServiceBlockingStub repositoryServiceClient) {
        final Map<Integer, Set<Long>> scopeObjectByClass = new HashMap<>();
        final Set<Long> groupIdsToResolve = new HashSet<>();
        for (PlanScopeEntry entry : planScopeEntries) {
            final String className = entry.getClassName();
            final long oid = entry.getScopeObjectOid();
            if (StringConstants.GROUP_TYPES.contains(className)) {
                groupIdsToResolve.add(oid);
            } else {
                // Note: in planScopeEntry the class name for ba is BusinessAccount
                // yet in the commonDTO.EntityType, ba is BUSINESS_ACCOUNT
                final int type = className.equals(StringConstants.BUSINESS_ACCOUNT)
                                ? EntityType.BUSINESS_ACCOUNT_VALUE
                                : ApiEntityType.fromStringToSdkType(className);
                scopeObjectByClass.computeIfAbsent(type, k -> new HashSet<>()).add(oid);
            }
        }
        // Resolve the groups
        if (groupIdsToResolve.size() > 0) {
            final GetMembersRequest membersRequest = GetMembersRequest.newBuilder()
                    .setExpandNestedGroups(true)
                    .addAllId(groupIdsToResolve)
                    .build();

            final Iterator<GetMembersResponse> response =
                    groupServiceClient.getMembers(membersRequest);
            final Set<Long> members = new HashSet<>();
            while (response.hasNext()) {
                members.addAll(response.next().getMemberIdList());
            }
            if (!members.isEmpty()) {
                final RetrieveTopologyEntitiesRequest getEntitiesrequest = RetrieveTopologyEntitiesRequest.newBuilder()
                                .addAllEntityOids(members)
                                .setReturnType(PartialEntity.Type.MINIMAL)
                                .build();
                RepositoryDTOUtil
                                .topologyEntityStream(repositoryServiceClient
                                                .retrieveTopologyEntities(getEntitiesrequest))
                                .map(PartialEntity::getMinimal)
                                .forEach(min -> scopeObjectByClass
                                                .computeIfAbsent(min.getEntityType(),
                                                                 k -> new HashSet<>())
                                                .add(min.getOid()));
            }
        }

        return scopeObjectByClass;
    }

    /**
     * Gets ids of entities that are in scope of the plan. If it is a group, then the member ids of
     * the group are returned.
     *
     * @param planInstance Instance of plan.
     * @param groupClient Client to resolving group id into its member ids.
     * @param repositoryClient Repo for looking up entity types.
     * @return List of entity ids that are in plan scope.
     */
    static List<Long> getScopeSeedIds(@Nonnull PlanInstance planInstance,
                                      @Nonnull GroupServiceBlockingStub groupClient,
                                      @Nonnull final RepositoryServiceBlockingStub repositoryClient) {
        if (!planInstance.hasScenario()) {
            return Collections.emptyList();
        }
        List<PlanScopeEntry> planScopeEntries = planInstance.getScenario().getScenarioInfo()
                .getScope().getScopeEntriesList();
        Map<Integer, Set<Long>> typeToOids = getClassNameToOids(planScopeEntries, groupClient,
                repositoryClient);
        return typeToOids.values()
                .stream()
                .flatMap(Set::stream)
                .map(Long.class::cast)
                .collect(Collectors.toList());
    }

    /**
     * Check if scaling (resize) is enabled for this plan scenario.
     *
     * @param scenarioInfo Input scenarioInfo
     * @return true if scaling is enabled, false otherwise
     */
    public static boolean isScalingEnabled(@Nonnull final ScenarioInfo scenarioInfo) {
        return scenarioInfo.getChangesList()
                .stream()
                .filter(ScenarioChange::hasSettingOverride)
                .map(ScenarioChange::getSettingOverride)
                .filter(SettingOverride::hasSetting)
                .map(SettingOverride::getSetting)
                .anyMatch(setting -> setting.getSettingSpecName()
                        .contains(ConfigurableActionSettings.Resize.getSettingName().toLowerCase())
                        && !setting.getEnumSettingValue().getValue().equals(
                                StringConstants.DISABLED));
    }

    /**
     * OCP or MCP have plan sub-types.
     *
     * @param scenarioInfo Scenario info having plan type.
     * @return Whether plan has a sub-type.
     */
    public static boolean hasPlanSubType(@Nonnull final ScenarioInfo scenarioInfo) {
        return StringConstants.OPTIMIZE_CLOUD_PLAN.equals(scenarioInfo.getType())
                || StringConstants.CLOUD_MIGRATION_PLAN.equals(scenarioInfo.getType());
    }

    /**
     * Get cloud plan subtype for this scenario.
     *
     * @param scenarioInfo Input scenarioInfo
     * @return The cloud plan sub type
     */
    public static String getCloudPlanSubType(ScenarioInfo scenarioInfo) {
        if (StringConstants.CLOUD_MIGRATION_PLAN.equals(scenarioInfo.getType())) {
            return isScalingEnabled(scenarioInfo)
                    ? CLOUD_MIGRATION_PLAN__CONSUMPTION : CLOUD_MIGRATION_PLAN__ALLOCATION;
        }
        final boolean isRIBuyEnabled = !scenarioInfo.getChangesList()
                .stream()
                .filter(ScenarioChange::hasRiSetting)
                .collect(Collectors.toList()).isEmpty();
        String planSubType;
        if (isRIBuyEnabled) {
            planSubType = isScalingEnabled(scenarioInfo)
                    ? StringConstants.OPTIMIZE_CLOUD_PLAN__RIBUY_AND_OPTIMIZE_SERVICES
                    : StringConstants.OPTIMIZE_CLOUD_PLAN__RIBUY_ONLY;
        } else {
            planSubType = StringConstants.OPTIMIZE_CLOUD_PLAN__OPTIMIZE_SERVICES;
        }
        return planSubType;
    }

    /**
     * Check whether BuyRI costs need to be updated in DB after a plan successfully completes.
     * This needs to be done for MPC plans, as main market and BuyRI processing happen at different
     * unpredictable times, and we need to wait for both to finish, before updating some final costs
     * in DB tables.
     *
     * @param planInstance Instance of plan to check.
     * @return Whether BuyRI costs should be updated for this plan, only true for MPC plans for now.
     */
    public static boolean updateBuyRICostsOnPlanCompletion(@Nonnull final PlanInstance planInstance) {
        return planInstance.getProjectType() == PlanProjectType.CLOUD_MIGRATION;
    }
}
