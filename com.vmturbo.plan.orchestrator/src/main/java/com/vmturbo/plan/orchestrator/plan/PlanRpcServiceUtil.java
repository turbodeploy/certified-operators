package com.vmturbo.plan.orchestrator.plan;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.utils.StringConstants;
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
        // when there is buy RI involved, trigger it first
        RISetting riSetting = riScenario.getRiSetting();
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
                                .setPlanSubType(getPlanSubType(scenarioInfo)))
                                .build())
                .setPurchaseProfile(RIPurchaseProfile.newBuilder()
                        .setRiType(ReservedInstanceType.newBuilder()
                                .setOfferingClass(riSetting.hasPreferredOfferingClass() ?
                                riSetting.getPreferredOfferingClass() : ReservedInstanceType
                                .OfferingClass.STANDARD)
                                .setPaymentOption(riSetting.hasPreferredPaymentOption() ?
                                riSetting.getPreferredPaymentOption()
                                : ReservedInstanceType.PaymentOption.ALL_UPFRONT)
                                .setTermYears(riSetting.hasPreferredTerm() ? riSetting.getPreferredTerm() : 1)));
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
                                : UIEntityType.fromStringToSdkType(className);
                scopeObjectByClass.computeIfAbsent(type, k -> new HashSet<>()).add(oid);
            }
        }
        // Resolve the groups
        if (groupIdsToResolve.size() > 0) {
            final GetMembersRequest.Builder membersRequest = GetMembersRequest.newBuilder()
                            .setExpandNestedGroups(true);
            groupIdsToResolve.forEach(id -> membersRequest.setId(id));
            final GroupDTO.GetMembersResponse response = groupServiceClient
                            .getMembers(membersRequest.build());
            if (response.hasMembers()) {
                final RetrieveTopologyEntitiesRequest getEntitiesrequest = RetrieveTopologyEntitiesRequest.newBuilder()
                                .addAllEntityOids(response.getMembers().getIdsList())
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
     * Check if scaling is enabled for this scenario.
     *
     * @param scenarioInfo Input scenarioInfo
     * @return true if scaling is enabled, false otherwise
     */
    public static boolean isScalingEnabled(ScenarioInfo scenarioInfo) {
        return scenarioInfo.getChangesList()
                .stream()
                .filter(sc -> sc.hasSettingOverride())
                .map(sc -> sc.getSettingOverride())
                .filter(so -> so.hasSetting())
                .map(so -> so.getSetting())
                .anyMatch(setting -> setting.getSettingSpecName()
                        .contains(EntitySettingSpecs.Resize.getSettingName().toLowerCase())
                        && !setting.getEnumSettingValue().getValue().equals(StringConstants.DISABLED));
    }

    /**
     * Get OCP subtype for this scenario.
     *
     * @param scenarioInfo Input scenarioInfo
     * @return The optimize cloud plan sub type
     */
    public static String getPlanSubType(ScenarioInfo scenarioInfo) {
        final boolean isScalingEnbld = scenarioInfo.getChangesList()
                .stream()
                .filter(sc -> sc.hasSettingOverride())
                .map(sc -> sc.getSettingOverride())
                .map(so -> so.getSetting())
                .anyMatch(setting -> setting.getSettingSpecName()
                        .contains(EntitySettingSpecs.Resize.getSettingName().toLowerCase())
                        && !setting.getEnumSettingValue().getValue().equals(StringConstants.DISABLED));
        final boolean isRIBuyEnabled = !scenarioInfo.getChangesList()
                .stream()
                .filter(c -> c.hasRiSetting())
                .collect(Collectors.toList()).isEmpty();
        String planSubType;
        if (isRIBuyEnabled) {
            planSubType = isScalingEnbld ? StringConstants.OPTIMIZE_CLOUD_PLAN__RIBUY_AND_OPTIMIZE_SERVICES :
                    StringConstants.OPTIMIZE_CLOUD_PLAN__RIBUY_ONLY;
        } else {
            planSubType = StringConstants.OPTIMIZE_CLOUD_PLAN__OPTIMIZE_SERVICES;
        }
        return planSubType;
    }
}
