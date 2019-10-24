package com.vmturbo.plan.orchestrator.plan;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
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
     * @return StartBuyRIAnalysisRequest
     */
    public static StartBuyRIAnalysisRequest createBuyRIRequest(@Nonnull ScenarioInfo scenarioInfo,
                                          @Nonnull ScenarioChange riScenario, long planId) {
        // when there is buy RI involved, trigger it first
        Map<String, Set<Long>> scopeObjectByClass = new HashMap();
        for (PlanScopeEntry entry : scenarioInfo.getScope().getScopeEntriesList()) {
            String className = entry.getClassName().toUpperCase();
            long oid = entry.getScopeObjectOid();
            if (scopeObjectByClass.containsKey(className)) {
                Set<Long> oidSet = scopeObjectByClass.get(className);
                oidSet.add(oid);
            } else {
                scopeObjectByClass.put(className, new HashSet<Long>(Arrays.asList(oid)));
            }
        }
        Set<Long> regionIds = scopeObjectByClass.get(EntityType.REGION.name());
        // Note: in planScopeEntry the class name for ba is BusinessAccount
        // yet in the commonDTO.EntityType, ba is BUSINESS_ACCOUNT
        Set<Long> baIds = scopeObjectByClass.get(StringConstants.BUSINESS_ACCOUNT);
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
        if (regionIds != null && !regionIds.isEmpty()) {
            buyRiRequest.addAllRegions(regionIds);
        }
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
                        .equals(EntitySettingSpecs.Resize.getSettingName())
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
                        .equals(EntitySettingSpecs.Resize.getSettingName())
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
