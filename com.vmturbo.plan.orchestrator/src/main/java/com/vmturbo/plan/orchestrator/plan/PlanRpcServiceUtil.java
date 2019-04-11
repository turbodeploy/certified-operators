package com.vmturbo.plan.orchestrator.plan;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class PlanRpcServiceUtil {

    private static final ImmutableSet<OSType> platforms = ImmutableSet
                    .of(OSType.UNKNOWN_OS, OSType.LINUX, OSType.SUSE, OSType.RHEL, OSType.WINDOWS,
                        OSType.WINDOWS_WITH_SQL_STANDARD, OSType.WINDOWS_WITH_SQL_WEB,
                        OSType.WINDOWS_WITH_SQL_ENTERPRISE, OSType.LINUX_WITH_SQL_STANDARD,
                        OSType.LINUX_WITH_SQL_WEB, OSType.LINUX_WITH_SQL_ENTERPRISE);

    private static final ImmutableSet<Tenancy> tenacies = ImmutableSet
                    .of(Tenancy.DEFAULT, Tenancy.DEDICATED, Tenancy.HOST);

    /**
     * Create StartBuyRIAnalysisRequest
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
        final StartBuyRIAnalysisRequest.Builder buyRiRequest = StartBuyRIAnalysisRequest
                .newBuilder().setTopologyInfo(TopologyInfo.newBuilder()
                        .setTopologyContextId(planId)
                        .setTopologyType(TopologyType.PLAN)
                        .setPlanInfo(PlanTopologyInfo.newBuilder()
                                .setPlanType(scenarioInfo.getType()))
                                .build())
                .addAllPlatforms(platforms) // currently buy RI use all platforms
                .addAllTenancies(tenacies) // currently buy RI use all tenancies
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
        if (regionIds !=null && !regionIds.isEmpty()) {
            buyRiRequest.addAllRegions(regionIds);
        }
        if (baIds != null && !baIds.isEmpty()) {
            buyRiRequest.addAllAccounts(baIds);
        }
        return buyRiRequest.build();
    }
}
