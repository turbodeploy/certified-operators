package com.vmturbo.plan.orchestrator.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByIdRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByTopologyRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.RiFilter;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.IncludedCoupons;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.utils.StringConstants;

/**
 * Sends commands to the Plan Reserved Instance Service using gRPC.
 * Supports communication between the plan-orchestrator and other components.
 *
 */
public class PlanReservedInstanceClient {

    private PlanReservedInstanceServiceBlockingStub planRIService;

    private final ReservedInstanceBoughtServiceBlockingStub riBoughtService;

    private Long realtimeTopologyContextId;

    /**
     * Constructor.
     *
     * @param planRIService PlanReservedInstanceServiceBlockingStub.
     * @param reservedInstanceBoughtService ReservedInstanceBoughtServiceBlockingStub.
     */
    public PlanReservedInstanceClient(@Nonnull PlanReservedInstanceServiceBlockingStub planRIService,
                      @Nonnull ReservedInstanceBoughtServiceBlockingStub reservedInstanceBoughtService,
                      final Long realtimeTopologyContextId) {
        this.planRIService = planRIService;
        this.riBoughtService = reservedInstanceBoughtService;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /** Save the list of {@link ReservedInstanceBought} which belong to the input scope, and are selected
     * by the user to be included in the plan.
     *
     * @param planInstance The plan instance.
     */
     public void savePlanIncludedCoupons(@Nonnull PlanInstance planInstance) {
        ScenarioInfo scenarioInfo = planInstance.getScenario().getScenarioInfo();
        String planSubType = PlanRpcServiceUtil.getPlanSubType(scenarioInfo);
        // OCP Option #3 has to do with New Buy RI recommendations only, hence there's no
        // point in including or saving existing RIs to db
        if (StringConstants.OPTIMIZE_CLOUD_PLAN__RIBUY_ONLY.equals(planSubType)) {
            return;
        }
        List<Long> includedCouponOidsList = new ArrayList<>();
        boolean includeAll = parsePlanIncludedCoupons(planInstance, includedCouponOidsList);
        Long topologyContextId = planInstance.getPlanId();
        List<Long> seedEntities = new ArrayList<>();
        List<PlanScopeEntry> planScopeEntries = scenarioInfo.getScope().getScopeEntriesList();
        // TODO:  One can only run OCP plan option 2 on groups at present, and it doesn't allow
        // the ability to select RIs to include.  But this could change, and we would need to add group
        // expansion here.
        for (PlanScopeEntry scopeEntry : planScopeEntries) {
            seedEntities.add(scopeEntry.getScopeObjectOid());
        }
        // TODO:  If the Plan RI store won't have RIs when first run, then one would need to
        // get them from real-time as well.
        if (includeAll) { //include all RIs in scope.
            List<ReservedInstanceBought> allRiBought = riBoughtService
                            .getReservedInstanceBoughtByTopology(
                                             GetReservedInstanceBoughtByTopologyRequest
                                                 .newBuilder()
                                                 .setTopologyType(TopologyType.PLAN)
                                                 .setTopologyContextId(realtimeTopologyContextId)
                                                 .addAllScopeSeedOids(seedEntities)
                                                 .build())
                            .getReservedInstanceBoughtList();
            insertPlanIncludedCoupons(allRiBought, topologyContextId);
        } else if (!includedCouponOidsList.isEmpty()) {  //include the selected RIs in scope.

            final GetReservedInstanceBoughtByIdRequest.Builder requestBuilder =
                                                              GetReservedInstanceBoughtByIdRequest
                                                                              .newBuilder();
            List<ReservedInstanceBought> riIncluded = riBoughtService
                            .getReservedInstanceBoughtById(requestBuilder
                                            .setRiFilter(RiFilter.newBuilder()
                                                            .addAllRiId(includedCouponOidsList))
                                            .build())
                            .getReservedInstanceBoughtList();
            insertPlanIncludedCoupons(riIncluded, topologyContextId);
        } else { //Include no RIs; hence save none.
            return;
        }
    }

     /**
      * Parse the Included RI OIDs from the plan's Scenario.
      *
      * @param planInstance  The plan Instance;
      * @param includedCouponOids The List of RI Oids to be populated
      * @return Returns true to include all RIs in scope, false otherwise.
      */
    private boolean parsePlanIncludedCoupons(@Nonnull PlanInstance planInstance,
                                                 @Nonnull List<Long> includedCouponOids) {
        if (!planInstance.hasScenario()) {
            return false;
        }
        ScenarioInfo scenarioInfo = planInstance.getScenario().getScenarioInfo();
        boolean includeAll = false;
        List<PlanChanges> planChanges = scenarioInfo.getChangesList()
                        .stream().filter(ScenarioChange::hasPlanChanges)
                        .map(ScenarioChange::getPlanChanges)
                        .collect(Collectors.toList());
        if (!planChanges.isEmpty()) {
            Optional<IncludedCoupons> planIncludedCoupons = planChanges
                            .stream().filter(PlanChanges::hasIncludedCoupons)
                            .map(PlanChanges::getIncludedCoupons)
                            .findFirst();
            if (planIncludedCoupons.isPresent()) {
                includedCouponOids.addAll(planIncludedCoupons.get().getIncludedCouponOidsList()
                                .stream()
                                .collect(Collectors.toList()));
            } else {
                includeAll = true;
            }
        } else {
            includeAll = true;
        }
        return includeAll;
    }

    /**
     * Insert a list of Plan Included RIs by plan ID into data store.
     *
     * @param planRIs The plan RIs/Coupons to be saved.
     * @param planId The plan ID.
     */
    private void insertPlanIncludedCoupons(@Nonnull List<ReservedInstanceBought> planRIs,
                                           final Long planId) {
        // save the user selected RI included in the plan, to db.
        final UploadRIDataRequest insertRiRequest =
                                                  UploadRIDataRequest
                                                                  .newBuilder()
                                                                  .setTopologyId(planId)
                                                                  .addAllReservedInstanceBought(planRIs)
                                                                  .build();

        planRIService.insertPlanReservedInstanceBought(insertRiRequest);
    }
}
