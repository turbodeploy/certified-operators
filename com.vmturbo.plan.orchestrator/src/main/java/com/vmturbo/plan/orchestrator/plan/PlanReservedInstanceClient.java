package com.vmturbo.plan.orchestrator.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByIdRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForScopeRequest;
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


/**
 * Sends commands to the Plan Reserved Instance Service using gRPC.
 * Supports communication between the plan-orchestrator and other components.
 *
 */
public class PlanReservedInstanceClient {

    private final Logger logger = LogManager.getLogger();

    // The plan reserved instance service client.
    private PlanReservedInstanceServiceBlockingStub planRIService;

    // The reserved instance bought service client.
    private final ReservedInstanceBoughtServiceBlockingStub riBoughtService;

    // The realtime context id.
    private Long realtimeTopologyContextId;

    /**
     * Constructor.
     *
     * @param planRIService PlanReservedInstanceServiceBlockingStub.
     * @param reservedInstanceBoughtService ReservedInstanceBoughtServiceBlockingStub.
     * @param realtimeTopologyContextId The real-time topology context id.
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
     * @param seedEntities OIDs of entities that are in the scope of this plan. If a group was
     *                     chosen as scope, then we pass in the resolved group member OIDs here.
     */
     public void savePlanIncludedCoupons(@Nonnull PlanInstance planInstance,
                                         @Nonnull final List<Long> seedEntities) {
        List<Long> includedCouponOidList = new ArrayList<>();
        boolean includeAll = parsePlanIncludedCoupons(planInstance, includedCouponOidList);
        Long topologyContextId = planInstance.getPlanId();
        logger.trace("Seed IDs for plan {} are: {}", topologyContextId, seedEntities);
        // TODO:  One can only run OCP plan option 2 on groups at present, and it doesn't allow
        // the ability to select RIs to include.  But this could change, and we would need to add group
        // expansion here.
        // TODO:  If the Plan RI store won't have RIs when first run, then one would need to
        // get them from real-time as well.
        if (includeAll) { //include all RIs in scope.
            List<ReservedInstanceBought> allRiBought =
                    riBoughtService.getReservedInstanceBoughtForScope(
                            GetReservedInstanceBoughtForScopeRequest.newBuilder()
                                    .addAllScopeSeedOids(seedEntities)
                                    .build()).getReservedInstanceBoughtList();
            insertPlanIncludedCoupons(allRiBought, topologyContextId);
        } else if (!includedCouponOidList.isEmpty()) {  //include the selected RIs in scope.

            final GetReservedInstanceBoughtByIdRequest.Builder requestBuilder =
                                                              GetReservedInstanceBoughtByIdRequest
                                                                              .newBuilder();
            List<ReservedInstanceBought> riIncluded = riBoughtService
                            .getReservedInstanceBoughtById(requestBuilder
                                            .setRiFilter(RiFilter.newBuilder()
                                                            .addAllRiId(includedCouponOidList))
                                            .build())
                            .getReservedInstanceBoughtList();
            insertPlanIncludedCoupons(riIncluded, topologyContextId);
        } // else include no RIs; hence save none.
    }

    /**
     * Parse the Included RI OIDs from the plan's Scenario.
     *
     * @param planInstance  The plan Instance;
     * @param includedCouponOids The List of RI Oids to be populated
     * @return Returns true to include all RIs in scope, false otherwise.
     */
    @VisibleForTesting
    boolean parsePlanIncludedCoupons(@Nonnull PlanInstance planInstance,
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
