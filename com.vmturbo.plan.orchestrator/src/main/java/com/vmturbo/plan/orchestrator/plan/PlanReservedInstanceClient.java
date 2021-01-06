package com.vmturbo.plan.orchestrator.plan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter.AccountFilterType;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtForScopeRequest;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.RiFilter;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.IncludedCoupons;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.MigrationReference;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

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
     * @param repositoryClient repository client
     */
    public void savePlanIncludedCoupons(@Nonnull PlanInstance planInstance,
                                        @Nonnull final List<Long> seedEntities,
                                        @Nonnull final RepositoryClient repositoryClient) {
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
        } else {
            final GetReservedInstanceBoughtByFilterRequest.Builder requestBuilder =
                    GetReservedInstanceBoughtByFilterRequest.newBuilder();
            // This set will store the destination account, if it is specified in the scenario.
            final Set<Long> accountSet = new HashSet<>();
            if (planInstance.hasScenario() && planInstance.getScenario().hasScenarioInfo()) {
                planInstance.getScenario().getScenarioInfo().getChangesList().stream()
                        .filter(ScenarioChange::hasTopologyMigration)
                        .map(ScenarioChange::getTopologyMigration)
                        .findFirst()
                        .ifPresent(topologyMigration -> {
                            if (topologyMigration.hasDestinationAccount()
                                    && topologyMigration.getDestinationAccount().hasOid()) {
                                final long accountOid = topologyMigration.getDestinationAccount().getOid();
                                accountSet.add(accountOid);
                                // Filter for RIs purchased by the accounts in the same billing family (AWS)
                                // or in the same Azure EA account.
                                Set<Long> relatedAccounts = repositoryClient.getAllRelatedBusinessAccountOids(accountOid);
                                requestBuilder.setAccountFilter(AccountFilter.newBuilder()
                                        .setAccountFilterType(AccountFilterType.PURCHASED_BY)
                                        .addAllAccountId(relatedAccounts).build());
                                logger.info("Plan {} will use RIs bought by the following accounts: {}",
                                        topologyContextId, relatedAccounts);
                            }

                            topologyMigration.getDestinationList().stream()
                                    .filter(d -> d.getEntityType() == EntityType.REGION_VALUE)
                                    .findFirst()
                                    .map(MigrationReference::getOid)
                                    .ifPresent(regionOid -> requestBuilder.setRegionFilter(
                                            RegionFilter.newBuilder().addRegionId(regionOid).build()));
                        });
            }

            if (!includedCouponOidList.isEmpty()) {
                requestBuilder.setRiFilter(RiFilter.newBuilder().addAllRiId(includedCouponOidList));
            }
            List<ReservedInstanceBought> riIncluded = riBoughtService
                    .getReservedInstanceBoughtByFilter(requestBuilder.build())
                    .getReservedInstanceBoughtsList();

            // If accounts were considered when filtering RIs, RIs bought by related accounts will
            // be return. However, if an RI is not bought by the destination account, and it is not
            // a "shared" RI, it need to be removed from the result.
            if (!accountSet.isEmpty()) {
                riIncluded = riIncluded.stream()
                        .filter(boughtReservedInstance -> {
                            if (!boughtReservedInstance.hasReservedInstanceBoughtInfo()) {
                                return false;
                            }
                            ReservedInstanceBought.ReservedInstanceBoughtInfo reservedInstanceBoughtInfo =
                                    boughtReservedInstance.getReservedInstanceBoughtInfo();
                            if (!reservedInstanceBoughtInfo.hasReservedInstanceScopeInfo()) {
                                return false;
                            }
                            ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo scopeInfo =
                                    reservedInstanceBoughtInfo.getReservedInstanceScopeInfo();
                            if (scopeInfo.getShared()) {
                                return true;
                            }
                            return scopeInfo.getApplicableBusinessAccountIdList().containsAll(accountSet);
                        })
                        .collect(Collectors.toList());
            }
            if (!riIncluded.isEmpty()) {
                insertPlanIncludedCoupons(riIncluded, topologyContextId);
            }
        }
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
                                                      .setTopologyContextId(planId)
                                                      .addAllReservedInstanceBought(planRIs)
                                                      .build();

        planRIService.insertPlanReservedInstanceBought(insertRiRequest);
    }
}
