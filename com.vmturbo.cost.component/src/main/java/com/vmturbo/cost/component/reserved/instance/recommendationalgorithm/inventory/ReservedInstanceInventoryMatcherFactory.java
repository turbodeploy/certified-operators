package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.PlanReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.AccountGroupingIdentifier;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.AccountGroupingIdentifier.AccountGroupingType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.ImmutableAccountGroupingIdentifier;
import com.vmturbo.group.api.GroupAndMembers;

/**
 * A factory class for creating instances of {@link ReservedInstanceInventoryMatcher}.
 */
public class ReservedInstanceInventoryMatcherFactory {


    private final ReservedInstanceBoughtStore riBoughtStore;

    private final PlanReservedInstanceStore planReservedInstanceStore;

    /**
     * Construct a new instance of {@link ReservedInstanceInventoryMatcherFactory}.
     *
     * @param riBoughtStore The RI bought store, used in querying RI bought instances in realtime
     *                      when creating an instance of {@link ReservedInstanceInventoryMatcher}.
     * @param planRiBoughtStore The plan RI bought store used in querying RI bought instances in plans
     *                          when creating an instance of {@link ReservedInstanceInventoryMatcher}.
     */
    public ReservedInstanceInventoryMatcherFactory(@Nonnull ReservedInstanceBoughtStore riBoughtStore,
                                                   @Nonnull PlanReservedInstanceStore planRiBoughtStore) {
        this.riBoughtStore = Objects.requireNonNull(riBoughtStore);
        this.planReservedInstanceStore = Objects.requireNonNull(planRiBoughtStore);
    }

    /**
     * Creates a regionally scoped {@link ReservedInstanceInventoryMatcher}. The intent of scoping
     * the matcher to a region it to limit the load in parsing the inventory and avoid loading RI
     * inventory in regions for which there is no RI analysis demand.
     *
     * <p>The factory is responsible for also determining the {@link AccountGroupingIdentifier} for
     * each {@link ReservedInstanceBought} instance, indicating whether an RI is tied to a billing
     * family or is associated with a standalone account (the {@link ReservedInstanceInventoryMatcher}
     * will determine whether to use a billing family identifier, based on the scope of the RI).
     *
     * @param cloudTopology The cloud topology, used to query the billing family associated with each
     *                      RI.
     * @param riSpecMatcher The {@link ReservedInstanceSpecMatcher}, used by the {@link ReservedInstanceInventoryMatcher}
     *                      in determining matches between demand clusters and RI inventory.
     * @param regionOid The target region OID. The returned matcher will only have RI inventory for
     *                  instances associated with this region OID (regardless of whether the RI is
     *                  regional or zonal).
     * @param topologyInfo The topologyInfo used for querying the correct
     * @return A newly created {@link ReservedInstanceInventoryMatcher} instance.
     */
    public ReservedInstanceInventoryMatcher createRegionalMatcher(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull ReservedInstanceSpecMatcher riSpecMatcher,
            @Nonnull TopologyInfo topologyInfo,
            long regionOid) {

        final List<ReservedInstanceBought> riBoughtInstances;
        if (topologyInfo.getTopologyType() == TopologyType.REALTIME) {
           riBoughtInstances = riBoughtStore.getReservedInstanceBoughtByFilter(
                            ReservedInstanceBoughtFilter.newBuilder()
                                    .regionFilter(RegionFilter.newBuilder()
                                            .addRegionId(regionOid)
                                            .build())
                                    .build());
        } else {
            riBoughtInstances = planReservedInstanceStore.getReservedInstanceBoughtByPlanId(topologyInfo.getTopologyContextId());
        }

        Map<Long, AccountGroupingIdentifier> riToAccountGroupIdMap =
                mapRIBoughtInstancesToAccountGroupingId(cloudTopology, riBoughtInstances);

        return new ReservedInstanceInventoryMatcher(riSpecMatcher,
                riBoughtInstances,
                riToAccountGroupIdMap);

    }

    private Map<Long, AccountGroupingIdentifier> mapRIBoughtInstancesToAccountGroupingId(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull Collection<ReservedInstanceBought> riBoughtInstances) {

        final ImmutableMap.Builder riToAccountGroupingIdBuilder = ImmutableMap.builder();
        final Map<Long, AccountGroupingIdentifier> accountOidToGroupingIdentifier =
                new HashMap<>();

        for (ReservedInstanceBought riBought : riBoughtInstances) {

            long purchasingAccountOid = riBought.getReservedInstanceBoughtInfo()
                    .getBusinessAccountId();

            final AccountGroupingIdentifier accountGroupingId = accountOidToGroupingIdentifier.computeIfAbsent(
                    purchasingAccountOid,
                    accountOid -> {

                        final Optional<GroupAndMembers> billingFamily =
                                cloudTopology.getBillingFamilyForEntity(purchasingAccountOid);

                        ImmutableAccountGroupingIdentifier accountGroupingIdentifier =
                                ImmutableAccountGroupingIdentifier.builder()
                                        .groupingType(billingFamily.isPresent() ?
                                                AccountGroupingType.BILLING_FAMILY :
                                                AccountGroupingType.STANDALONE_ACCOUNT)
                                        .id(billingFamily
                                                .map(g -> g.group().getId())
                                                .orElse(purchasingAccountOid))
                                        .build();

                        return accountGroupingIdentifier;
                    });
            riToAccountGroupingIdBuilder.put(riBought.getId(), accountGroupingId);
        }

        return riToAccountGroupingIdBuilder.build();
    }

}
