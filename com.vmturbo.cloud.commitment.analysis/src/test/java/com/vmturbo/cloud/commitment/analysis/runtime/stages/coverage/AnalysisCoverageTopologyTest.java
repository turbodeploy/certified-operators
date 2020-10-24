package com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.AnalysisCoverageTopology.AnalysisCoverageTopologyFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregateInfo;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregateInfo.PlatformInfo;
import com.vmturbo.cloud.common.commitment.aggregator.ReservedInstanceAggregateInfo.TierInfo;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.identity.IdentityProvider.DefaultIdentityProvider;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CloudAggregationInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageEntityInfo;
import com.vmturbo.reserved.instance.coverage.allocator.topology.VirtualMachineInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

public class AnalysisCoverageTopologyTest {

    private final IdentityProvider identityProvider = new DefaultIdentityProvider(0);

    private final ThinTargetCache thinTargetCache = mock(ThinTargetCache.class);

    private final CloudTopology cloudTierTopology = mock(CloudTopology.class);

    private final MinimalCloudTopology cloudTopology = mock(MinimalCloudTopology.class);

    private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory =
            mock(ComputeTierFamilyResolverFactory.class);

    private final ComputeTierFamilyResolver computeTierFamilyResolver =
            mock(ComputeTierFamilyResolver.class);

    private final AnalysisCoverageTopologyFactory topologyFactory = new AnalysisCoverageTopologyFactory(
            identityProvider, thinTargetCache, computeTierFamilyResolverFactory);


    private final ReservedInstanceAggregateInfo riAggregateInfoA = ReservedInstanceAggregateInfo.builder()
            .billingFamilyId(1L)
            .purchasingAccountOid(2L)
            .regionOid(3L)
            .tierInfo(TierInfo.builder()
                    .tierFamily("A")
                    .tierType(EntityType.COMPUTE_TIER)
                    .tierOid(4L)
                    .isSizeFlexible(true)
                    .build())
            .platformInfo(PlatformInfo.builder()
                    .isPlatformFlexible(true)
                    .build())
            .scopeInfo(ReservedInstanceScopeInfo.newBuilder()
                    .setShared(true)
                    .build())
            .build();
    private final ReservedInstanceAggregateInfo riAggregateInfoB = ReservedInstanceAggregateInfo.builder()
            .from(riAggregateInfoA)
            .regionOid(7L)
            .build();
    private final ReservedInstanceAggregate riAggregateA = ReservedInstanceAggregate.builder()
            .aggregateId(5L)
            .aggregateInfo(riAggregateInfoA)
            .build();
    private final ReservedInstanceAggregate riAggregateB = ReservedInstanceAggregate.builder()
            .aggregateId(6L)
            .aggregateInfo(riAggregateInfoB)
            .build();

    // Setup the aggregated cloud tier demand
    private final AggregateCloudTierDemand demandA = AggregateCloudTierDemand.builder()
            .cloudTierInfo(ScopedCloudTierInfo.builder()
                    .cloudTierDemand(ComputeTierDemand.builder()
                            .cloudTierOid(1)
                            .osType(OSType.RHEL)
                            .tenancy(Tenancy.DEFAULT)
                            .build())
                    .accountOid(2)
                    .billingFamilyId(11L)
                    .regionOid(3)
                    .availabilityZoneOid(12L)
                    .serviceProviderOid(4)
                    .build())
            .classification(DemandClassification.of(AllocatedDemandClassification.ALLOCATED))
            .putDemandByEntity(
                    EntityInfo.builder().entityOid(6).build(),
                    1.0)
            .putDemandByEntity(
                    EntityInfo.builder().entityOid(7).build(),
                    2.0)
            .build();
    private final AggregateCloudTierDemand demandB = AggregateCloudTierDemand.builder()
            .from(demandA)
            .cloudTierInfo(ScopedCloudTierInfo.builder()
                    .from(demandA.cloudTierInfo())
                    .accountOid(5)
                    .build())
            .build();

    @Before
    public void setup() {
        when(computeTierFamilyResolverFactory.createResolver(any())).thenReturn(computeTierFamilyResolver);
    }

    @Test
    public void testGetAggregatedDemandById() {

        // construct analysis topology
        final AnalysisCoverageTopology analysisTopology = topologyFactory.newTopology(
                cloudTierTopology,
                cloudTopology,
                ImmutableSet.of(demandA, demandB),
                Collections.emptySet(),
                Collections.emptyMap());

        // ASSERTIONS
        assertThat(analysisTopology.getAggregatedDemandById().values(), hasSize(2));
        assertThat(analysisTopology.getAggregatedDemandById().values(), containsInAnyOrder(demandA, demandB));
    }

    @Test
    public void testGetCommitmentAggregatesById() {

        // construct analysis topology
        final AnalysisCoverageTopology analysisTopology = topologyFactory.newTopology(
                cloudTierTopology,
                cloudTopology,
                Collections.emptySet(),
                ImmutableSet.of(riAggregateA, riAggregateB),
                Collections.emptyMap());

        // ASSERTIONS
        assertThat(analysisTopology.getCommitmentAggregatesById().values(), hasSize(2));
        assertThat(analysisTopology.getCommitmentAggregatesById().values(), containsInAnyOrder(riAggregateA, riAggregateB));

        assertThat(analysisTopology.getAllRIAggregates(), hasSize(2));
        assertThat(analysisTopology.getAllRIAggregates(), containsInAnyOrder(riAggregateA, riAggregateB));
    }

    @Test
    public void testGetCommitmentCapacityByOid() {

        final Map<Long, Double> commitmentCapacityMap = ImmutableMap.of(
                1L, 2.0,
                3L, 4.0);

        // construct analysis topology
        final AnalysisCoverageTopology analysisTopology = topologyFactory.newTopology(
                cloudTierTopology,
                cloudTopology,
                Collections.emptySet(),
                Collections.emptySet(),
                commitmentCapacityMap);

        // ASSERTIONS
        assertThat(analysisTopology.getCommitmentCapacityByOid(), equalTo(commitmentCapacityMap));
    }

    @Test
    public void testGetCoverageCapacityForEntity() {

        // setup the compute tier family resolver
        when(computeTierFamilyResolver.getNumCoupons(anyLong())).thenReturn(Optional.of(4L));

        // construct analysis topology
        final AnalysisCoverageTopology analysisTopology = topologyFactory.newTopology(
                cloudTierTopology,
                cloudTopology,
                ImmutableSet.of(demandA),
                Collections.emptySet(),
                Collections.emptyMap());

        // ASSERTIONS
        long demandAID = analysisTopology.getAggregatedDemandById().entrySet()
                .stream()
                .filter(e -> e.getValue().equals(demandA))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Cant find demand ID"));
        // should be equal to 3.0 (from aggregate) * 4 (for coupon value)
        assertThat(analysisTopology.getCoverageCapacityForEntity(demandAID), equalTo(12.0));
        // check a random ID not contained within the aggregate demand map
        assertThat(analysisTopology.getCoverageCapacityForEntity(1231232L), equalTo(0.0));
    }

    @Test
    public void testGetEntityInfo() {

        // construct analysis topology
        final AnalysisCoverageTopology analysisTopology = topologyFactory.newTopology(
                cloudTierTopology,
                cloudTopology,
                ImmutableSet.of(demandA),
                Collections.emptySet(),
                Collections.emptyMap());

        // invoke the target method
        long demandAID = analysisTopology.getAggregatedDemandById().entrySet()
                .stream()
                .filter(e -> e.getValue().equals(demandA))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Cant find demand ID"));

        final Optional<CoverageEntityInfo> entityInfo = analysisTopology.getEntityInfo(demandAID);

        // ASSERTIONS
        assertTrue(entityInfo.isPresent());
        assertThat(entityInfo.get(), instanceOf(VirtualMachineInfo.class));
        final VirtualMachineInfo vmInfo = (VirtualMachineInfo)entityInfo.get();
        assertThat(vmInfo.entityState(), equalTo(EntityState.POWERED_ON));
        assertThat(vmInfo.platform(), equalTo(((ComputeTierDemand)demandA.cloudTierInfo().cloudTierDemand()).osType()));
        // check an empty ID
        assertThat(analysisTopology.getEntityInfo(21129323L), equalTo(Optional.empty()));
    }

    @Test
    public void testGetAggregationInfo() {

        // construct analysis topology
        final AnalysisCoverageTopology analysisTopology = topologyFactory.newTopology(
                cloudTierTopology,
                cloudTopology,
                ImmutableSet.of(demandA),
                Collections.emptySet(),
                Collections.emptyMap());

        // invoke the target method
        long demandAID = analysisTopology.getAggregatedDemandById().entrySet()
                .stream()
                .filter(e -> e.getValue().equals(demandA))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Cant find demand ID"));

        final Optional<CloudAggregationInfo> aggregationInfo = analysisTopology.getAggregationInfo(demandAID);

        final ScopedCloudTierInfo demandATierInfo = demandA.cloudTierInfo();
        assertTrue(aggregationInfo.isPresent());
        assertThat(aggregationInfo.get().billingFamilyId(), equalTo(OptionalLong.of(demandATierInfo.billingFamilyId().get())));
        assertThat(aggregationInfo.get().accountOid(), equalTo(demandATierInfo.accountOid()));
        assertThat(aggregationInfo.get().regionOid(), equalTo(demandATierInfo.regionOid()));
        assertThat(aggregationInfo.get().zoneOid(), equalTo(OptionalLong.of(demandATierInfo.availabilityZoneOid().get())));
        // check a random ID
        assertFalse(analysisTopology.getAggregationInfo(12321321L).isPresent());
    }

    @Test
    public void testGetComputeTierInfoForEntity() {

        // setup the cloud tier topology
        final String family = "familyTest";
        final long computeTierOid = demandA.cloudTierInfo().cloudTierDemand().cloudTierOid();
        final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
                .setOid(computeTierOid)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setComputeTier(ComputeTierInfo.newBuilder()
                                .setFamily(family)
                                .build())
                        .build())
                .build();

        when(cloudTierTopology.getEntity(eq(computeTierOid))).thenReturn(Optional.of(computeTier));

        // construct analysis topology
        final AnalysisCoverageTopology analysisTopology = topologyFactory.newTopology(
                cloudTierTopology,
                cloudTopology,
                ImmutableSet.of(demandA),
                Collections.emptySet(),
                Collections.emptyMap());

        // invoke the target method
        long demandAID = analysisTopology.getAggregatedDemandById().entrySet()
                .stream()
                .filter(e -> e.getValue().equals(demandA))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Cant find demand ID"));
        final Optional<com.vmturbo.reserved.instance.coverage.allocator.topology.ComputeTierInfo>
                computeTierInfo = analysisTopology.getComputeTierInfoForEntity(demandAID);

        // ASSERTIONS
        assertTrue(computeTierInfo.isPresent());
        assertThat(computeTierInfo.get().family(), equalTo(Optional.of(family)));
        assertThat(computeTierInfo.get().tierOid(), equalTo(computeTierOid));
        // check an empty response
        assertFalse(analysisTopology.getComputeTierInfoForEntity(12312312L).isPresent());
    }
}
