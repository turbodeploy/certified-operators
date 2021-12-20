package com.vmturbo.reserved.instance.coverage.allocator;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.AggregationFailureException;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.commitment.aggregator.DefaultCloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.DefaultCloudCommitmentAggregator.DefaultCloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.identity.IdentityProvider;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.BillingFamilyRetrieverFactory.DefaultBillingFamilyRetrieverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory.DefaultCoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.ComputeCommitmentMatcher.ComputeCommitmentMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.CoverageEntityMatcher.CoverageEntityMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.entity.DefaultCoverageEntityMatcher.DefaultCoverageEntityMatcherFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.ConfigurableCoverageRule.ConfigurableCoverageRuleFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.CoverageRulesFactory;
import com.vmturbo.reserved.instance.coverage.allocator.rules.filter.CloudCommitmentFilterFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

public class AbstractReservedInstanceCoverageAllocatorTest {

    protected final CloudCommitmentFilterFactory cloudCommitmentFilterFactory =
            new CloudCommitmentFilterFactory();

    protected final ComputeCommitmentMatcherFactory computeCommitmentMatcherFactory =
            new ComputeCommitmentMatcherFactory();

    protected final ConfigurableCoverageRuleFactory coverageRuleFactory = new ConfigurableCoverageRuleFactory(
            cloudCommitmentFilterFactory, computeCommitmentMatcherFactory);

    protected final CoverageEntityMatcherFactory coverageEntityMatcherFactory =
            new DefaultCoverageEntityMatcherFactory();

    protected final CoverageRulesFactory coverageRulesFactory = new CoverageRulesFactory(
            coverageRuleFactory, coverageEntityMatcherFactory);

    protected final CoverageAllocatorFactory allocatorFactory = new DefaultCoverageAllocatorFactory(
            coverageRulesFactory);

    protected final GroupMemberRetriever groupMemberRetriever = mock(GroupMemberRetriever.class);

    protected final IdentityProvider identityProvider = mock(IdentityProvider.class);

    protected final BillingFamilyRetrieverFactory billingFamilyRetrieverFactory =
            new DefaultBillingFamilyRetrieverFactory(groupMemberRetriever);

    protected ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory =
            new ComputeTierFamilyResolverFactory();

    protected final CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory =
            new DefaultCloudCommitmentAggregatorFactory(
                    identityProvider,
                    computeTierFamilyResolverFactory,
                    billingFamilyRetrieverFactory);

    protected CoverageTopology generateCoverageTopology(
            SDKProbeType cspType,
            @Nonnull Set<ReservedInstanceBought> reservedInstances,
            @Nonnull Set<ReservedInstanceSpec> riSpecs,
            @Nonnull GroupMemberRetriever groupMemberRetriever,
            TopologyEntityDTO... entityDtos) {

        TopologyEntityCloudTopologyFactory cloudTopologyFactory =
            new DefaultTopologyEntityCloudTopologyFactory(groupMemberRetriever);

        ThinTargetCache mockTargetCache = mock(ThinTargetCache.class);
        final ThinTargetInfo targetInfo = ImmutableThinTargetInfo.builder()
                .oid(1)
                .displayName("mockedThinTargetInfo")
                .isHidden(false)
                .probeInfo(ImmutableThinProbeInfo.builder()
                        .oid(1)
                        .type(cspType.toString())
                        .category("Cloud")
                        .uiCategory("Cloud")
                        .build())
                .build();
        when(mockTargetCache.getTargetInfo(anyLong())).thenReturn(Optional.of(targetInfo));

        final CloudTopology<TopologyEntityDTO> cloudTopology =
                cloudTopologyFactory.newCloudTopology(Arrays.stream(entityDtos));
        final Set<CloudCommitmentAggregate> commitmentAggregates =
                createRIAggregates(reservedInstances, riSpecs, cloudTopology);

        CoverageTopologyFactory coverageTopologyFactory = new CoverageTopologyFactory(mockTargetCache);
        return coverageTopologyFactory.createCoverageTopology(
                cloudTopology,
                commitmentAggregates);
    }

    private Set<CloudCommitmentAggregate> createRIAggregates(@Nonnull Set<ReservedInstanceBought> reservedInstances,
                                                                @Nonnull Set<ReservedInstanceSpec> riSpecs,
                                                                @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

        final Map<Long, ReservedInstanceSpec> riSpecsById = riSpecs.stream()
                .collect(ImmutableMap.toImmutableMap(
                        ReservedInstanceSpec::getId,
                        Function.identity()));



        final Set<ReservedInstanceData> riDataSet = reservedInstances.stream()
                .map(ri -> {
                    final long riSpecId = ri.getReservedInstanceBoughtInfo().getReservedInstanceSpec();
                    final ReservedInstanceSpec riSpec = riSpecsById.get(riSpecId);

                    if (riSpec != null) {
                        return ReservedInstanceData.builder()
                                .spec(riSpec)
                                .commitment(ri)
                                .build();
                    } else {
                        return null;
                    }
                }).filter(Objects::nonNull)
                .collect(Collectors.toSet());


        final CloudCommitmentAggregator cloudCommitmentAggregator =
                cloudCommitmentAggregatorFactory.newIdentityAggregator(cloudTopology);

        riDataSet.forEach(riData -> {
            try {
                cloudCommitmentAggregator.collectCommitment(riData);
            } catch (AggregationFailureException e) {
                e.printStackTrace();
            }
        });

        return cloudCommitmentAggregator.getAggregates();
    }
}
