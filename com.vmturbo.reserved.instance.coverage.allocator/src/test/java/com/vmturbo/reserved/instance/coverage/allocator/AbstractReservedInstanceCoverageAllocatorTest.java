package com.vmturbo.reserved.instance.coverage.allocator;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.reserved.instance.coverage.allocator.RICoverageAllocatorFactory.DefaultRICoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;
import com.vmturbo.topology.processor.api.util.ImmutableThinProbeInfo;
import com.vmturbo.topology.processor.api.util.ImmutableThinTargetInfo;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;
import com.vmturbo.topology.processor.api.util.ThinTargetCache.ThinTargetInfo;

public class AbstractReservedInstanceCoverageAllocatorTest {

    protected final RICoverageAllocatorFactory allocatorFactory = new DefaultRICoverageAllocatorFactory();

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
                        .build())
                .build();
        when(mockTargetCache.getTargetInfo(anyLong())).thenReturn(Optional.of(targetInfo));
        final CloudTopology<TopologyEntityDTO> cloudTopology =
                cloudTopologyFactory.newCloudTopology(Arrays.stream(entityDtos));

        CoverageTopologyFactory coverageTopologyFactory = new CoverageTopologyFactory(mockTargetCache);
        return coverageTopologyFactory.createCoverageTopology(
                cloudTopology,
                riSpecs,
                reservedInstances);
    }
}
