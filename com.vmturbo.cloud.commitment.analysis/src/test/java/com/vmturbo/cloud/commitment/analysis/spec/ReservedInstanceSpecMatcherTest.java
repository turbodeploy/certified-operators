package com.vmturbo.cloud.commitment.analysis.spec;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecMatcher.ReservedInstanceSpecMatcherFactory;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class ReservedInstanceSpecMatcherTest {


    private final ReservedInstanceSpecMatcherFactory reservedInstanceSpecMatcherFactory =
            new ReservedInstanceSpecMatcherFactory();

    private final RISpecPurchaseFilter riSpecPurchaseFilter = mock(RISpecPurchaseFilter.class);

    private final ReservedInstanceSpecMatcher reservedInstanceSpecMatcher =
            reservedInstanceSpecMatcherFactory.newMatcher(riSpecPurchaseFilter);

    private final VirtualMachineCoverageScope matchingCoverageScope = ImmutableVirtualMachineCoverageScope.builder()
            .regionOid(1L)
            .cloudTierOid(2L)
            .osType(OSType.RHEL)
            .tenancy(Tenancy.HOST)
            .build();

    private final ReservedInstanceSpecData matchingSpecData = ImmutableReservedInstanceSpecData
            .builder()
            .spec(ReservedInstanceSpec.newBuilder()
                    .setId(3L)
                    .build())
            .cloudTier(TopologyEntityDTO.newBuilder()
                    .setEnvironmentType(EnvironmentType.CLOUD)
                    .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                    .setOid(4L)
                    .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                            .setComputeTier(ComputeTierInfo.newBuilder()
                                    .setNumCoupons(1)
                                    .setFamily("a")))
                    .build())
            .build();

    @Before
    public void setup() {
        when(riSpecPurchaseFilter.getAvailableRegionalSpecs(any(), anyLong()))
                .thenReturn(ImmutableMap.of(matchingCoverageScope, matchingSpecData));
    }

    @Test
    public void testMatching() {

        final ScopedCloudTierInfo scopedCloudTierDemand = ScopedCloudTierInfo.builder()
                .accountOid(6L)
                .regionOid(matchingCoverageScope.regionOid())
                .serviceProviderOid(7L)
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(matchingCoverageScope.cloudTierOid())
                        .osType(matchingCoverageScope.osType())
                        .tenancy(matchingCoverageScope.tenancy())
                        .build())
                .build();


        final Optional<ReservedInstanceSpecData> specData =
                reservedInstanceSpecMatcher.matchDemandToSpecs(scopedCloudTierDemand);

        assertTrue(specData.isPresent());
        assertThat(specData.get(), equalTo(matchingSpecData));
    }
}
