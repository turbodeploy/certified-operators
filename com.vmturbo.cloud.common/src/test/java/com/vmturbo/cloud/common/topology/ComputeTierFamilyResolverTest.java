package com.vmturbo.cloud.common.topology;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Optional;
import java.util.stream.Stream;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Test;

import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierNotFoundException;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.IncompatibleTiersException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class ComputeTierFamilyResolverTest {

    private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory =
            new ComputeTierFamilyResolverFactory();

    private final TopologyEntityDTO computeTierSmallFamilyA = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(1L)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(1)
                            .setFamily("a")))
            .build();

    private final TopologyEntityDTO computeTierMediumFamilyA = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(2L)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(8)
                            .setFamily("a")))
            .build();

    private final TopologyEntityDTO computeTierLargeFamilyA = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(3L)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(10)
                            .setFamily("a")))
            .build();

    private final TopologyEntityDTO computeTierLargeBFamilyA = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(4L)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(10)
                            .setFamily("a")))
            .build();

    private final TopologyEntityDTO computeTierSmallFamilyB = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(5L)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setNumCoupons(1)
                            .setFamily("b")))
            .build();

    private final TopologyEntityDTO computeTierFamilyBZeroCoupons = TopologyEntityDTO.newBuilder()
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(5L)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setComputeTier(ComputeTierInfo.newBuilder()
                            .setFamily("b")))
            .build();

    private final CloudTopology<TopologyEntityDTO> cloudTopology =
            new DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class))
                    .newCloudTopology(Stream.of(
                            computeTierLargeFamilyA,
                            computeTierSmallFamilyA,
                            computeTierMediumFamilyA,
                            computeTierLargeBFamilyA,
                            computeTierSmallFamilyB,
                            computeTierFamilyBZeroCoupons));

    @Test
    public void testFamilyResolution() {

        final ComputeTierFamilyResolver computeTierFamilyResolver =
                computeTierFamilyResolverFactory.createResolver(cloudTopology);

        assertThat(computeTierFamilyResolver.getSiblingsInFamily(computeTierSmallFamilyA.getOid()),
                IsIterableContainingInOrder.contains(
                        computeTierSmallFamilyA.getOid(),
                        computeTierMediumFamilyA.getOid(),
                        computeTierLargeFamilyA.getOid(),
                        computeTierLargeBFamilyA.getOid()));
        assertThat(computeTierFamilyResolver.getSiblingsInFamily(computeTierMediumFamilyA.getOid()),
                IsIterableContainingInOrder.contains(
                        computeTierSmallFamilyA.getOid(),
                        computeTierMediumFamilyA.getOid(),
                        computeTierLargeFamilyA.getOid(),
                        computeTierLargeBFamilyA.getOid()));
        assertThat(computeTierFamilyResolver.getSiblingsInFamily(computeTierSmallFamilyA.getOid()),
                IsIterableContainingInOrder.contains(
                        computeTierSmallFamilyA.getOid(),
                        computeTierMediumFamilyA.getOid(),
                        computeTierLargeFamilyA.getOid(),
                        computeTierLargeBFamilyA.getOid()));
        assertThat(computeTierFamilyResolver.getSiblingsInFamily(computeTierLargeBFamilyA.getOid()),
                IsIterableContainingInOrder.contains(
                        computeTierSmallFamilyA.getOid(),
                        computeTierMediumFamilyA.getOid(),
                        computeTierLargeFamilyA.getOid(),
                        computeTierLargeBFamilyA.getOid()));

        assertThat(computeTierFamilyResolver.getSiblingsInFamily(computeTierSmallFamilyB.getOid()),
                IsIterableContainingInOrder.contains(computeTierSmallFamilyB.getOid()));

    }

    @Test
    public void testInvalidOid() {

        final ComputeTierFamilyResolver computeTierFamilyResolver =
                computeTierFamilyResolverFactory.createResolver(cloudTopology);

        assertTrue(computeTierFamilyResolver.getSiblingsInFamily(3890832L).isEmpty());
    }

    @Test(expected = ComputeTierNotFoundException.class)
    public void testCompareTierNotFound() throws IncompatibleTiersException, ComputeTierNotFoundException {

        final ComputeTierFamilyResolver computeTierFamilyResolver =
                computeTierFamilyResolverFactory.createResolver(cloudTopology);

        computeTierFamilyResolver.compareTiers(12321312L, 5);
    }

    @Test(expected = IncompatibleTiersException.class)
    public void testCompareIncompatibleTiers() throws IncompatibleTiersException, ComputeTierNotFoundException {
        final ComputeTierFamilyResolver computeTierFamilyResolver =
                computeTierFamilyResolverFactory.createResolver(cloudTopology);

        computeTierFamilyResolver.compareTiers(
                computeTierSmallFamilyA.getOid(),
                computeTierSmallFamilyB.getOid());
    }

    @Test
    public void testCompare() throws IncompatibleTiersException, ComputeTierNotFoundException {

        final ComputeTierFamilyResolver computeTierFamilyResolver =
                computeTierFamilyResolverFactory.createResolver(cloudTopology);

        final long actualComparison = computeTierFamilyResolver.compareTiers(
                computeTierSmallFamilyA.getOid(),
                computeTierLargeBFamilyA.getOid());

        assertThat(actualComparison, lessThan(0L));

    }

    @Test
    public void testCoverageFamily() {

        final ComputeTierFamilyResolver computeTierFamilyResolver =
                computeTierFamilyResolverFactory.createResolver(cloudTopology);

        assertThat(computeTierFamilyResolver.getCoverageFamily(computeTierSmallFamilyA.getOid()),
                equalTo(Optional.of("a")));
    }

    /**
     * Verify that if the compute tier does not have a coupon value set, it is not assigned to
     * a coverage family
     */
    @Test
    public void testNoCouponCoverageFamily() {

        final ComputeTierFamilyResolver computeTierFamilyResolver =
                computeTierFamilyResolverFactory.createResolver(cloudTopology);

        assertThat(computeTierFamilyResolver.getCoverageFamily(computeTierFamilyBZeroCoupons.getOid()),
                equalTo(Optional.empty()));
    }

    @Test
    public void testNumCoupons() {
        final ComputeTierFamilyResolver computeTierFamilyResolver =
                computeTierFamilyResolverFactory.createResolver(cloudTopology);

        assertThat(computeTierFamilyResolver.getNumCoupons(computeTierLargeFamilyA.getOid()),
                equalTo(Optional.of(10L)));
    }
}
