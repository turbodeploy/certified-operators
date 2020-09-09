package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.CloudTierFilter.CloudTierFilterFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope.ComputeTierDemandScope;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class CloudTierFilterTest {

    private final CloudTierFilterFactory cloudTierFilterFactory = new CloudTierFilterFactory();

    private final ComputeTierDemand computeTierDemand = ComputeTierDemand.builder()
            .cloudTierOid(1)
            .osType(OSType.RHEL)
            .tenancy(Tenancy.DEFAULT)
            .build();

    @Test
    public void testEmptyCloudTiers() {

        final DemandScope demandScope = DemandScope.newBuilder().build();

        final CloudTierFilter cloudTierFilter = cloudTierFilterFactory.newFilter(demandScope);

        assertTrue(cloudTierFilter.filter(computeTierDemand));
    }

    @Test
    public void testPassingCloudTierOid() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .addCloudTierOid(computeTierDemand.cloudTierOid())
                .build();

        final CloudTierFilter cloudTierFilter = cloudTierFilterFactory.newFilter(demandScope);

        assertTrue(cloudTierFilter.filter(computeTierDemand));
    }

    @Test
    public void testFilteredCloudTierOid() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .addCloudTierOid(computeTierDemand.cloudTierOid() + 1)
                .build();

        final CloudTierFilter cloudTierFilter = cloudTierFilterFactory.newFilter(demandScope);

        assertFalse(cloudTierFilter.filter(computeTierDemand));
    }

    @Test
    public void testPassesPlatform() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .setComputeTierScope(ComputeTierDemandScope.newBuilder()
                        .addPlatform(computeTierDemand.osType()))
                .build();

        final CloudTierFilter cloudTierFilter = cloudTierFilterFactory.newFilter(demandScope);

        assertTrue(cloudTierFilter.filter(computeTierDemand));
    }

    @Test
    public void testFilteredPlatform() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .setComputeTierScope(ComputeTierDemandScope.newBuilder()
                        .addPlatform(OSType.UNKNOWN_OS))
                .build();

        final CloudTierFilter cloudTierFilter = cloudTierFilterFactory.newFilter(demandScope);

        assertFalse(cloudTierFilter.filter(computeTierDemand));
    }

    @Test
    public void testPassesTenancy() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .setComputeTierScope(ComputeTierDemandScope.newBuilder()
                        .addTenancy(computeTierDemand.tenancy()))
                .build();

        final CloudTierFilter cloudTierFilter = cloudTierFilterFactory.newFilter(demandScope);

        assertTrue(cloudTierFilter.filter(computeTierDemand));
    }

    @Test
    public void testFilteredTenancy() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .setComputeTierScope(ComputeTierDemandScope.newBuilder()
                        .addTenancy(Tenancy.HOST))
                .build();

        final CloudTierFilter cloudTierFilter = cloudTierFilterFactory.newFilter(demandScope);

        assertFalse(cloudTierFilter.filter(computeTierDemand));
    }
}
