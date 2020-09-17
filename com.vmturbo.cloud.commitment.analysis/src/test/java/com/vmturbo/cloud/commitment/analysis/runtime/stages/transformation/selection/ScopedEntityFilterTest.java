package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ScopedEntityFilter.ScopedEntityFilterFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;

public class ScopedEntityFilterTest {

    private final ScopedEntityFilterFactory scopedEntityFilterFactory = new ScopedEntityFilterFactory();

    private final ClassifiedEntityDemandAggregate entityAggregate = ClassifiedEntityDemandAggregate.builder()
            .entityOid(1)
            .accountOid(2)
            .regionOid(3)
            .serviceProviderOid(4)
            .build();

    @Test
    public void testPassThroughFilter() {

        final DemandScope demandScope = DemandScope.newBuilder().build();

        final ScopedEntityFilter scopedEntityFilter = scopedEntityFilterFactory.createFilter(demandScope);

        assertTrue(scopedEntityFilter.filter(entityAggregate));
    }

    @Test
    public void testPassesEntityFilter() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .addEntityOid(entityAggregate.entityOid())
                .build();

        final ScopedEntityFilter scopedEntityFilter = scopedEntityFilterFactory.createFilter(demandScope);

        assertTrue(scopedEntityFilter.filter(entityAggregate));
    }

    @Test
    public void testRejectedEntityFilter() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .addEntityOid(entityAggregate.entityOid() + 1)
                .build();

        final ScopedEntityFilter scopedEntityFilter = scopedEntityFilterFactory.createFilter(demandScope);

        assertFalse(scopedEntityFilter.filter(entityAggregate));
    }

    @Test
    public void testPassesAccountFilter() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .addAccountOid(entityAggregate.accountOid())
                .build();

        final ScopedEntityFilter scopedEntityFilter = scopedEntityFilterFactory.createFilter(demandScope);

        assertTrue(scopedEntityFilter.filter(entityAggregate));
    }

    @Test
    public void testRejectedAccountFilter() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .addAccountOid(entityAggregate.accountOid() + 1)
                .build();

        final ScopedEntityFilter scopedEntityFilter = scopedEntityFilterFactory.createFilter(demandScope);

        assertFalse(scopedEntityFilter.filter(entityAggregate));
    }

    @Test
    public void testPassesRegionFilter() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .addRegionOid(entityAggregate.regionOid())
                .build();

        final ScopedEntityFilter scopedEntityFilter = scopedEntityFilterFactory.createFilter(demandScope);

        assertTrue(scopedEntityFilter.filter(entityAggregate));
    }

    @Test
    public void testRejectedRegionFilter() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .addRegionOid(entityAggregate.regionOid() + 1)
                .build();

        final ScopedEntityFilter scopedEntityFilter = scopedEntityFilterFactory.createFilter(demandScope);

        assertFalse(scopedEntityFilter.filter(entityAggregate));
    }

    @Test
    public void testPassesServiceProviderFilter() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .addServiceProviderOid(entityAggregate.serviceProviderOid())
                .build();

        final ScopedEntityFilter scopedEntityFilter = scopedEntityFilterFactory.createFilter(demandScope);

        assertTrue(scopedEntityFilter.filter(entityAggregate));
    }

    @Test
    public void testRejectedServiceProviderFilter() {

        final DemandScope demandScope = DemandScope.newBuilder()
                .addServiceProviderOid(entityAggregate.serviceProviderOid() + 1)
                .build();

        final ScopedEntityFilter scopedEntityFilter = scopedEntityFilterFactory.createFilter(demandScope);

        assertFalse(scopedEntityFilter.filter(entityAggregate));
    }
}
