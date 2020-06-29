package com.vmturbo.cloud.commitment.analysis.topology;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.vmturbo.cloud.commitment.analysis.topology.MinimalCloudTopology.MinimalCloudTopologyFactory;
import com.vmturbo.cloud.commitment.analysis.topology.MinimalEntityCloudTopology.DefaultMinimalEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;

public class MinimalEntityCloudTopologyTest {

    private final BillingFamilyRetriever billingFamilyRetriever = mock(BillingFamilyRetriever.class);
    private final BillingFamilyRetrieverFactory billingFamilyRetrieverFactory =
            mock(BillingFamilyRetrieverFactory.class);

    private final MinimalCloudTopologyFactory<MinimalEntity> cloudTopologyFactory =
            new DefaultMinimalEntityCloudTopologyFactory(billingFamilyRetrieverFactory);

    @Before
    public void setup() {
        when(billingFamilyRetrieverFactory.newInstance()).thenReturn(billingFamilyRetriever);
    }

    @Test
    public void testGetEntities() {
        // setup input
        final MinimalEntity minimalEntityA = MinimalEntity.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final MinimalEntity minimalEntityB = MinimalEntity.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final MinimalEntity minimalEntityC = MinimalEntity.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.ON_PREM)
                .build();

        final Stream<MinimalEntity> minimalEntityStream = Stream.of(minimalEntityA, minimalEntityB, minimalEntityC);

        // setup cloud topology
        final MinimalCloudTopology<MinimalEntity> cloudTopology = cloudTopologyFactory.createCloudTopology(minimalEntityStream);

        // assertions
        final Map<Long, MinimalEntity> expectedEntitiesMap = ImmutableMap.of(
                minimalEntityA.getOid(), minimalEntityA,
                minimalEntityB.getOid(), minimalEntityB);

        assertTrue(Maps.difference(cloudTopology.getEntities(), expectedEntitiesMap).areEqual());
    }

    @Test
    public void testGetEntity() {
        // setup input
        final MinimalEntity minimalEntityA = MinimalEntity.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final MinimalEntity minimalEntityB = MinimalEntity.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final Stream<MinimalEntity> minimalEntityStream = Stream.of(minimalEntityA, minimalEntityB);

        // setup cloud topology
        final MinimalCloudTopology<MinimalEntity> cloudTopology = cloudTopologyFactory.createCloudTopology(minimalEntityStream);

        // assertions
        assertThat(cloudTopology.getEntity(minimalEntityA.getOid()), equalTo(Optional.of(minimalEntityA)));
        assertThat(cloudTopology.getEntity(minimalEntityB.getOid()), equalTo(Optional.of(minimalEntityB)));
    }

    @Test
    public void testEntityExists() {
        // setup input
        final MinimalEntity minimalEntityA = MinimalEntity.newBuilder()
                .setOid(1L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final MinimalEntity minimalEntityB = MinimalEntity.newBuilder()
                .setOid(2L)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();

        final Stream<MinimalEntity> minimalEntityStream = Stream.of(minimalEntityA, minimalEntityB);

        // setup cloud topology
        final MinimalCloudTopology<MinimalEntity> cloudTopology = cloudTopologyFactory.createCloudTopology(minimalEntityStream);

        // assertions
        assertTrue(cloudTopology.entityExists(minimalEntityA.getOid()));
        assertTrue(cloudTopology.entityExists(minimalEntityA.getOid()));
        assertFalse(cloudTopology.entityExists(3L));
    }
}
