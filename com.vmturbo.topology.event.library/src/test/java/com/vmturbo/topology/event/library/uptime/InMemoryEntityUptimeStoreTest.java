package com.vmturbo.topology.event.library.uptime;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.entity.scope.CloudScopeStore;
import com.vmturbo.cloud.common.entity.scope.EntityCloudScope;
import com.vmturbo.common.protobuf.cost.EntityUptime.CloudScopeFilter;

public class InMemoryEntityUptimeStoreTest {

    private CloudScopeStore cloudScopeStore = mock(CloudScopeStore.class);

    private InMemoryEntityUptimeStore entityUptimeStore;

    @Before
    public void setup() {
        entityUptimeStore = new InMemoryEntityUptimeStore(cloudScopeStore);
    }

    @Test
    public void testPersistence() {

        final TimeInterval uptimeWindow = TimeInterval.builder()
                .startTime(Instant.now().minus(2, ChronoUnit.HOURS))
                .endTime(Instant.now())
                .build();

        final EntityUptime entityUptimeA = EntityUptime.builder()
                .uptime(Duration.ofSeconds(123))
                .totalTime(Duration.ofSeconds(123))
                .uptimePercentage(100.0)
                .build();
        final EntityUptime entityUptimeB = EntityUptime.builder()
                .uptime(Duration.ofSeconds(100))
                .totalTime(Duration.ofSeconds(200))
                .uptimePercentage(50.0)
                .build();
        final Map<Long, EntityUptime> entityUptimeMap = ImmutableMap.of(
                1L, entityUptimeA,
                2L, entityUptimeB);

        // invoke SUT
        entityUptimeStore.persistTopologyUptime(uptimeWindow, entityUptimeMap);

        // ASSERTS
        assertThat(entityUptimeStore.getUptimeWindow(), equalTo(uptimeWindow));
        assertThat(entityUptimeStore.getEntityUptime(1), equalTo(entityUptimeA));
        assertThat(entityUptimeStore.getEntityUptime(2), equalTo(entityUptimeB));
        assertThat(entityUptimeStore.getEntityUptime(3), equalTo(entityUptimeStore.getDefaultUptime()));
    }

    @Test
    public void testGetUptimeByFilter() {

        final TimeInterval uptimeWindow = TimeInterval.builder()
                .startTime(Instant.now().minus(2, ChronoUnit.HOURS))
                .endTime(Instant.now())
                .build();

        final EntityUptime entityUptimeA = EntityUptime.builder()
                .uptime(Duration.ofSeconds(123))
                .totalTime(Duration.ofSeconds(123))
                .uptimePercentage(100.0)
                .build();
        final EntityUptime entityUptimeB = EntityUptime.builder()
                .uptime(Duration.ofSeconds(100))
                .totalTime(Duration.ofSeconds(200))
                .uptimePercentage(50.0)
                .build();
        final Map<Long, EntityUptime> entityUptimeMap = ImmutableMap.of(
                1L, entityUptimeA,
                2L, entityUptimeB);

        // setup the store
        entityUptimeStore.persistTopologyUptime(uptimeWindow, entityUptimeMap);

        // setup the cloud scope store
        final EntityCloudScope entityCloudScopeA = EntityCloudScope.builder()
                .entityOid(1)
                .accountOid(2)
                .regionOid(3)
                .serviceProviderOid(4)
                .creationTime(Instant.now())
                .build();
        final EntityCloudScope entityCloudScopeC = EntityCloudScope.builder()
                .entityOid(3)
                .accountOid(2)
                .regionOid(3)
                .serviceProviderOid(4)
                .creationTime(Instant.now())
                .build();
        when(cloudScopeStore.streamByFilter(any()))
                .thenReturn(Stream.of(entityCloudScopeA, entityCloudScopeC));

        // invoke SUT
        final Map<Long, EntityUptime> actualUptimeMap =
                entityUptimeStore.getUptimeByFilter(CloudScopeFilter.getDefaultInstance());

        // ASSERTS
        final Map<Long, EntityUptime> expectedUptimeMap = ImmutableMap.of(
                1L, entityUptimeA,
                3L, entityUptimeStore.getDefaultUptime());
        assertThat(actualUptimeMap, equalTo(expectedUptimeMap));

    }
}
