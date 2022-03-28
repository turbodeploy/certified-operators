package com.vmturbo.topology.event.library.uptime;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.entity.scope.CloudScopeStore;
import com.vmturbo.cloud.common.entity.scope.EntityCloudScope;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudScopeFilter;

public class InMemoryEntityUptimeStoreTest {

    private CloudScopeStore cloudScopeStore = mock(CloudScopeStore.class);

    private InMemoryEntityUptimeStore entityUptimeStore;

    @Before
    public void setup() {
        entityUptimeStore = new InMemoryEntityUptimeStore(cloudScopeStore, EntityUptime.UNKNOWN_DEFAULT_TO_ALWAYS_ON);
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
        assertThat(entityUptimeStore.getEntityUptime(1), equalTo(Optional.of(entityUptimeA)));
        assertThat(entityUptimeStore.getEntityUptime(2), equalTo(Optional.of(entityUptimeB)));
        assertThat(entityUptimeStore.getEntityUptime(3), equalTo(Optional.empty()));
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
        doAnswer(inv -> {
            Consumer<EntityCloudScope> consumer = inv.getArgumentAt(1, Consumer.class);
            consumer.accept(entityCloudScopeA);
            consumer.accept(entityCloudScopeC);
            return null;
        }).when(cloudScopeStore).streamByFilter(any(), any());

        // invoke SUT
        final Map<Long, EntityUptime> actualUptimeMap =
                entityUptimeStore.getUptimeByFilter(CloudScopeFilter.getDefaultInstance());

        // ASSERTS
        final Map<Long, EntityUptime> expectedUptimeMap = ImmutableMap.of(
                1L, entityUptimeA);
        assertThat(actualUptimeMap, equalTo(expectedUptimeMap));

    }
}
