package com.vmturbo.topology.event.library.uptime;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.common.data.BoundedDuration;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.topology.event.library.TopologyEventProvider;
import com.vmturbo.topology.event.library.TopologyEvents;
import com.vmturbo.topology.event.library.TopologyEvents.TopologyEventLedger;
import com.vmturbo.topology.event.library.uptime.EntityUptimeCalculator.EntityUptimeCalculation;

public class EntityUptimeManagerTest {

    private TopologyEventProvider topologyEventProvider = mock(TopologyEventProvider.class);

    private EntityUptimeCalculator entityUptimeCalculator = mock(EntityUptimeCalculator.class);

    private EntityUptimeStore entityUptimeStore = mock(EntityUptimeStore.class);

    private final Duration windowDuration = Duration.ofHours(1);

    private final BoundedDuration calculationInterval = BoundedDuration.builder()
            .unit(ChronoUnit.HOURS)
            .amount(0)
            .build();

    @Test
    public void testFullUptimeCalculation() {

        // setup the current stored time window
        when(entityUptimeStore.getUptimeWindow()).thenReturn(TimeInterval.EPOCH);

        // setup the topology event provider
        final TopologyEventLedger firstEventLedger = TopologyEventLedger.builder()
                .entityOid(1)
                .build();
        final TopologyEventLedger secondEventLedger = TopologyEventLedger.builder()
                .entityOid(2)
                .build();
        final TopologyEvents topologyEvents = TopologyEvents.fromEventLedgers(firstEventLedger, secondEventLedger);
        when(topologyEventProvider.getTopologyEvents(any(), any())).thenReturn(topologyEvents);

        // setup the entity uptime calculator
        final EntityUptimeCalculation entityUptimeCalculationA = EntityUptimeCalculation.builder()
                .entityOid(1)
                .entityUptime(EntityUptime.UNKNOWN_DEFAULT_TO_ALWAYS_ON)
                .cachedUptime(CachedEntityUptime.EMPTY_UPTIME)
                .build();
        final EntityUptimeCalculation entityUptimeCalculationB = EntityUptimeCalculation.builder()
                .entityOid(2)
                .entityUptime(EntityUptime.UNKNOWN_DEFAULT_TO_ALWAYS_ON)
                .cachedUptime(CachedEntityUptime.EMPTY_UPTIME)
                .build();
        // not intended to mirror the passed in event ledger
        when(entityUptimeCalculator.calculateUptime(any(), any()))
                .thenReturn(entityUptimeCalculationA, entityUptimeCalculationB);

        // invoke the SUT
        final EntityUptimeManager entityUptimeManager = createManager(false);
        // invoke the calculation
        entityUptimeManager.onTopologyEventUpdate();

        // verify entity store persistence
        final ArgumentCaptor<TimeInterval> uptimeWindowCaptor = ArgumentCaptor.forClass(TimeInterval.class);
        final ArgumentCaptor<Map> uptimeMapCaptor = ArgumentCaptor.forClass(Map.class);
        verify(entityUptimeStore).persistTopologyUptime(uptimeWindowCaptor.capture(), uptimeMapCaptor.capture());

        final Map<Long, EntityUptime> actualUptimeMap = uptimeMapCaptor.getValue();
        assertThat(actualUptimeMap.entrySet(), hasSize(2));

        final Map<Long, EntityUptime> expectedUptimeMap = ImmutableMap.of(
                1L, EntityUptime.UNKNOWN_DEFAULT_TO_ALWAYS_ON,
                2L, EntityUptime.UNKNOWN_DEFAULT_TO_ALWAYS_ON);
        assertThat(actualUptimeMap, equalTo(expectedUptimeMap));
    }

    private EntityUptimeManager createManager(boolean cacheUptime) {
        return new EntityUptimeManager(
                topologyEventProvider,
                entityUptimeCalculator,
                entityUptimeStore,
                windowDuration,
                calculationInterval,
                cacheUptime,
                true);
    }
}
