package com.vmturbo.topology.processor.stitching.journal;

import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.Stitching.EntityFilter;
import com.vmturbo.common.protobuf.topology.Stitching.EntityTypeFilter;
import com.vmturbo.common.protobuf.topology.Stitching.FilteredJournalRequest;
import com.vmturbo.common.protobuf.topology.Stitching.IncludeAllFilter;
import com.vmturbo.common.protobuf.topology.Stitching.OperationFilter;
import com.vmturbo.common.protobuf.topology.Stitching.ProbeTypeFilter;
import com.vmturbo.common.protobuf.topology.Stitching.ProbeTypeFilter.NamesList;
import com.vmturbo.common.protobuf.topology.Stitching.TargetFilter;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.journal.JournalFilter;
import com.vmturbo.stitching.journal.JournalFilter.FilterByEntity;
import com.vmturbo.stitching.journal.JournalFilter.FilterByEntityType;
import com.vmturbo.stitching.journal.JournalFilter.FilterByOperation;
import com.vmturbo.stitching.journal.JournalFilter.FilterByTargetId;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

public class JournalFilterFactoryTest {
    final ProbeStore probeStore = mock(ProbeStore.class);
    final TargetStore targetStore = mock(TargetStore.class);
    final JournalableEntity<?> entityA = mock(JournalableEntity.class);
    final JournalableEntity<?> entityB = mock(JournalableEntity.class);
    final Target target = mock(Target.class);

    final JournalFilterFactory factory = new JournalFilterFactory(probeStore, targetStore);

    @Before
    public void setup() {
        when(entityA.getDiscoveringTargetIds()).thenReturn(Stream.of(1234L));
        when(entityB.getDiscoveringTargetIds()).thenReturn(Stream.of(5678L));
        when(target.getId()).thenReturn(1234L);
    }

    @Test
    public void probeNameFilter() {
        when(targetStore.getProbeTargets(9999L))
            .thenReturn(Collections.singletonList(target));
        when(probeStore.getProbeIdForType(eq("foo")))
            .thenReturn(Optional.of(9999L));
        when(probeStore.getProbeIdForType(eq("bar")))
            .thenReturn(Optional.empty());

        final JournalFilter filter = factory.filterFor(ProbeTypeFilter.newBuilder()
            .setProbeNames(NamesList.newBuilder()
                .addAllNames(Arrays.asList("foo", "bar")))
            .build());

        assertTrue(filter.shouldEnter(entityA));
        assertFalse(filter.shouldEnter(entityB));
    }

    @Test
    public void testProbeCategoryNameFilterLowercase() {
        when(targetStore.getProbeTargets(9999L))
            .thenReturn(Collections.singletonList(target));
        when(probeStore.getProbeIdsForCategory(eq(ProbeCategory.HYPERVISOR)))
            .thenReturn(Collections.singletonList(9999L));

        final JournalFilter filter = factory.filterFor(ProbeTypeFilter.newBuilder()
            .setProbeCategoryNames(NamesList.newBuilder()
                .addNames("hypervisor"))
            .build());

        assertTrue(filter.shouldEnter(entityA));
        assertFalse(filter.shouldEnter(entityB));
    }

    @Test
    public void testProbeCategoryNameFilterUppercase() {
        when(targetStore.getProbeTargets(9999L))
            .thenReturn(Collections.singletonList(target));
        when(probeStore.getProbeIdsForCategory(eq(ProbeCategory.HYPERVISOR)))
            .thenReturn(Collections.singletonList(9999L));

        final JournalFilter filter = factory.filterFor(ProbeTypeFilter.newBuilder()
            .setProbeCategoryNames(NamesList.newBuilder()
                .addNames("HYPERVISOR"))
            .build());

        assertTrue(filter.shouldEnter(entityA));
        assertFalse(filter.shouldEnter(entityB));
    }

    @Test
    public void testFilterForJournalRequest() {
        final FilteredJournalRequest entityFilter = FilteredJournalRequest.newBuilder()
            .setEntityFilter(EntityFilter.getDefaultInstance())
            .build();
        final FilteredJournalRequest entityTypeFilter = FilteredJournalRequest.newBuilder()
            .setEntityTypeFilter(EntityTypeFilter.getDefaultInstance())
            .build();
        final FilteredJournalRequest probeTypeFilter = FilteredJournalRequest.newBuilder()
            .setProbeTypeFilter(ProbeTypeFilter.newBuilder().setProbeCategoryNames(NamesList.getDefaultInstance()))
            .build();
        final FilteredJournalRequest targetFilter = FilteredJournalRequest.newBuilder()
            .setTargetFilter(TargetFilter.getDefaultInstance())
            .build();
        final FilteredJournalRequest operationFilter = FilteredJournalRequest.newBuilder()
            .setOperationFilter(OperationFilter.getDefaultInstance())
            .build();
        final FilteredJournalRequest includeAllFilter = FilteredJournalRequest.newBuilder()
            .setIncludeAllFilter(IncludeAllFilter.getDefaultInstance())
            .build();

        final Map<FilteredJournalRequest, Class<?>> filterMap = ImmutableMap.<FilteredJournalRequest, Class<?>>builder()
            .put(entityFilter, FilterByEntity.class)
            .put(entityTypeFilter, FilterByEntityType.class)
            .put(probeTypeFilter, FilterByTargetId.class)
            .put(targetFilter, FilterByTargetId.class)
            .put(operationFilter, FilterByOperation.class)
            .put(includeAllFilter, JournalFilter.IncludeAllFilter.class)
            .build();

        filterMap.forEach((filter, klass) ->
            assertEquals(klass, factory.filterFor(filter).getClass()));
    }
}