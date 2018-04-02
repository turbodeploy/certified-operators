package com.vmturbo.stitching.journal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.EntityFilter;
import com.vmturbo.common.protobuf.topology.Stitching.EntityTypeFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.journal.JournalFilter.IncludeAllFilter;
import com.vmturbo.stitching.journal.JournalFilter.FilterByEntity;
import com.vmturbo.stitching.journal.JournalFilter.FilterByEntityType;

public class JournalFilterTest {
    final JournalableOperation operation = mock(JournalableOperation.class);
    final JournalableEntity<?> fooEntity = mock(JournalableEntity.class);
    final JournalableEntity<?> barEntity = mock(JournalableEntity.class);

    @Before
    public void setup() {
        when(fooEntity.getOid()).thenReturn(1234L);
        when(fooEntity.getDisplayName()).thenReturn("foo");
        when(fooEntity.getJournalableEntityType()).thenReturn(EntityType.VIRTUAL_MACHINE);

        when(barEntity.getOid()).thenReturn(5678L);
        when(barEntity.getDisplayName()).thenReturn("bar");
        when(barEntity.getJournalableEntityType()).thenReturn(EntityType.PHYSICAL_MACHINE);
    }

    @Test
    public void testAllowAllFilter() {
        final JournalFilter filter = new IncludeAllFilter();

        assertTrue(filter.shouldEnter(operation));
        assertTrue(filter.shouldEnter(fooEntity));
    }

    @Test
    public void testFilterByEntityOid() {
        final JournalFilter filter = new FilterByEntity(EntityFilter.newBuilder()
            .addOids(1234L)
            .build());

        assertTrue(filter.shouldEnter(operation));
        assertTrue(filter.shouldEnter(fooEntity));
        assertFalse(filter.shouldEnter(barEntity));
    }

    @Test
    public void testFilterByEntityName() {
        final JournalFilter filter = new FilterByEntity(EntityFilter.newBuilder()
            .addDisplayNames("foo")
            .build());

        assertTrue(filter.shouldEnter(operation));
        assertTrue(filter.shouldEnter(fooEntity));
        assertFalse(filter.shouldEnter(barEntity));
    }

    @Test
    public void testFilterByEntityOidOrName() {
        final JournalFilter filter = new FilterByEntity(EntityFilter.newBuilder()
            .addDisplayNames("foo")
            .addOids(5678L)
            .build());

        assertTrue(filter.shouldEnter(operation));
        assertTrue(filter.shouldEnter(fooEntity));
        assertTrue(filter.shouldEnter(barEntity));
    }

    @Test
    public void testFilterByEntityType() {
        final JournalFilter filter = new FilterByEntityType(EntityTypeFilter.newBuilder()
            .addEntityTypeNames(EntityType.VIRTUAL_MACHINE.name())
            .build());

        assertTrue(filter.shouldEnter(operation));
        assertTrue(filter.shouldEnter(fooEntity));
        assertFalse(filter.shouldEnter(barEntity));
    }

    @Test
    public void testFilterByTargetId() {

    }

    @Test
    public void testFilterByOperation() {

    }
}