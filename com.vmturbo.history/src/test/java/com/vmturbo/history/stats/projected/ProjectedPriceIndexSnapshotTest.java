package com.vmturbo.history.stats.projected;

import static com.vmturbo.history.stats.projected.ProjectedStatsTestConstants.COMMODITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Comparator;
import java.util.Optional;

import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;

public class ProjectedPriceIndexSnapshotTest {

    private static final long SMALLER_ENTITY_OID = 7L;

    private static final float SMALLER_ENTITY_PRICE_INDEX = 123.0f;

    private static final long BIGGER_ENTITY_OID = 77L;

    private static final float BIGGER_ENTITY_PRICE_INDEX = 223.0f;

    private ProjectedPriceIndexSnapshot snapshot;

    @Before
    public void setup() {
        Long2FloatMap map = new Long2FloatOpenHashMap();
        map.put(SMALLER_ENTITY_OID, SMALLER_ENTITY_PRICE_INDEX);
        map.put(BIGGER_ENTITY_OID, BIGGER_ENTITY_PRICE_INDEX);
        snapshot = ProjectedPriceIndexSnapshot.newFactory().createSnapshot(map);
    }

    @Test
    public void testDescendingCompator() {
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);
        when(params.isAscending()).thenReturn(false);

        final Comparator<Long> entityComparator = snapshot.getEntityComparator(params);
        final int result = entityComparator.compare(SMALLER_ENTITY_OID, BIGGER_ENTITY_OID);
        assertThat(result, is(1));
    }

    @Test
    public void testAscendingComparator() {
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);
        when(params.isAscending()).thenReturn(true);

        final Comparator<Long> entityComparator = snapshot.getEntityComparator(params);
        final int result = entityComparator.compare(SMALLER_ENTITY_OID, BIGGER_ENTITY_OID);
        assertThat(result, is(-1));
    }

    @Test
    public void testDescendingComparatorEqualValue() {
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);
        when(params.isAscending()).thenReturn(false);

        final Comparator<Long> entityComparator = snapshot.getEntityComparator(params);
        // Pick IDs that won't have a price index.
        final long smallerId = SMALLER_ENTITY_OID - 1;
        final long biggerId = BIGGER_ENTITY_OID + 1;
        // The stat values are the same, so the order should be determined by the id.
        final int result = entityComparator.compare(smallerId, biggerId);
        assertThat(result, is(1));
    }

    @Test
    public void testAscendingComparatorEqualValue() {
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(StringConstants.PRICE_INDEX);
        when(params.isAscending()).thenReturn(true);

        final Comparator<Long> entityComparator = snapshot.getEntityComparator(params);
        // Pick IDs that won't have a price index.
        final long smallerId = SMALLER_ENTITY_OID - 1;
        final long biggerId = BIGGER_ENTITY_OID + 1;
        // The stat values are the same, so the order should be determined by the id.
        final int result = entityComparator.compare(smallerId, biggerId);
        assertThat(result, is(-1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalCommodity() {
        final EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);
        when(params.getSortCommodity()).thenReturn(COMMODITY);

        snapshot.getEntityComparator(params);
    }

    @Test
    public void testGetRecordSingleEntity() {
        Optional<StatRecord> record = snapshot.getRecord(Collections.singleton(SMALLER_ENTITY_OID));
        assertThat(record.get().getName(), is(StringConstants.PRICE_INDEX));
        assertThat(record.get().getCurrentValue(), is((float)SMALLER_ENTITY_PRICE_INDEX));
    }

    @Test
    public void testGetRecordMultiEntities() {
        Optional<StatRecord> record = snapshot.getRecord(Sets.newHashSet(SMALLER_ENTITY_OID, BIGGER_ENTITY_OID));
        assertThat(record.get().getName(), is(StringConstants.PRICE_INDEX));
        assertThat(record.get().getCurrentValue(),
                // This works because we pick values that divide nicely :)
                is((float)(SMALLER_ENTITY_PRICE_INDEX + BIGGER_ENTITY_PRICE_INDEX) / 2));
    }

    @Test
    public void testGetRecordEntireTopology() {
        Optional<StatRecord> record = snapshot.getRecord(Collections.emptySet());
        assertThat(record.get().getName(), is(StringConstants.PRICE_INDEX));
        assertThat(record.get().getCurrentValue(),
                // This works because we pick values that divide nicely :)
                is((float)(SMALLER_ENTITY_PRICE_INDEX + BIGGER_ENTITY_PRICE_INDEX) / 2));
    }

    @Test
    public void testRecordMissingEntity() {
        Optional<StatRecord> record = snapshot.getRecord(Sets.newHashSet(12345L));
        assertFalse(record.isPresent());
    }
}
