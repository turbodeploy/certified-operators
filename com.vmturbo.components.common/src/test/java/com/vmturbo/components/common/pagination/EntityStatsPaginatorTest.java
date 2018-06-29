package com.vmturbo.components.common.pagination;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;

/**
 * Tests for the {@link EntityStatsPaginator}.
 */
public class EntityStatsPaginatorTest {

    private static final float E1_VAL = 1.0f;
    private static final float E2_VAL = 1.0f;
    private static final String COMMODITY = "love";

    private static final EntityStats E1 = EntityStats.newBuilder()
            .setOid(1L)
            .addStatSnapshots(StatSnapshot.newBuilder()
                    .addStatRecords(StatRecord.newBuilder()
                            .setName(COMMODITY)
                            .setValues(StatValue.newBuilder()
                                    .setAvg(E1_VAL))))
            .build();

    private static final EntityStats E2 = EntityStats.newBuilder()
            .setOid(2L)
            .addStatSnapshots(StatSnapshot.newBuilder()
                    .addStatRecords(StatRecord.newBuilder()
                            .setName(COMMODITY)
                            .setValues(StatValue.newBuilder()
                                    .setAvg(E2_VAL))))
            .build();

    private final EntityStatsPaginator paginator = new EntityStatsPaginator();

    private EntityStatsPaginationParams params = mock(EntityStatsPaginationParams.class);

    @Before
    public void setup() {
        when(params.getSortCommodity()).thenReturn(COMMODITY);
        when(params.getNextCursor()).thenReturn(Optional.empty());
        when(params.getLimit()).thenReturn(10);
        when(params.isAscending()).thenReturn(false);
    }

    @Test
    public void testPaginateAscending() {
        when(params.isAscending()).thenReturn(true);
        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E2.toBuilder(), E1.toBuilder()), params);
        assertThat(response.getStatsPage(), contains(E1, E2));
    }

    @Test
    public void testPaginateDescending() {
        when(params.isAscending()).thenReturn(false);
        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E1.toBuilder(), E2.toBuilder()), params);
        assertThat(response.getStatsPage(), contains(E2, E1));
    }

    @Test
    public void testPaginateLimitAndNextCursorSet() {
        when(params.getLimit()).thenReturn(1);
        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E1.toBuilder(), E2.toBuilder()), params);
        assertThat(response.getStatsPage(), contains(E2));
        assertThat(response.getPaginationResponse().getNextCursor(), is("1"));
    }

    @Test
    public void testPaginateNextCursorInput() {
        when(params.getNextCursor()).thenReturn(Optional.of("1"));
        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E1.toBuilder(), E2.toBuilder()), params);
        assertThat(response.getStatsPage(), contains(E1));
    }

    @Test
    public void testPaginateEndOfResults() {
        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(E1.toBuilder(), E2.toBuilder()), params);
        assertThat(response.getStatsPage(), contains(E2, E1));
        assertThat(response.getPaginationResponse().getNextCursor(), is(""));
    }

    @Test
    public void testPaginateNoSnapshot() {
        final EntityStats.Builder e1Builder = E1.toBuilder().removeStatSnapshots(0);

        final PaginatedStats response =
            paginator.paginate(Lists.newArrayList(e1Builder, E2.toBuilder()), params);
        // Entity with no snapshot is considered smaller.
        assertThat(response.getStatsPage(), contains(E2, e1Builder.build()));
    }

    @Test
    public void testPaginateNoRecord() {
        final EntityStats.Builder e1Builder = E1.toBuilder();
        e1Builder.getStatSnapshotsBuilder(0).removeStatRecords(0);

        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(e1Builder, E2.toBuilder()), params);
        // Entity with no record is considered smaller.
        assertThat(response.getStatsPage(), contains(E2, e1Builder.build()));
    }

    @Test
    public void testPaginateStableSort() {
        final EntityStats.Builder e1Builder = E1.toBuilder().removeStatSnapshots(0);
        final EntityStats.Builder e2Builder = E2.toBuilder().removeStatSnapshots(0);

        final PaginatedStats response =
                paginator.paginate(Lists.newArrayList(e1Builder, e2Builder), params);
        // Entity with smaller ID is considered smaller.
        assertThat(response.getStatsPage(), contains(e2Builder.build(), e1Builder.build()));
    }
}
