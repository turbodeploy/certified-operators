package com.vmturbo.cost.component.cleanup;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.TableInfo;
import com.vmturbo.cost.component.cleanup.RetentionDurationFetcher.BoundedDuration;

public class CustomRetentionCleanupTest {

    private final TableInfo tableInfo = ImmutableTableInfo.builder()
            .table(ENTITY_COST)
            .timeField(ENTITY_COST.CREATED_TIME)
            .shortTableName("entity_cost")
            .build();

    private final DSLContext dslContext = mock(DSLContext.class);

    private final long daysSinceEpoch = 52L;
    private final Instant invocationTime = Instant.ofEpochMilli(Duration.ofDays(daysSinceEpoch).toMillis());
    private final Clock clock = new MutableFixedClock(invocationTime.toEpochMilli());

    private final RetentionDurationFetcher retentionDurationFetcher = mock(RetentionDurationFetcher.class);

    private CustomRetentionCleanup customRetentionCleanup;

    @Before
    public void setup() {
        customRetentionCleanup = new CustomRetentionCleanup(
                dslContext,
                clock,
                tableInfo,
                retentionDurationFetcher);
    }

    @Test
    public void testTableInfo() {

        assertThat(customRetentionCleanup.tableInfo(), equalTo(tableInfo));
    }

    @Test
    public void testTrimTimeMonthly() {

        final BoundedDuration retentionDuration = ImmutableBoundedDuration.builder()
                .unit(ChronoUnit.MONTHS)
                .amount(1)
                .build();
        when(retentionDurationFetcher.getRetentionDuration()).thenReturn(retentionDuration);

        final LocalDateTime actualTrimTime = customRetentionCleanup.getTrimTime();

        final LocalDateTime expectedTrimTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
        assertThat(actualTrimTime, equalTo(expectedTrimTime));
    }

    @Test
    public void testTrimTimeDaily() {

        final BoundedDuration retentionDuration = ImmutableBoundedDuration.builder()
                .unit(ChronoUnit.DAYS)
                .amount(15)
                .build();
        when(retentionDurationFetcher.getRetentionDuration()).thenReturn(retentionDuration);

        final LocalDateTime actualTrimTime = customRetentionCleanup.getTrimTime();

        final LocalDateTime expectedTrimTime = LocalDateTime.ofInstant(
                Instant.EPOCH.plus(daysSinceEpoch - 15, ChronoUnit.DAYS),
                ZoneOffset.UTC);
        assertThat(actualTrimTime, equalTo(expectedTrimTime));
    }

    @Test
    public void testTrimTimeHourly() {

        final BoundedDuration retentionDuration = ImmutableBoundedDuration.builder()
                .unit(ChronoUnit.HOURS)
                .amount(15)
                .build();
        when(retentionDurationFetcher.getRetentionDuration()).thenReturn(retentionDuration);

        final LocalDateTime actualTrimTime = customRetentionCleanup.getTrimTime();

        final LocalDateTime expectedTrimTime = LocalDateTime.ofInstant(
                Instant.EPOCH.plus(daysSinceEpoch, ChronoUnit.DAYS)
                        .minus(15, ChronoUnit.HOURS),
                ZoneOffset.UTC);
        assertThat(actualTrimTime, equalTo(expectedTrimTime));
    }

    @Test
    public void testTrimTimeMinutes() {

        final BoundedDuration retentionDuration = ImmutableBoundedDuration.builder()
                .unit(ChronoUnit.MINUTES)
                .amount(20)
                .build();
        when(retentionDurationFetcher.getRetentionDuration()).thenReturn(retentionDuration);

        final LocalDateTime actualTrimTime = customRetentionCleanup.getTrimTime();

        final LocalDateTime expectedTrimTime = LocalDateTime.ofInstant(
                Instant.EPOCH.plus(daysSinceEpoch, ChronoUnit.DAYS)
                        .minus(20, ChronoUnit.MINUTES),
                ZoneOffset.UTC);
        assertThat(actualTrimTime, equalTo(expectedTrimTime));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedTrimTime() {

        final BoundedDuration retentionDuration = ImmutableBoundedDuration.builder()
                .unit(ChronoUnit.DECADES)
                .amount(20)
                .build();
        when(retentionDurationFetcher.getRetentionDuration()).thenReturn(retentionDuration);

        final LocalDateTime actualTrimTime = customRetentionCleanup.getTrimTime();
    }
}
