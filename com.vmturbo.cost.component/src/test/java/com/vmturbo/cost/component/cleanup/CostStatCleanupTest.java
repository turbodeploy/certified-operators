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
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.TableInfo;

/**
 * This class tests all of the cost stat cleanup tables (e.g. CostStatDayTable, CostStatHourTable, etc)
 */
public class CostStatCleanupTest {

    private final DSLContext dslContext = mock(DSLContext.class);

    private final long daysSinceEpoch = 52L;
    private final Instant invocationTime = Instant.ofEpochMilli(Duration.ofDays(daysSinceEpoch).toMillis());
    private final Clock clock = new MutableFixedClock(invocationTime.toEpochMilli());

    private final TableInfo tableInfo = ImmutableTableInfo.builder()
            .table(ENTITY_COST)
            .timeField(ENTITY_COST.CREATED_TIME)
            .shortTableName("entity_cost")
            .build();

    private final RetentionPeriodFetcher retentionPeriodFetcher = mock(RetentionPeriodFetcher.class);


    private final int latestRetentionMinutes = 10;
    private final int hourlyRetentionHours = 15;
    private final int dailyRetentionDays = 20;
    private final int monthlyRetentionMonths = 1;
    private final RetentionPeriods retentionPeriods = new RetentionPeriods() {

        @Override
        public int latestRetentionMinutes() {
            return latestRetentionMinutes;
        }

        @Override
        public int hourlyRetentionHours() {
            return hourlyRetentionHours;
        }

        @Override
        public int dailyRetentionDays() {
            return dailyRetentionDays;
        }

        @Override
        public int monthlyRetentionMonths() {
            return monthlyRetentionMonths;
        }
    };

    @Before
    public void setup() {
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
    }

    @Test
    public void testDayTableTrimTime() {

        final CostStatDayTable costStatDayTable = new CostStatDayTable(
                dslContext,
                clock,
                tableInfo,
                retentionPeriodFetcher);

        final LocalDateTime actualDateTime = costStatDayTable.getTrimTime();

        final LocalDateTime expectedDateTime = LocalDateTime.ofInstant(
                Instant.EPOCH.plus(daysSinceEpoch, ChronoUnit.DAYS)
                        .minus(dailyRetentionDays, ChronoUnit.DAYS),
                ZoneOffset.UTC);
        assertThat(actualDateTime, equalTo(expectedDateTime));

    }

    @Test
    public void testHourTableTrimTime() {

        final CostStatHourTable costStatHourTable = new CostStatHourTable(
                dslContext,
                clock,
                tableInfo,
                retentionPeriodFetcher);

        final LocalDateTime actualDateTime = costStatHourTable.getTrimTime();

        final LocalDateTime expectedDateTime = LocalDateTime.ofInstant(
                Instant.EPOCH.plus(daysSinceEpoch, ChronoUnit.DAYS)
                        .minus(hourlyRetentionHours, ChronoUnit.HOURS),
                ZoneOffset.UTC);
        assertThat(actualDateTime, equalTo(expectedDateTime));

    }

    @Test
    public void testLatestTableTrimTime() {

        final CostStatLatestTable costStatLatestTable = new CostStatLatestTable(
                dslContext,
                clock,
                tableInfo,
                retentionPeriodFetcher);

        final LocalDateTime actualDateTime = costStatLatestTable.getTrimTime();

        final LocalDateTime expectedDateTime = LocalDateTime.ofInstant(
                Instant.EPOCH.plus(daysSinceEpoch, ChronoUnit.DAYS)
                        .minus(latestRetentionMinutes, ChronoUnit.MINUTES),
                ZoneOffset.UTC);
        assertThat(actualDateTime, equalTo(expectedDateTime));

    }

    @Test
    public void testMonthlyTableTrimTime() {

        final CostStatMonthlyTable costStatMonthlyTable = new CostStatMonthlyTable(
                dslContext,
                clock,
                tableInfo,
                retentionPeriodFetcher);

        final LocalDateTime actualDateTime = costStatMonthlyTable.getTrimTime();

        final LocalDateTime expectedDateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC);
        assertThat(actualDateTime, equalTo(expectedDateTime));

    }
}
