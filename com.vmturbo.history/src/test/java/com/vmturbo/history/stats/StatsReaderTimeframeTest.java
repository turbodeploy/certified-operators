package com.vmturbo.history.stats;

import static com.vmturbo.reports.db.abstraction.Tables.PM_STATS_BY_DAY;
import static com.vmturbo.reports.db.abstraction.Tables.PM_STATS_BY_MONTH;
import static com.vmturbo.reports.db.abstraction.tables.PmStatsByHour.PM_STATS_BY_HOUR;
import static com.vmturbo.reports.db.abstraction.tables.PmStatsLatest.PM_STATS_LATEST;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.reports.db.BasedbIO;

/**
 * Test methods select timeframe and table based on startTime, endTime.
 */
@RunWith(Parameterized.class)
public class StatsReaderTimeframeTest {

    /**
     * These are the time ranges queried by the UX, and the expected tables to supply the
     * stats values.
     *
     * @return delta startTime, delta endTime, and expectedTableToRead for each test
     */
    @Parameters(name="{index}: startTime {0}, endTime {1}, table {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {TimeUnit.MINUTES.toMillis(60), TimeUnit.MINUTES.toMillis(10), PM_STATS_LATEST},
                {TimeUnit.HOURS.toMillis(24), TimeUnit.HOURS.toMillis(1), PM_STATS_BY_HOUR},
                {TimeUnit.DAYS.toMillis(7), TimeUnit.DAYS.toMillis(1), PM_STATS_BY_DAY},
                {TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(7), PM_STATS_BY_DAY},
                {TimeUnit.DAYS.toMillis(365), TimeUnit.DAYS.toMillis(30), PM_STATS_BY_MONTH}
        });
    }

    /**
     * These are the test parameters injected by Parameterized runner. Must be public
     * to allow injection.

     * Value to subtract from NOW to give earliest time in the range.
     */
    @Parameter(0)
    public long startTimeDeltaMs;

    /**
     * Value to add to NOW to give the most recent time in the range.
     */
    @Parameter(1)
    public long endTimeDeltaMs;

    /**
     * The DB table which should be read for this time range.
     */
    @Parameter(2)
    public Table expectedTableToRead;

    @Test
    public void testGetStatsRecords() throws Exception {

        // copied from standard OpsManager configuration
        final int NUM_RETAINED_MINUTES=120;
        final int NUM_RETAINED_HOURS=72;
        final int NUM_RETAINED_DAYS=60;
        long LATEST_TABLE_TIME_WINDOW_MS = 60000;

        // Arrange
        final long NOW = new Date().getTime();
        Optional<Timestamp> mockNow = Optional.of(new Timestamp(NOW));
        HistorydbIO mockHistorydbIO = Mockito.mock(HistorydbIO.class);
        when(mockHistorydbIO.getMostRecentTimestamp()).thenReturn(mockNow);
        DSLContext testDSLContext = DSL.using(SQLDialect.MYSQL);
        when(mockHistorydbIO.JooqBuilder()).thenReturn(testDSLContext);
        when(mockHistorydbIO.getTypesForEntities(any())).thenReturn(ImmutableMap.of(
                "PM1", "PhysicalMachine"
        ));
        Select mockQuery = Mockito.mock(Select.class);
        when(mockHistorydbIO.getStatsSelect(any(), any(), any(), any()))
                .thenReturn(mockQuery);

        Result queryResult = testDSLContext.newResult(expectedTableToRead);
        when(mockHistorydbIO.execute(any(BasedbIO.Style.class), any(Select.class))).thenReturn(queryResult);

        LiveStatsReader statsReaderUndertest = new LiveStatsReader(mockHistorydbIO, NUM_RETAINED_MINUTES,
                NUM_RETAINED_HOURS, NUM_RETAINED_DAYS, LATEST_TABLE_TIME_WINDOW_MS);


        List<String> entityIds = ImmutableList.of("PM1");
        List<String> commodityNames = ImmutableList.of("C1");

        // Act
        statsReaderUndertest.getStatsRecords(entityIds, NOW - startTimeDeltaMs, NOW + endTimeDeltaMs,
                commodityNames);

        // Assert
        verify(mockHistorydbIO).getMostRecentTimestamp();
        verify(mockHistorydbIO).getTypesForEntities(any());

        ArgumentCaptor<Table> queryCaptor = ArgumentCaptor.forClass(Table.class);
        verify(mockHistorydbIO, times(1)).getStatsSelect(queryCaptor.capture(), any(), any(), any());
        Table queryTable = queryCaptor.getAllValues().get(0);
        assertThat(queryTable, is(expectedTableToRead));
        verify(mockHistorydbIO, times(1)).execute(eq(BasedbIO.Style.FORCED), eq(mockQuery));

    }


}