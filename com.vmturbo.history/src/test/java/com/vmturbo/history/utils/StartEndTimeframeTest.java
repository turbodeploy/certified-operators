package com.vmturbo.history.utils;

import static com.vmturbo.history.utils.HistoryStatsUtils.betweenStartEndTimestampCond;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jooq.Condition;
import org.jooq.TableField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.schema.abstraction.tables.VmStatsByDay;
import com.vmturbo.history.schema.abstraction.tables.VmStatsByHour;
import com.vmturbo.history.schema.abstraction.tables.VmStatsByMonth;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;

/**
 * Test the utility to create an SQL query "where" clause for start and end time for a given
 * TimeFrame.
 */
@RunWith(Parameterized.class)
public class StartEndTimeframeTest {

    @Parameters
    public static Collection<Object[]> testData() {
        return Arrays.asList(new Object[][] {
                {TimeFrame.LATEST, "exact values",
                        "2016-03-13 09:20:45.123", "2016-03-13 09:20:45.123",
                        "2016-03-13 11:20:45.123",   "2016-03-13 11:20:45.123",
                        VmStatsLatest.VM_STATS_LATEST.SNAPSHOT_TIME, timestampPattern},
                {TimeFrame.HOUR, "start-of-hour, end-of-hour",
                        "2016-03-13 09:20:45.123", "2016-03-13 09:00:00.0",
                        "2016-03-13 11:20:45.123",   "2016-03-13 11:59:59.0",
                        VmStatsByHour.VM_STATS_BY_HOUR.SNAPSHOT_TIME, timestampPattern},
                {TimeFrame.DAY, "start-of-day, end-of-day",
                        "2016-03-13 09:20:45.123", "2016-03-13 00:00:00.0",
                        "2016-03-13 11:20:45.123",   "2016-03-13 23:59:59.0",
                        VmStatsByDay.VM_STATS_BY_DAY.SNAPSHOT_TIME, timestampPattern},
                {TimeFrame.MONTH, "start-of-month, end-of-month",
                        "2016-03-13 09:20:45.123", "2016-03-01 00:00:00.0",
                        "2016-03-13 11:20:45.123",   "2016-03-31 23:59:59.0",
                        VmStatsByMonth.VM_STATS_BY_MONTH.SNAPSHOT_TIME, timestampPattern}
        });
    }
    private static final Pattern timestampPattern =
            Pattern.compile(".*between timestamp '([\\d .:\\-]*)' and timestamp '([\\d .:\\-]*)'$");
    private static final Pattern datePattern =
            Pattern.compile(".*between date '([\\d .:\\-]*)' and date '([\\d .:\\-]*)'$");
    private static final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Parameter(0)
    public TimeFrame timeFrame;
    @Parameter(1)
    public String desc;
    @Parameter(2)
    public String startTest;
    @Parameter(3)
    public String startExpected;
    @Parameter(4)
    public String endTest;
    @Parameter(5)
    public String endExpected;
    @Parameter(6)
    public TableField<?,?> field;
    @Parameter(7)
    public Pattern resultPattern;

    @Test
    public void betweenStartEndTimestampCondTest() throws Exception {
        // Arrange
        desc = timeFrame + ": " + desc;
        long startTime = dateParser.parse(startTest).getTime();
        long endTime = dateParser.parse(endTest).getTime();

        // Act
        Condition answer = betweenStartEndTimestampCond(field, timeFrame,
                startTime, endTime);
        String result = answer.toString();
        // Assert
        Matcher m = resultPattern.matcher(result);
        assertThat(desc, m.find(), is(true));
        assertThat(desc, m.groupCount(), is(2));

        String resultStart = m.group(1);
        assertThat(desc + "; start", resultStart, equalTo(startExpected));

        String resultEnd = m.group(2);
        assertThat(desc + "; end", resultEnd, equalTo(endExpected));

    }
}
