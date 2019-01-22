package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionGroupStat;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=action"})
public class LatestActionStatTableTest {
    @Autowired
    protected TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private DSLContext dsl;

    private static final int ACTION_GROUP_ID = 1123;

    private RolledUpStatCalculator calculator = mock(RolledUpStatCalculator.class);

    private RollupTestUtils rollupTestUtils;

    @Before
    public void setup() {

        // Clean the database and bring it up to the production configuration before running test
        flyway = dbConfig.flyway();
        flyway.clean();
        flyway.migrate();

        // Grab a handle for JOOQ DB operations
        dsl = dbConfig.dsl();

        rollupTestUtils = new RollupTestUtils(dsl);
        rollupTestUtils.insertActionGroup(ACTION_GROUP_ID);
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testNoWriter() {
        final LatestActionStatTable statsTable = new LatestActionStatTable(dsl,
            calculator, HourActionStatTable.HOUR_TABLE_INFO);
        assertFalse(statsTable.writer().isPresent());
    }


    @Test
    public void testReaderRollup() {
        final LatestActionStatTable statsTable = new LatestActionStatTable(dsl,
            calculator, HourActionStatTable.HOUR_TABLE_INFO);
        final int mgmtSubgroupId = 1;
        final LocalDateTime time = RollupTestUtils.time(12, 30);
        final LocalDateTime time2 = RollupTestUtils.time(12, 40);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroupId, ACTION_GROUP_ID, time);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroupId, ACTION_GROUP_ID, time2);

        final LatestActionStatTable.Reader reader = statsTable.reader().get();
        RolledUpActionGroupStat rollupStat = mock(RolledUpActionGroupStat.class);
        when(calculator.rollupLatestRecords(anyInt(), any())).thenReturn(Optional.of(rollupStat));

        final Optional<RolledUpActionStats> statSummaryOpt =
            reader.rollup(mgmtSubgroupId, time.truncatedTo(ChronoUnit.HOURS));
        final RolledUpActionStats statSummary = statSummaryOpt.get();
        assertThat(statSummary.statsByActionGroupId().keySet(), containsInAnyOrder(ACTION_GROUP_ID));
        assertThat(statSummary.statsByActionGroupId().get(ACTION_GROUP_ID), is(rollupStat));
    }

    @Test
    public void testReaderRollupNoSnapshots() {
        final LatestActionStatTable statsTable = new LatestActionStatTable(dsl,
            calculator, HourActionStatTable.HOUR_TABLE_INFO);
        final LatestActionStatTable.Reader reader = statsTable.reader().get();
        assertFalse(reader.rollup(1, RollupTestUtils.time(12, 0)).isPresent());
    }

}
