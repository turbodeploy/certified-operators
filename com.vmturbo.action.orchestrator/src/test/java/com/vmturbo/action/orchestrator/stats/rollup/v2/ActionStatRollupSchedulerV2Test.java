package com.vmturbo.action.orchestrator.stats.rollup.v2;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.google.common.util.concurrent.MoreExecutors;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.action.orchestrator.TestActionOrchestratorDbEndpointConfig;
import com.vmturbo.action.orchestrator.db.Action;
import com.vmturbo.action.orchestrator.stats.rollup.RollupTestUtils;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit tests for {@link ActionStatRollupSchedulerV2}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestActionOrchestratorDbEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class ActionStatRollupSchedulerV2Test {

    @Autowired(required = false)
    private TestActionOrchestratorDbEndpointConfig dbEndpointConfig;

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Action.ACTION);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @Rule
    public DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("ao");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule =
            new FeatureFlagTestRule().testAllCombos(FeatureFlags.POSTGRES_PRIMARY_DB);

    private DSLContext dsl;

    private static final int ACTION_GROUP_ID = 1123;

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private ActionStatRollupSchedulerV2 scheduler;

    private RollupTestUtils rollupTestUtils;

    private ExecutorService executorService = MoreExecutors.newDirectExecutorService();

    private HourActionRollupFactory rollupFactory;

    private ActionRollupAlgorithmMigrator algorithmMigrator;

    /**
     * Set up for tests.
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if thread has been interrupted
     */
    @Before
    public void setup() throws SQLException, UnsupportedDialectException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        algorithmMigrator = mock(ActionRollupAlgorithmMigrator.class);
        rollupFactory = mock(HourActionRollupFactory.class);
        HourActionStatRollup mockRollup = mock(HourActionStatRollup.class);
        when(rollupFactory.newRollup(any(), any(), anyInt()))
                .thenReturn(mockRollup);

        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.actionOrchestratorEndpoint());
            dsl = dbEndpointConfig.actionOrchestratorEndpoint().dslContext();
        } else {
            dsl = dbConfig.getDslContext();
        }

        rollupTestUtils = new RollupTestUtils(dsl);
        scheduler = new ActionStatRollupSchedulerV2(rollupFactory, algorithmMigrator, executorService, dsl, clock);
    }

    /**
     * Test that the right times are scheduled for rollup.
     */
    @Test
    public void testScheduleRollupTimes() {
        verify(algorithmMigrator).doMigration();
        final LocalDateTime curTime = RollupTestUtils.time(13, 00);
        clock.changeInstant(curTime.toInstant(ZoneOffset.UTC));

        final LocalDateTime time = RollupTestUtils.time(12, 30);
        final LocalDateTime time1 = RollupTestUtils.time(12, 45);
        final int mgmtSubgroup1 = 1;
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time1);

        // The 12:00 time should be ready for rollup, because it's now past 12:59.
        verifyExpectedRollupTimes(Collections.singletonMap(RollupTestUtils.time(12, 0), 2));
    }

    /**
     * Test that the current hour is not scheduled for rollup.
     */
    @Test
    public void testReaderRollupReadyTimesCurHourExcluded() {
        final LocalDateTime curTime = RollupTestUtils.time(13, 30);
        clock.changeInstant(curTime.toInstant(ZoneOffset.UTC));

        final LocalDateTime time2 = RollupTestUtils.time(13, 2);
        final int mgmtSubgroup2 = 2;
        final int mgmtSubgroup3 = 3;
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup2, ACTION_GROUP_ID, time2);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup3, ACTION_GROUP_ID, time2);

        // The latest time does not get included, because it's still not ready for rollup,
        // since it's in the "current" hour.
        verifyExpectedRollupTimes(Collections.emptyMap());
    }

    /**
     * Test that already-rolled-up hours are not scheduled for rollup.
     */
    @Test
    public void testReaderRollupReadyAlreadyRolledUp() {
        final LocalDateTime time = RollupTestUtils.time(12, 30);
        final LocalDateTime time1 = RollupTestUtils.time(12, 45);
        final int mgmtSubgroup1 = 1;
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time);
        rollupTestUtils.insertMgmtUnitStatRecord(mgmtSubgroup1, ACTION_GROUP_ID, time1);

        // Insert hourly snapshot for the time
        rollupTestUtils.insertHourlySnapshotOnly(time);

        verifyExpectedRollupTimes(Collections.emptyMap());
    }

    /**
     * Test the case where the database is empty.
     */
    @Test
    public void testReaderRollupReadyNoData() {
        verifyExpectedRollupTimes(Collections.emptyMap());
    }

    private void verifyExpectedRollupTimes(Map<LocalDateTime, Integer> timesAndNumSnapshots) {
        scheduler.scheduleRollups();
        ArgumentCaptor<LocalDateTime> timeCaptor = ArgumentCaptor.forClass(LocalDateTime.class);
        ArgumentCaptor<Integer> numSnapshotsCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(rollupFactory, times(timesAndNumSnapshots.size())).newRollup(any(), timeCaptor.capture(), numSnapshotsCaptor.capture());
        List<LocalDateTime> allTimes = timeCaptor.getAllValues();
        List<Integer> allSnapshots = numSnapshotsCaptor.getAllValues();
        assertThat(allTimes.size(), is(timesAndNumSnapshots.size()));
        for (int i = 0; i < allTimes.size(); ++i) {
            LocalDateTime time = allTimes.get(i);
            Integer snapshot = allSnapshots.get(i);
            assertTrue(timesAndNumSnapshots.containsKey(time));
            assertThat(timesAndNumSnapshots.get(time), is(snapshot));
        }
    }
}
