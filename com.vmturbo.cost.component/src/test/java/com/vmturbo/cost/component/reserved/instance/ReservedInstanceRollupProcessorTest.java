package com.vmturbo.cost.component.reserved.instance;

import static org.mockito.Mockito.mock;

import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Optional;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.rollup.LastRollupTimes;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.rollup.RollupTimesStore;
import com.vmturbo.cost.component.topology.IngestedTopologyStore;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Class to test {@link ReservedInstanceRollupProcessor}.
 */
@RunWith(Parameterized.class)
public class ReservedInstanceRollupProcessorTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    private final RollupTimesStore rollupUtilizationTimesStore = Mockito.mock(RollupTimesStore.class);

    private final RollupTimesStore rollupCoverageTimesStore = Mockito.mock(RollupTimesStore.class);

    private final IngestedTopologyStore ingestedTopologyStore =
        Mockito.mock(IngestedTopologyStore.class);

    private ReservedInstanceRollupProcessor reservedInstanceRollupProcessor;

    private ReservedInstanceUtilizationStore reservedInstanceUtilizationStore =
        mock(ReservedInstanceUtilizationStore.class);

    private ReservedInstanceCoverageStore reservedInstanceCoverageStore =
            mock(ReservedInstanceCoverageStore.class);

    private final Clock clock = Clock.systemUTC();

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect DB dialect to use
     * @throws SQLException if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    public ReservedInstanceRollupProcessorTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost", TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Rule chain to manage db provisioning and lifecycle.
     */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    /**
     * Set up before each test.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Before
    public void setup() throws SQLException, UnsupportedDialectException, InterruptedException {
        reservedInstanceRollupProcessor = new ReservedInstanceRollupProcessor(
                reservedInstanceUtilizationStore, reservedInstanceCoverageStore,
                rollupUtilizationTimesStore, rollupCoverageTimesStore, ingestedTopologyStore,
                clock);
    }

    /**
     * Test that rollups run properly
     */
    @Test
    public void testRollupRoutine() {
        LocalDateTime today = LocalDateTime.now();
        LocalDateTime yesterday = today.minus(1, ChronoUnit.DAYS);
        long yesterdayMs = yesterday.toEpochSecond(ZoneOffset.UTC);
        LastRollupTimes lastRollupTimes = new LastRollupTimes(yesterdayMs, yesterdayMs,
            yesterdayMs, yesterdayMs);
        Mockito.when(rollupUtilizationTimesStore.getLastRollupTimes()).thenReturn(lastRollupTimes);
        Mockito.when(ingestedTopologyStore.getCreationTimeSinceLastTopologyRollup(Optional.of(yesterdayMs)))
            .thenReturn(Collections.singletonList(today));
        reservedInstanceRollupProcessor.execute();

        Mockito.verify(reservedInstanceUtilizationStore, Mockito.times(1))
            .performRollup(RollupDurationType.DAILY, Collections.singletonList(today));
        Mockito.verify(reservedInstanceUtilizationStore, Mockito.times(1))
            .performRollup(RollupDurationType.HOURLY, Collections.singletonList(today));
        Mockito.verify(reservedInstanceUtilizationStore, Mockito.times(1))
            .performRollup(RollupDurationType.MONTHLY, Collections.singletonList(today));

        Mockito.verify(reservedInstanceCoverageStore, Mockito.times(1))
                .performRollup(RollupDurationType.DAILY, Collections.singletonList(today));
        Mockito.verify(reservedInstanceCoverageStore, Mockito.times(1))
                .performRollup(RollupDurationType.HOURLY, Collections.singletonList(today));
        Mockito.verify(reservedInstanceCoverageStore, Mockito.times(1))
                .performRollup(RollupDurationType.MONTHLY, Collections.singletonList(today));

        Mockito.verify(rollupUtilizationTimesStore, Mockito.times(1))
                .setLastRollupTimes(lastRollupTimes);
    }
}
