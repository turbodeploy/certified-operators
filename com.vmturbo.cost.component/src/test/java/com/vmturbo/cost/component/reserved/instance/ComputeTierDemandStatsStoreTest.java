package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestCostDbEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class ComputeTierDemandStatsStoreTest {

    @Autowired(required = false)
    private TestCostDbEndpointConfig dbEndpointConfig;

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @Rule
    public DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("cost");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    private DSLContext dsl;

    /**
     * Set up before each test.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Before
    public void before() throws SQLException, UnsupportedDialectException, InterruptedException {
        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.costEndpoint());
            dsl = dbEndpointConfig.costEndpoint().dslContext();
        } else {
            dsl = dbConfig.getDslContext();
        }
    }

    @Test
    public void testPersistence() {

        // Construct 2 records across different hours
        final ComputeTierTypeHourlyByWeekRecord recordA = new ComputeTierTypeHourlyByWeekRecord(
                (byte)1, (byte)2, 3L, 4L, 5L, (byte)6, (byte)7, new BigDecimal("8.000"), new BigDecimal("9.000"));
        final ComputeTierTypeHourlyByWeekRecord recordB = new ComputeTierTypeHourlyByWeekRecord(
                (byte)1, (byte)3, 3L, 4L, 5L, (byte)6, (byte)7, new BigDecimal("8.000"), new BigDecimal("9.000"));

        final ComputeTierDemandStatsStore store = new ComputeTierDemandStatsStore(dsl, 1, 1);

        // persist the records
        store.persistComputeTierDemandStats(ImmutableList.of(recordA, recordB), false);

        // verify the records match
        final List<ComputeTierTypeHourlyByWeekRecord> actualRecords = dsl.selectFrom(Tables.COMPUTE_TIER_TYPE_HOURLY_BY_WEEK)
                .fetchInto(ComputeTierTypeHourlyByWeekRecord.class);

        assertThat(actualRecords, hasSize(2));
        assertThat(actualRecords, containsInAnyOrder(recordA, recordB));
    }

}
