package com.vmturbo.plan.orchestrator;

import static com.vmturbo.plan.orchestrator.db.tables.Scenario.SCENARIO;
import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;
import java.util.Collections;

import org.jooq.DSLContext;
import org.jooq.Result;
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

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.db.tables.records.ScenarioRecord;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileDaoImplTest.TestPlanOrchestratorDBEndpointConfig;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Tests for database schema.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestPlanOrchestratorDBEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class DatabaseSchemaIT {

    @Autowired(required = false)
    private TestPlanOrchestratorDBEndpointConfig dbEndpointConfig;

    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Plan.PLAN);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @Rule
    public DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("tp");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    protected DSLContext dsl;

    @Before
    public void prepare() throws Exception {
        IdentityGenerator.initPrefix(0);

        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.planEndpoint());
            dsl = dbEndpointConfig.planEndpoint().dslContext();
        } else {
            dsl = dbConfig.getDslContext();
        }
    }

    @Test
    public void testCreatePlanProject() throws Exception {
        long projectId = IdentityGenerator.next();
        LocalDateTime now = LocalDateTime.now();

        dsl.insertInto(SCENARIO)
            .set(SCENARIO.ID, projectId)
            .set(SCENARIO.CREATE_TIME, now)
            .set(SCENARIO.UPDATE_TIME, now)
            .set(SCENARIO.SCENARIO_INFO, ScenarioInfo.newBuilder()
                .setName("foo")
                .build())
            .execute();

        Result<ScenarioRecord> results = dsl.selectFrom(SCENARIO)
            .where(SCENARIO.ID.eq(projectId))
            .fetch();

        assertEquals(Collections.singletonList(projectId), results.getValues(SCENARIO.ID));
        assertEquals("foo", results.getValues(SCENARIO.SCENARIO_INFO).get(0).getName());
    }
}