package com.vmturbo.extractor.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

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
import com.vmturbo.cost.calculation.CloudCostCalculator;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Tests of certain schema objects that require a live database.
 *
 * <p>For example, unlike our enum types, where we can test correct member list by using metadata
 * captured by jOOQ in its generated classes, a table function that needs to be executed in order to
 * be tested will need a live DB.</p>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"enableReporting=true", "sqlDialect=POSTGRES"})
public class DBTests {

    private DbEndpoint endpoint = null;

    /** Get access to extractor's configured {@link DbEndpoint} instances. */
    @Autowired
    private ExtractorDbConfig dbConfig;

    /**
     * Manage the live DB endpoint we're using for our tests.
     */
    @Rule
    @ClassRule
    public static DbEndpointTestRule endpointRule = new DbEndpointTestRule("extractor");

    /** Manage feature flag states. */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule()
            .testAllCombos(FeatureFlags.POSTGRES_PRIMARY_DB);

    /**
     * Set up for tests.
     *
     * @throws UnsupportedDialectException if the endpoint is mis-configured
     * @throws SQLException                if there's a problem
     * @throws InterruptedException        if interrupted
     */
    @Before
    public void before() throws UnsupportedDialectException, SQLException, InterruptedException {
        this.endpoint = dbConfig.ingesterEndpoint();
        endpointRule.addEndpoints(endpoint);
    }

    /**
     * Test that our `entity_types_with_cost` table function delivers entity types that agree with
     * the constant list in the cost component.
     *
     * @throws SQLException                If there's a DB problem
     * @throws UnsupportedDialectException for unsupported dialect
     * @throws InterruptedException        if we're interrupted
     */
    @Test
    public void testEntityTypesWithCostIsCorrect() throws SQLException, UnsupportedDialectException, InterruptedException {
        final Set<String> expected = CloudCostCalculator.ENTITY_TYPES_WITH_COST.stream()
                .map(CommonDTO.EntityDTO.EntityType::forNumber)
                .map(Enum::name)
                .collect(Collectors.toSet());
        final Set<String> actual = endpoint.dslContext().selectFrom(Extractor.ENTITY_TYPES_WITH_COST())
                .fetch(Extractor.ENTITY_TYPES_WITH_COST().TYPE).stream()
                .map(EntityType::name)
                .collect(Collectors.toSet());
        assertThat(actual, is(expected));
    }

}
