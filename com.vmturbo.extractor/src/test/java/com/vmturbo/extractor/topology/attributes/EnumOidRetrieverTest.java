package com.vmturbo.extractor.topology.attributes;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.sql.SQLException;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.schema.Extractor;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.topology.attributes.EnumOidRetriever.EnumOidRetrievalException;
import com.vmturbo.extractor.topology.attributes.EnumOidRetriever.PostgresEnumOidRetriever;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit tests for {@link EnumOidRetriever}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ExtractorDbConfig.class, ExtractorDbBaseConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"enableReporting=true", "sqlDialect=POSTGRES"})
public class EnumOidRetrieverTest implements ApplicationContextAware {

    @Autowired
    private ExtractorDbConfig dbConfig;

    /**
     * Rule to manage configured endpoints for tests.
     */
    @Rule
    @ClassRule
    public static DbEndpointTestRule endpointRule = new DbEndpointTestRule("extractor");

    /**
     * Manage feature flags.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

    private DbEndpoint ingesterEndpoint;

    /**
     * Test that the {@link PostgresEnumOidRetriever} retrieves enum oids from the database.
     *
     * @throws EnumOidRetrievalException Compiler.
     */
    @Test
    public void testOidRetriever() throws EnumOidRetrievalException {
        PostgresEnumOidRetriever<EntityState> stateEnumRetriever = new PostgresEnumOidRetriever<>(EntityState.class,
                EntityState.POWERED_ON.getName());
        Map<EntityState, Integer> retMap = stateEnumRetriever.getEnumOids(ingesterEndpoint);
        assertThat(retMap.keySet(), containsInAnyOrder(EntityState.values()));
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        try {
            this.ingesterEndpoint = endpointRule.completeEndpoint(dbConfig.ingesterEndpoint(),
                    Extractor.EXTRACTOR).getDbEndpoint();
        } catch (UnsupportedDialectException | InterruptedException | SQLException e) {
            throw new BeanCreationException("Failed to configure endpoint", e);
        }
    }
}