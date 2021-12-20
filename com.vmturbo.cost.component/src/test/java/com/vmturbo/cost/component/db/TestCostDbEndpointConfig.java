package com.vmturbo.cost.component.db;

import java.sql.SQLException;

import org.jooq.DSLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Workaround for {@link CostDbEndpointConfig} (remove conditional annotation), since
 * it's conditionally initialized based on {@link com.vmturbo.components.common.featureflags.FeatureFlags#POSTGRES_PRIMARY_DB}. When we
 * test all combinations of it using {@link com.vmturbo.test.utils.FeatureFlagTestRule}, first it's false, so
 * {@link CostDbEndpointConfig} is not created; then second it's true,
 * {@link CostDbEndpointConfig} is created, but the endpoint inside is also eagerly
 * initialized due to the same FF, which results in several issues like: it doesn't go through
 * DbEndpointTestRule, making call to auth to get root password, etc.
 */
@Configuration
public class TestCostDbEndpointConfig extends CostDbEndpointConfig {

    /**
     * Overrides the existing bean to return null.
     *
     * @return {@link DSLContext}
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Override
    @Bean
    public DSLContext dsl() throws SQLException, UnsupportedDialectException, InterruptedException {
        return null;
    }
}
