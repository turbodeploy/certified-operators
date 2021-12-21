package com.vmturbo.history.db;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.sql.utils.ConditionalDbConfig.DbEndpointCondition;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.DbEndpointsConfig;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Config class to establish DB access for history compoennt, based on {@link DbEndpoint} facility.
 */
@Configuration
@Conditional(DbEndpointCondition.class)
public class HistoryDbEndpointConfig extends DbEndpointsConfig {

    /**
     * Create a {@link DbEndpoint} for accessing history DB.
     *
     * @return DbEndpoint
     */
    @Bean
    public DbEndpoint historyEndpoint() {
        return fixEndpointForMultiDb(
                dbEndpoint("dbs.history", sqlDialect)
                        // TODO remove next line as part of OM-77149
                        .withMigrationLocations("db.migration")
                        .withShouldProvision(true)
                        .withRootAccessEnabled(true)
                        .withAccess(DbEndpointAccess.ALL))
                .build();
    }

    /**
     * Workaround for {@link HistoryDbEndpointConfig} (remove conditional annotation), since it's
     * conditionally initialized based on {@link FeatureFlags#POSTGRES_PRIMARY_DB}. When we test all
     * combinations of it using {@link FeatureFlagTestRule}, first it's false, so {@link
     * HistoryDbEndpointConfig} is not created; then second it's true, {@link
     * HistoryDbEndpointConfig} is created, but the endpoint inside is also eagerly initialized due
     * to the same FF, which results in several issues like: it doesn't go through
     * DbEndpointTestRule, making call to auth to get root password, etc.
     */
    @Configuration
    public static class TestHistoryDbEndpointConfig extends HistoryDbEndpointConfig {

        @Override
        public DbEndpointCompleter endpointCompleter() {
            // prevent actual completion of the DbEndpoint
            DbEndpointCompleter dbEndpointCompleter = Mockito.spy(super.endpointCompleter());
            doNothing().when(dbEndpointCompleter).setEnvironment(any());
            return dbEndpointCompleter;
        }
    }
}
