package com.vmturbo.history.db;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;

import org.mockito.Mockito;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Workaround for {@link HistoryDbEndpointConfig} (remove conditional annotation), since it's
 * conditionally initialized based on {@link FeatureFlags#POSTGRES_PRIMARY_DB}. When we test all
 * combinations of it using {@link FeatureFlagTestRule}, first it's false, so {@link
 * HistoryDbEndpointConfig} is not created; then second it's true, {@link HistoryDbEndpointConfig}
 * is created, but the endpoint inside is also eagerly initialized due to the same FF, which results
 * in several issues like: it doesn't go through DbEndpointTestRule, making call to auth to get root
 * password, etc.
 */
@Configuration
public class TestHistoryDbEndpointConfig extends HistoryDbEndpointConfig {

    @Override
    public DbEndpointCompleter endpointCompleter() {
        // prevent actual completion of the DbEndpoint
        DbEndpointCompleter dbEndpointCompleter = Mockito.spy(super.endpointCompleter());
        doNothing().when(dbEndpointCompleter).setEnvironment(any());
        return dbEndpointCompleter;
    }
}
