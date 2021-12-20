package com.vmturbo.repository.plan.db;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;

/**
 * Testing config by removing the {@link Conditional} annotation and mocking a
 * {@link DbEndpointCompleter} so it doesn't actually complete.
 */
@Configuration
public class TestRepositoryDbEndpointConfig extends RepositoryDBEndpointConfig {
    @Override
    public DbEndpointCompleter endpointCompleter() {
        // Prevent actual completion of the DbEndpoint.
        DbEndpointCompleter dbEndpointCompleter = spy(super.endpointCompleter());
        doNothing().when(dbEndpointCompleter).setEnvironment(any());
        return dbEndpointCompleter;
    }
}
