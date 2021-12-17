package com.vmturbo.group;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;

/**
 * Test Endpoint.
 */
@Configuration
public class TestGroupDBEndpointConfig extends GroupDBEndpointConfig {

    @Override
    public DbEndpointCompleter endpointCompleter() {
        // prevent actual completion of the DbEndpoint
        DbEndpointCompleter dbEndpointCompleter = spy(super.endpointCompleter());
        doNothing().when(dbEndpointCompleter).setEnvironment(any());
        return dbEndpointCompleter;
    }
}
