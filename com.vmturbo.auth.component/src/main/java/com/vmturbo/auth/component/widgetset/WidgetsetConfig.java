package com.vmturbo.auth.component.widgetset;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.component.AuthDBConfig;
import com.vmturbo.auth.component.store.AuthProvider;

/**
 * Spring Configuration for the WidgetsetDBStore functionality. Uses the MariaDB configuration.
 **/
@Configuration
@Import({AuthDBConfig.class})
public class WidgetsetConfig {

    @Autowired
    private AuthDBConfig authDBConfig;

    @Bean
    public WidgetsetDbStore widgetsetDbStore() {
        return new WidgetsetDbStore(authDBConfig.dsl());
    }

    @Bean
    public WidgetsetRpcService widgetsetRpcService(AuthProvider targetStore) {
        return new WidgetsetRpcService(widgetsetDbStore(), targetStore);
    }
}
