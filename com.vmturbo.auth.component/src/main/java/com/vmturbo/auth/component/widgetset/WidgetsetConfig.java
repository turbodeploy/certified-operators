package com.vmturbo.auth.component.widgetset;

import com.vmturbo.auth.component.DbAccessConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.component.store.AuthProvider;

import java.sql.SQLException;

/**
 * Spring Configuration for the WidgetsetDBStore functionality. Uses the MariaDB configuration.
 **/
@Configuration
@Import({DbAccessConfig.class})
public class WidgetsetConfig {

    @Autowired
    private DbAccessConfig authDBConfig;

    @Bean
    public WidgetsetDbStore widgetsetDbStore() {
        try {
            return new WidgetsetDbStore(authDBConfig.dsl());
        } catch (SQLException | DbEndpoint.UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create Widgetset Db Store", e);
        }
    }

    @Bean
    public WidgetsetRpcService widgetsetRpcService(AuthProvider targetStore) {
        return new WidgetsetRpcService(widgetsetDbStore(), targetStore);
    }
}
