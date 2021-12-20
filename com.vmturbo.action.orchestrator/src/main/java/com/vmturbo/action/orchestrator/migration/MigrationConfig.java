package com.vmturbo.action.orchestrator.migration;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.DbAccessConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({DbAccessConfig.class})
public class MigrationConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Bean
    public ActionMigrationsLibrary actionsMigrationsLibrary() {
        try {
            return new ActionMigrationsLibrary(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create actionsMigrationsLibrary", e);
        }
    }
}
