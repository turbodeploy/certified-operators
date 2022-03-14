package com.vmturbo.group;

import java.sql.SQLException;

import org.jooq.SQLDialect;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDB;

/**
 * Configuration for supporting multiple databases by resolving any discrepancies between them.
 */
@Configuration
@Import({DbAccessConfig.class})
public class GroupMultiDBConfig {

    @Autowired
    private DbAccessConfig dbConfig;

    /**
     * MultiDB object to provide support for multiple databases.
     *
     * @return MultiDB instance
     * @throws BeanCreationException in case the bean creation failed
     */
    @Bean
    public MultiDB multiDB() {
        try {
            final SQLDialect dialect = dbConfig.dsl().configuration().family();
            return MultiDB.of(dialect);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create MultiDB", e);
        }
    }
}
