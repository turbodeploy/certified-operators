package com.vmturbo.repository.plan.db;

import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for repository database access.
 */
@Configuration
public class RepositoryDBConfig extends SQLDatabaseConfig {

    /**
     * DB user name accessible to given schema.
     */
    @Value("${repositoryDbUsername:repository}")
    private String repoDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${repositoryDbPassword:}")
    private String repositoryDbPassword;

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:repository}")
    private String dbSchemaName;

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }

    @Bean
    @Override
    public DataSource dataSource() {
        return getDataSource(dbSchemaName, repoDbUsername,
                Optional.ofNullable(!Strings.isEmpty(repositoryDbPassword) ? repositoryDbPassword : null));
    }
}
