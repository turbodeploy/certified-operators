package com.vmturbo.market;

import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for market component interaction with a schema.
 */
@Configuration
public class MarketDBConfig extends SQLDatabaseConfig {
    /**
     * DB user name accessible to given schema.
     */
    @Value("${marketDbUsername:market}")
    private String marketDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${marketDbPassword:}")
    private String marketDbPassword;

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:market}")
    private String dbSchemaName;

    /**
     * Initialize market component DB config by running flyway migration and creating a user.
     *
     * @return DataSource of market component DB.
     */
    @Bean
    @Override
    public DataSource dataSource() {
        return getDataSource(dbSchemaName, marketDbUsername, Optional.ofNullable(
                !Strings.isEmpty(marketDbPassword) ? marketDbPassword : null));
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }

}

