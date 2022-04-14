package com.vmturbo.cost.component.db;

import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.jooq.DSLContext;
import org.jooq.impl.DefaultDSLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.ConditionalDbConfig.SQLDatabaseConfigCondition;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for cost component interaction with a schema.
 */
@Configuration
@Conditional(SQLDatabaseConfigCondition.class)
public class CostDBConfig extends SQLDatabaseConfig {

    /**
     * DB user name accessible to given schema.
     */
    @Value("${costDbUsername:cost}")
    private String costDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${costDbPassword:}")
    private String costDbPassword;

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:cost}")
    private String dbSchemaName;

    @Bean
    @Override
    public DataSource dataSource() {
        return getDataSource(dbSchemaName, costDbUsername, Optional.ofNullable(
                !Strings.isEmpty(costDbPassword) ? costDbPassword : null));
    }

    /**
     * Get a {@link DataSource} that will produce connections that are not part of the connection
     * pool. This may be advisable for connections that will be used for potentially long-running
     * operations, to avoid tying up limited pool connections.
     *
     * @return unpooled datasource
     */
    @Bean
    public DataSource unpooledDataSource() {
        return getUnpooledDataSource(dbSchemaName, costDbUsername,
                Optional.ofNullable(!Strings.isEmpty(costDbPassword) ? costDbPassword : null));
    }

    /** Whether DbMonitor reports should be produced at all. */
    @Value("${dbMonitorEnabled:true}")
    private boolean dbMonitorEnabled;

    public boolean isDbMonitorEnabled() {
        return dbMonitorEnabled;
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }

    @Override
    public String getDbUsername() {
        return costDbUsername;
    }

    /**
     * Get a {@link DSLContext} that uses unpooled connections to perform database operations.
     * This may be advisable when performing potentially long-running DB operaitions to avoid
     * tying up limited pool connections.
     *
     * @return DSLContext that uses unpooled connections
     */
    public DSLContext unpooledDsl() {
        return new DefaultDSLContext(configuration(unpooledDataSource()));
    }
}
