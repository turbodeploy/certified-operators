package com.vmturbo.group;

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
 * Configuration for group component interaction with a schema.
 */
@Configuration
@Conditional(SQLDatabaseConfigCondition.class)
public class GroupComponentDBConfig extends SQLDatabaseConfig {

    /**
     * DB user name accessible to given schema.
     */
    @Value("${groupComponentDbUsername:group_component}")
    private String groupComponentDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${groupComponentDbPassword:}")
    private String groupComponentDbPassword;

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:group_component}")
    private String dbSchemaName;

    @Bean
    @Override
    public DataSource dataSource() {
        return getDataSource(dbSchemaName, groupComponentDbUsername, Optional.ofNullable(
                !Strings.isEmpty(groupComponentDbPassword) ? groupComponentDbPassword : null));
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
        return groupComponentDbUsername;
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
        return getUnpooledDataSource(dbSchemaName, groupComponentDbUsername,
                Optional.ofNullable(!Strings.isEmpty(groupComponentDbPassword) ? groupComponentDbPassword : null));
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
