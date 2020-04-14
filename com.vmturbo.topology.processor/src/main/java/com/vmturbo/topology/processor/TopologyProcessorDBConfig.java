package com.vmturbo.topology.processor;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for topology-processor component interaction with a schema.
 */
@Configuration
public class TopologyProcessorDBConfig extends SQLDatabaseConfig {

    /**
     * DB user name accessible to given schema.
     */
    @Value("${topologyProcessorDbUsername:topology_processor}")
    private String topologyProcessorDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${topologyProcessorDbPassword:}")
    private String topologyProcessorDbPassword;

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:topology_processor}")
    private String dbSchemaName;

    @Bean
    @Override
    public DataSource dataSource() {
        // If no db password specified, use root password by default.
        DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort, authRoute,
            authRetryDelaySecs);
        String dbPassword = !Strings.isEmpty(topologyProcessorDbPassword) ?
            topologyProcessorDbPassword : dbPasswordUtil.getSqlDbRootPassword();
        return dataSourceConfig(dbSchemaName, topologyProcessorDbUsername, dbPassword);
    }
}

