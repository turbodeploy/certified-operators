package com.vmturbo.topology.processor;

import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
        return getDataSource(dbSchemaName, topologyProcessorDbUsername, Optional.ofNullable(
                !Strings.isEmpty(topologyProcessorDbPassword) ? topologyProcessorDbPassword : null));
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }
}

