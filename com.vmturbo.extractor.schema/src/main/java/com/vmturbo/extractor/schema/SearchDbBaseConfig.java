package com.vmturbo.extractor.schema;

import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.SQLDatabaseConfig2;

/**
 * Configuration of DB endpoints needed for extractor component.
 */
@Configuration
@Import(SQLDatabaseConfig2.class)
public class SearchDbBaseConfig {

    private static final String MYSQL_MIGRATION_LOCATION = "db.migration.mysql";
    @Autowired
    private SQLDatabaseConfig2 dbConfig;

    /**
     * Default name of database for extractor database.
     */
    @Value("${dbs.search.databaseName:search}")
    private String searchDatabaseName;

    /**
     * Default user for mysql database.
     */
    @Value("${dbs.search.user:extractor}")
    private String searchDatabaseUser;

    /**
     * Default root user for mysql database.
     */
    @Value("${dbs.search.root.user:root}")
    private String searchDatabaseRootUser;

    /**
     * Default host for mysql database.
     */
    @Value("${searchDbHost:db}")
    private String searchDbHost;

    /**
     * Abstract endpoint to use as base for active endpoints that access the extractor database.
     *
     * @return endpoint bound to extractor database
     */
    @Bean
    public DbEndpoint extractorMySqlDbEndpoint() {
        return dbConfig.abstractDbEndpoint(null, SQLDialect.MYSQL)
                .withDatabaseName(searchDatabaseName)
                .withSchemaName(searchDatabaseName)
                .withRootUserName(searchDatabaseRootUser)
                .withUserName(searchDatabaseUser)
                .withHost(searchDbHost)
                .withMigrationLocations(MYSQL_MIGRATION_LOCATION)
                .build();
    }
}
