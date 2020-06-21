package com.vmturbo.search;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.extractor.schema.ExtractorDbConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.SQLDatabaseConfig2;

/**
 * Configuration of {@link DbEndpoint} needed for component.
 *
 * <p>We only configure {@link DbEndpoint} here.
 * Initialization occurs in ApiComponent.onStartComponent()</p>
 */
@Configuration
@Import({SQLDatabaseConfig2.class, ExtractorDbConfig.class})
public class SearchDBConfig {

    /**
     * For configuring {@link DbEndpoint}.
     */
//    @Autowired
//    private SQLDatabaseConfig2 sqlDatabaseConfig2;

    @Autowired
    private ExtractorDbConfig extractorDbConfig;

    //TODO: These config are used to wire to remote db until development
    //  of search api is complete, remove and remap apiQueryEngine to use
//////////////////////////////////////START//////////////////////////////////////////////
//////////////////////////////////////START//////////////////////////////////////////////
//////////////////////////////////////START//////////////////////////////////////////////
//////////////////////////////////////START//////////////////////////////////////////////
//////////////////////////////////////START//////////////////////////////////////////////

//    @Value("${dbHost:xlr01.eng.turbonomic.com}")
//    private String dbHost;
//
//    @Value("${dbPort:5432}")
//    private String dbPort;
//
//    @Value("${dbDatabaseName:turbodb}")
//    private String dbDatabaseName;
//
//    @Value("${dbSchemaName:turbo-do}")
//    private String dbSchemaName;
//
//    @Value("${dbUserName:turbo-do}")
//    private String dbUserName;
//
//    @Value("${dbPassword:turbo-do}")
//    private String dbPassword;

    /**
     * General R/W endpoint for persisting data from topologies.
     *
     * @return ingestion endpoint
     * @throws UnsupportedDialectException should not happen
     */
//    @Bean
//    public Supplier<DbEndpoint> ingesterEndpoint() throws UnsupportedDialectException {
//        return sqlDatabaseConfig2.primaryDbEndpoint(SQLDialect.POSTGRES)
//                .withDbAccess(DbEndpointAccess.ALL)
//                .withNoDbMigrations()
//                .withDbDestructiveProvisioningEnabled(false)
////                .withDbHost(dbHost)
////                .withDbDatabaseName(dbDatabaseName)
////                .withDbSchemaName(dbSchemaName)
////                .withDbRootUserName(dbUserName)
////                .withDbPassword(dbPassword)
//                .build();
//    }


//    @Bean
//    public ApiQueryEngine apiQueryEngine() throws UnsupportedDialectException {
//        return new ApiQueryEngine(this.ingesterEndpoint());
//    }

//////////////////////////////////////END//////////////////////////////////////////////
//////////////////////////////////////END//////////////////////////////////////////////
//////////////////////////////////////END//////////////////////////////////////////////
//////////////////////////////////////END//////////////////////////////////////////////
//////////////////////////////////////END//////////////////////////////////////////////

    @Bean
    public ApiQueryEngine apiQueryEngine() {
        return new ApiQueryEngine(this.extractorDbConfig.queryEndpoint());
    }

}
