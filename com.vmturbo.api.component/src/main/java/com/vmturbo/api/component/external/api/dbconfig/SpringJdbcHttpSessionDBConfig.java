package com.vmturbo.api.component.external.api.dbconfig;

import java.util.Optional;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.session.jdbc.config.annotation.web.http.EnableJdbcHttpSession;
import org.springframework.transaction.PlatformTransactionManager;

import com.vmturbo.api.component.security.SpringJdbcHttpSessionCondition;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for API component to use HTTP Session to support clustered sessions with DB.
 */
@Configuration
@Conditional(SpringJdbcHttpSessionCondition.class)
@EnableJdbcHttpSession
@Order(Ordered.HIGHEST_PRECEDENCE)
public class SpringJdbcHttpSessionDBConfig extends SQLDatabaseConfig {
    /**
     * DB user name accessible to given schema.
     */
    @Value("${apiDbUsername:api}")
    private String apiDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${apiDbPassword:}")
    private String apiDbPassword;

    /**
     * DB schema name.
     */
    @Value("${dbSchemaName:api}")
    private String dbSchemaName;

    /**
     * DB schema location.
     */
    @Value("${apiMigrationLocation:db/api/migration}")
    private String apiMigrationLocation;

    /**
     * Initialize api component DB config by running flyway migration and creating a user.
     *
     * @return DataSource of market component DB.
     */
    @Bean
    @Override
    public DataSource dataSource() {
        super.setMigrationLocation(apiMigrationLocation);
        return getDataSource(dbSchemaName, apiDbUsername, Optional.ofNullable(
                !Strings.isEmpty(apiDbPassword) ? apiDbPassword : null));
    }

    /**
     * Get DB schema name.
     * @return schema name
     */
    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }

    /**
     * Get DB user name.
     * @return user name
     */
    @Override
    public String getDbUsername() {
        return apiDbUsername;
    }

    /**
     * Spring HTTP session transaction manager bean.
     * @param dataSource data source object for transaction manager.
     * @return transaction manager
     */
    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}