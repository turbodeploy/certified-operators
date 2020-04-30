package com.vmturbo.cost.component;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for cost component interaction with a schema.
 */
@Configuration
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
        // If no db password specified, use root password by default.
        DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort, authRoute,
            authRetryDelaySecs);
        String dbPassword = !Strings.isEmpty(costDbPassword) ?
            costDbPassword : dbPasswordUtil.getSqlDbRootPassword();
        return dataSourceConfig(dbSchemaName, costDbUsername, dbPassword);
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }
}
