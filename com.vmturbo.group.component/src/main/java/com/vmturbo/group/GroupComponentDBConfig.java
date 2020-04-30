package com.vmturbo.group;

import javax.sql.DataSource;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for group component interaction with a schema.
 */
@Configuration
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
        // If no db password specified, use root password by default.
        DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort, authRoute,
            authRetryDelaySecs);
        String dbPassword = !Strings.isEmpty(groupComponentDbPassword) ?
            groupComponentDbPassword : dbPasswordUtil.getSqlDbRootPassword();
        return dataSourceConfig(dbSchemaName, groupComponentDbUsername, dbPassword);
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
    }
}
