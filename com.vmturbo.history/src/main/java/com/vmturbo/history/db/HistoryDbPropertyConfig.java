package com.vmturbo.history.db;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.sql.utils.DbPropertyProvider;

/**
 * Bean to provide otherwise inaccessible default property values without triggering DB
 * provisioning.
 */
@Configuration
public class HistoryDbPropertyConfig implements DbPropertyProvider {

    @Value("${historyDbUsername:history}")
    private String historyDbUsername;

    @Value("${historyDbPassword:}")
    private String historyDbPassword;

    @Value("${dbSchemaName:vmtdb}")
    private String dbSchemaName;

    @Override
    public String getDatabaseName() {
        return dbSchemaName;
    }

    @Override
    public String getSchemaName() {
        return dbSchemaName;
    }

    @Override
    public String getUserName() {
        return historyDbUsername;
    }

    @Override
    public String getPassword() {
        return historyDbPassword;
    }
}
