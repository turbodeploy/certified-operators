package com.vmturbo.sql.utils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;

/**
 * Class that maintains properties that define a {@link DbEndpoint}.
 *
 * <p>See {@link DbEndpoint} for a detailed description of all the properties.</p>
 */
public class DbEndpointConfig {

    private String tag;
    private SQLDialect dialect;

    private String dbHost;
    private Integer dbPort;
    private String dbDatabaseName;
    private String dbSchemaName;
    private String dbUserName;
    private String dbPassword;
    private DbEndpointAccess dbAccess;
    private String dbRootUserName;
    private String dbRootPassword;
    private Map<String, String> dbDriverProperties;
    private Boolean dbSecure;
    private String dbMigrationLocations;
    private FlywayCallback[] dbFlywayCallbacks;
    private Boolean dbDestructiveProvisioningEnabled;
    private Boolean dbEndpointEnabled;
    private DbEndpoint template;
    private String dbNameSuffix;

    // By default, wait for 30 minutes for context to finish initializing.
    private long maxAwaitCompletionMs = TimeUnit.MINUTES.toMillis(30);

    public String getTag() {
        return tag;
    }

    public void setTag(final String tag) {
        this.tag = tag;
    }

    /**
     * Set the maximum time to wait for completion.
     *
     * @param maxAwaitCompletion The time to wait for completion.
     * @param timeUnit The time unit.
     */
    public void setMaxAwaitCompletion(long maxAwaitCompletion, TimeUnit timeUnit) {
        this.maxAwaitCompletionMs = timeUnit.toMillis(maxAwaitCompletion);
    }

    /**
     * Get the maximum time to wait for completion, in millis.
     *
     * @return The time, in millis.
     */
    public long getMaxAwaitCompletionMs() {
        return maxAwaitCompletionMs;
    }

    public SQLDialect getDialect() {
        return dialect;
    }

    public void setDialect(final SQLDialect dialect) {
        this.dialect = dialect;
    }

    public String getDbHost() {
        return dbHost;
    }

    public void setDbHost(final String dbHost) {
        this.dbHost = dbHost;
    }

    public Integer getDbPort() {
        return dbPort;
    }

    public void setDbPort(final Integer dbPort) {
        this.dbPort = dbPort;
    }

    public String getDbDatabaseName() {
        return dbDatabaseName;
    }

    public void setDbDatabaseName(final String dbDatabaseName) {
        this.dbDatabaseName = dbDatabaseName;
    }

    public String getDbSchemaName() {
        return dbSchemaName;
    }

    public void setDbSchemaName(final String dbSchemaName) {
        this.dbSchemaName = dbSchemaName;
    }

    public String getDbUserName() {
        return dbUserName;
    }

    public void setDbUserName(final String dbUserName) {
        this.dbUserName = dbUserName;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(final String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public DbEndpointAccess getDbAccess() {
        return dbAccess;
    }

    public void setDbAccess(final DbEndpointAccess dbAccess) {
        this.dbAccess = dbAccess;
    }

    public String getDbRootUserName() {
        return dbRootUserName;
    }

    public void setDbRootUserName(final String dbRootUserName) {
        this.dbRootUserName = dbRootUserName;
    }

    public String getDbRootPassword() {
        return dbRootPassword;
    }

    public void setDbRootPassword(final String dbRootPassword) {
        this.dbRootPassword = dbRootPassword;
    }

    public Map<String, String> getDbDriverProperties() {
        return dbDriverProperties;
    }

    public void setDbDriverProperties(final Map<String, String> dbDriverProperties) {
        this.dbDriverProperties = dbDriverProperties;
    }

    public Boolean getDbSecure() {
        return dbSecure;
    }

    public void setDbSecure(final Boolean dbSecure) {
        this.dbSecure = dbSecure;
    }

    public String getDbMigrationLocations() {
        return dbMigrationLocations;
    }

    public void setDbMigrationLocations(final String dbMigrationLocations) {
        this.dbMigrationLocations = dbMigrationLocations;
    }

    public FlywayCallback[] getDbFlywayCallbacks() {
        return dbFlywayCallbacks;
    }

    public void setDbFlywayCallbacks(final FlywayCallback[] dbFlywayCallbacks) {
        this.dbFlywayCallbacks = dbFlywayCallbacks;
    }

    public Boolean getDbDestructiveProvisioningEnabled() {
        return dbDestructiveProvisioningEnabled;
    }

    public void setDbDestructiveProvisioningEnabled(final Boolean dbDestructiveProvisioningEnabled) {
        this.dbDestructiveProvisioningEnabled = dbDestructiveProvisioningEnabled;
    }

    public Boolean getDbEndpointEnabled() {
        return dbEndpointEnabled;
    }

    public void setDbEndpointEnabled(final Boolean dbEndpointEnabled) {
        this.dbEndpointEnabled = dbEndpointEnabled;
    }

    public DbEndpoint getTemplate() {
        return template;
    }

    public void setTemplate(final DbEndpoint template) {
        this.template = template;
    }

    public String getDbNameSuffix() {
        return dbNameSuffix;
    }

    public void setDbNameSuffix(final String dbNameSuffix) {
        this.dbNameSuffix = dbNameSuffix;
    }
}
