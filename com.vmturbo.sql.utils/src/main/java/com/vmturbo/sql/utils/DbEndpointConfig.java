package com.vmturbo.sql.utils;

import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;

import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Class that maintains properties that define a {@link DbEndpoint}.
 *
 * <p>See {@link DbEndpoint} for a detailed description of all the properties.</p>
 */
public class DbEndpointConfig {
    private final String name;
    private SQLDialect dialect;

    private String host;
    private Integer port;
    private String databaseName;
    private String schemaName;
    private String userName;
    private String password;
    private DbEndpointAccess access;
    private String rootUserName;
    private String rootPassword;
    private Boolean rootAccessEnabled;
    private Map<String, String> driverProperties;
    private Boolean secure;
    private String migrationLocations;
    private FlywayCallback[] flywayCallbacks;
    private Boolean endpointEnabled;
    private Function<UnaryOperator<String>, Boolean> endpointEnabledFn;
    private DbEndpoint template;
    private String provisioningSuffix;
    private Boolean shouldProvisionDatabase;
    private Boolean shouldProvisionUser;
    private boolean isAbstract;

    DbEndpointConfig(@Nonnull final String name) {
        this.name = name;
    }

    /**
     * Get the tag associated with this endpoint.
     *
     * @return The tag.
     */
    public String getName() {
        return name;
    }

    public SQLDialect getDialect() {
        return dialect;
    }

    public void setDialect(final SQLDialect dialect) {
        this.dialect = dialect;
    }

    public String getHost() {
        return host;
    }

    public void setHost(final String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(final Integer port) {
        this.port = port;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(final String databaseName) {
        this.databaseName = databaseName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(final String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(final String password) {
        this.password = password;
    }

    public DbEndpointAccess getAccess() {
        return access;
    }

    public void setAccess(final DbEndpointAccess access) {
        this.access = access;
    }

    public String getRootUserName() {
        return rootUserName;
    }

    public void setRootUserName(final String rootUserName) {
        this.rootUserName = rootUserName;
    }

    public Boolean isRootAccessEnabled() { return rootAccessEnabled; }

    public void setRootAccessEnabled(final boolean rootAccessEnabled) {
        this.rootAccessEnabled = rootAccessEnabled;
    }

    public String getRootPassword() {
        return rootPassword;
    }

    public void setRootPassword(final String rootPassword) {
        this.rootPassword = rootPassword;
    }

    public Map<String, String> getDriverProperties() {
        return driverProperties;
    }

    public void setDriverProperties(final Map<String, String> driverProperties) {
        this.driverProperties = driverProperties;
    }

    public Boolean getSecure() {
        return secure;
    }

    public void setSecure(final Boolean secure) {
        this.secure = secure;
    }

    public String getMigrationLocations() {
        return migrationLocations;
    }

    public void setMigrationLocations(final String migrationLocations) {
        this.migrationLocations = migrationLocations;
    }

    public FlywayCallback[] getFlywayCallbacks() {
        return flywayCallbacks;
    }

    public void setFlywayCallbacks(final FlywayCallback[] flywayCallbacks) {
        this.flywayCallbacks = flywayCallbacks;
    }

    public Boolean getEndpointEnabled() {
        return endpointEnabled;
    }

    public void setEndpointEnabled(final boolean endpointEnabled) {
        this.endpointEnabled = endpointEnabled;
    }

    public Function<UnaryOperator<String>, Boolean> getEndpointEnabledFn() {
        return endpointEnabledFn;
    }

    public void setEndpointEnabledFn(final Function<UnaryOperator<String>, Boolean> endpointEnabledFn) {
        this.endpointEnabledFn = endpointEnabledFn;
    }

    public DbEndpoint getTemplate() {
        return template;
    }

    public void setTemplate(final DbEndpoint template) {
        this.template = template;
    }

    public String getProvisioningSuffix() {
        return provisioningSuffix;
    }

    public void setProvisioningSuffix(final String provisioningSuffix) {
        this.provisioningSuffix = provisioningSuffix;
    }

    /**
     * Check whether this is an abstract endpoint.
     *
     * @return true if this is an abstract endpoint
     */
    public boolean isAbstract() {
        return isAbstract;
    }

    /** Mark this endpoint as abstract. */
    public void setAbstract() {
        isAbstract = true;
    }

    public Boolean getShouldProvisionDatabase() {
        return shouldProvisionDatabase;
    }

    public void setShouldProvisionDatabase(final boolean shouldProvisionDatabase) {
        this.shouldProvisionDatabase = shouldProvisionDatabase;
    }

    public Boolean getShouldProvisionUser() {
        return shouldProvisionUser;
    }

    public void setShouldProvisionUser(final Boolean shouldProvisionUser) {
        this.shouldProvisionUser = shouldProvisionUser;
    }

    @Override
    public String toString() {
        String protocol;
        try {
            protocol = DbAdapter.getJdbcProtocol(this);
        } catch (UnsupportedDialectException e) {
            protocol = "?";
        }
        String url = String.format("jdbc:%s://%s:%s/%s", protocol,
                getHost() != null ? getHost() : "?",
                getPort() != null ? getPort() : "?",
                getDatabaseName() != null ? getDatabaseName() : "?");
        return String.format("DbEndpoint[%s; url=%s; user=%s]",
                getName() != null ? getName() : "(unnamed)", url,
                getUserName() != null ? getUserName() : "?");
    }
}
