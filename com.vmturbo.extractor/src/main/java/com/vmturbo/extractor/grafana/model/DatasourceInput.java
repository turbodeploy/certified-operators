package com.vmturbo.extractor.grafana.model;

import javax.annotation.Nonnull;

import com.google.gson.annotations.SerializedName;

import org.jooq.SQLDialect;

import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointConfig;

/**
 * See: https://grafana.com/docs/grafana/latest/http_api/data_source/.
 *
 * <p/>We only support the "postgres" datasource for now.
 */
public class DatasourceInput {

    @SerializedName("name")
    private String displayName;

    @SerializedName("url")
    private String url;

    @SerializedName("database")
    private String database;

    @SerializedName("user")
    private String user;

    @SerializedName("orgId")
    private long orgId = 1;

    @SerializedName("type")
    private String datasourceType;

    @SerializedName("basicAuth")
    private boolean basicAuth = false;

    @SerializedName("basicAuthUser")
    private String basicAuthUser = "";

    @SerializedName("basicAuthPassword")
    private String basicAuthPassword = "";

    @SerializedName("withCredentials")
    private boolean withCredentials = false;

    @SerializedName("access")
    private String access = "proxy";

    @SerializedName("isDefault")
    private boolean isDefault = true;

    // TODO (roman, June 5 2020: We can have the jsonData be a templated field, and plug
    // datasource-specific DTOs. Right now we only care about postgres.
    @SerializedName("jsonData")
    private PostgresJsonData jsonData = new PostgresJsonData();

    @SerializedName("secureJsonData")
    private SecureData secureJsonData = new SecureData();

    /**
     * Create a new {@link DatasourceInput} which will configure a connection to the provided
     * database endpoint.
     *
     * @param displayName The display name for the datasource.
     * @param endpointConfig The {@link DbEndpoint}.
     * @return A {@link DatasourceInput} that can be used to create a Grafana datasource pointing
     *         to this endpoint.
     * @throws InterruptedException If the endpoint is not ready (this will be removed).
     * @throws UnsupportedDialectException If the type of endpoint is unsupported.
     */
    @Nonnull
    public static DatasourceInput fromDbEndpoint(String displayName, DbEndpointConfig endpointConfig)
            throws UnsupportedDialectException {
        final DatasourceInput input = new DatasourceInput();
        input.displayName = displayName;
        input.database = endpointConfig.getDbDatabaseName();
        input.user = endpointConfig.getDbUserName();
        // No scheme.
        input.url = endpointConfig.getDbHost() + ":" + endpointConfig.getDbPort();
        if (endpointConfig.getDialect() == SQLDialect.POSTGRES) {
            input.datasourceType = "postgres";
        } else {
            throw new UnsupportedDialectException(endpointConfig.getDialect());
        }
        input.secureJsonData.password = endpointConfig.getDbPassword();
        return input;
    }

    /**
     * Return the display name of the data source.
     *
     * @return The display name.
     */
    @Nonnull
    public String getDisplayName() {
        return displayName;
    }

    /**
     * Container for the "secure" data - basically passwords.
     */
    public static class SecureData {
        @SerializedName("password")
        private String password = "vmturbo";
    }

    /**
     * Postgres specific fields.
     */
    public static class PostgresJsonData {
        @SerializedName("sslmode")
        private String ssl = "disable";

        @SerializedName("postgresVersion")
        private int postgresVersion = 1200;

        @SerializedName("timescaledb")
        private boolean timescale = true;
    }

    /**
     * Return whether or not this {@link DatasourceInput} is equivalent to another one.
     * Equivalent does not mean equal. Equivalent means "pointing at the same data".
     * If this input is equivalent to another one, we need to issue an "update" request instead
     * of a "create" one.
     *
     * @param input The other {@link DatasourceInput}.
     * @return True if the inputs are equivalent, false otherwise.
     */
    public boolean matchesInput(DatasourceInput input) {
        // Grafana doesn't allow two datasources with the same display name, so a display name
        // equality overrides any other equalities.
        final boolean roughMatch = this.displayName.equals(input.getDisplayName())
                || (this.database.equals(input.database)
                && this.url.equals(input.url)
                && this.user.equals(input.user));
        return roughMatch;
    }
}
