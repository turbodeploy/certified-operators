package com.vmturbo.kibitzer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SQLDialect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.vmturbo.kibitzer.activities.KibitzerActivity;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointAccess;
import com.vmturbo.sql.utils.DbEndpointBuilder;
import com.vmturbo.sql.utils.DbEndpointsConfig;
import com.vmturbo.sql.utils.DbPropertyProvider;

/**
 * Class that provides database access to the attached component's primary database. Access is
 * provided using {@link DbEndpoint} regardless of the approach used in the attached component.
 *
 * <p>This class can be used to obtain access to the component's production database or it can
 * provision and utilize a copy of that database. In the latter case, the schema will be complete,
 * but the database will initially be empty except for reference-data records added by
 * migrations.</p>
 */
@Configuration
public class KibitzerDb extends DbEndpointsConfig {
    private static final Logger logger = LogManager.getLogger();
    private static final String KIBITZER_DB_PREFIX = "kibitzer_";

    @Autowired
    private Environment env;

    @Value("${sqlDialect}")
    private SQLDialect dialect;

    @Value("${dbAutoprovision:false}")
    boolean dbAutoprovision;

    private final Map<String, DbEndpoint> endpoints = Collections.synchronizedMap(new HashMap<>());

    /**
     * Get database access required for a given activity.
     *
     * @param activity the activity that needs the database
     * @param context  Spring application context
     * @return A {@link DbEndpoint} instance that can provide the needed access
     */
    public Optional<DbEndpoint> getDatabase(KibitzerActivity<?, ?> activity,
            ApplicationContext context) {
        return getDatabase(activity.getComponent().info(), activity.getTag(),
                activity.getDbMode(), context);
    }

    /**
     * Get database access other than the primary access configured for the activity.
     *
     * <p>The other overload of this method has a simpler signature but will only create the
     * primary endpoint for the activity.</p>
     *
     * @param componentInfo info for the target component
     * @param tag           {@link KibitzerActivity tab}
     * @param dbMode        {@link KibitzerActivity} db mode
     * @param context       Spring application context
     * @return new endpoint
     */
    public Optional<DbEndpoint> getDatabase(ComponentInfo componentInfo, String tag,
            DbMode dbMode, ApplicationContext context) {
        String key = getEndpointKey(componentInfo, tag, dbMode);
        if (!endpoints.containsKey(key)) {
            endpoints.put(key, createEndpoint(componentInfo, tag, dbMode, context));
        }
        return Optional.ofNullable((endpoints.get(key)));
    }

    /**
     * Clean up when an activity is done with an endpoint. The endpoint is removed from the cache,
     * this is the last activity using it. And in the case of an activity whose db mode is `COPY`,
     * the database is dropped.
     *
     * @param activity activity that is done with the endpoint
     */
    public void onActivityComplete(KibitzerActivity<?, ?> activity) {
        if (activity.getDbMode() == DbMode.COPY) {
            String key = getEndpointKey(activity.getComponent().info(), activity.getTag(),
                    activity.getDbMode());
            DbEndpoint endpoint = endpoints.get(key);
            if (endpoint != null) {
                try {
                    endpoints.remove(key);
                    if (endpoint.getConfig().getShouldProvisionDatabase() && dbAutoprovision) {
                        endpoint.getAdapter().tearDown();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private String getEndpointKey(ComponentInfo componentInfo, String tag, DbMode dbMode) {
        String key = componentInfo.getName() + "_kibitzer_db";
        if (isCopyMode(dbMode)) {
            key += "_" + tag;
        }
        return key;
    }

    private boolean isCopyMode(DbMode dbMode) {
        return dbMode == DbMode.COPY || dbMode == DbMode.RETAINED_COPY;
    }

    private DbEndpoint createEndpoint(ComponentInfo componentInfo, String tag, DbMode dbMode,
            ApplicationContext context) {
        if (dbMode == DbMode.NONE) {
            return null;
        } else {
            boolean copyMode = isCopyMode(dbMode);
            String prefix = copyMode ? KIBITZER_DB_PREFIX : "";
            String endpointName = String.join(".", "dbs", componentInfo.getName(), tag);
            DbPropertyProvider dbPropertyProvider = componentInfo.getDbPropertyProvider(context);
            DbEndpointBuilder builder = dbEndpoint(endpointName, dialect)
                    .withDatabaseName(prefix + dbPropertyProvider.getDatabaseName())
                    .withSchemaName(prefix + dbPropertyProvider.getSchemaName())
                    .withUserName(dbPropertyProvider.getUserName())
                    .withPassword(dbPropertyProvider.getPassword())
                    .withShouldProvision(copyMode && dbAutoprovision)
                    .withRootAccessEnabled(copyMode && dbAutoprovision)
                    .withAccess(DbEndpointAccess.ALL)
                    .withUseConnectionPool(false);
            return fixEndpointForMultiDb(builder, componentInfo.getName()).build();
        }
    }

    /**
     * Enum with database "modes" that can be used by activities.
     */
    public enum DbMode {
        /** no database access required. */
        NONE,
        /** direct access to the running component's database is required (use with care!). */
        COMPONENT,
        /**
         * a copy of the component's primary database is created with an activity-specific name, and
         * it is dropped when the last using activity completes.
         */
        COPY,
        /**
         * Like COPY, but the database is not dropped. This means that successive executions of this
         * activity in the separate {@link Kibitzer} pods would end up using the same database.
         */
        RETAINED_COPY
    }
}
