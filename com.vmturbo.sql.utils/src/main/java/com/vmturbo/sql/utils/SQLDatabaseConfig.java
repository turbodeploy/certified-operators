package com.vmturbo.sql.utils;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.sql.DataSource;

import com.google.common.annotations.VisibleForTesting;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.callback.FlywayCallback;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderNameStyle;
import org.jooq.conf.Settings;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.mariadb.jdbc.MariaDbDataSource;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.components.common.utils.EnvironmentUtils;

/**
 * Configuration for interaction with database.
 *
 * Components that want to connect to the database should import this configuration into their
 * Spring context with an @Import annotation (please do not @ComponentScan the sql.utils package!).
 */
@Configuration
@EnableTransactionManagement
public class SQLDatabaseConfig {
    private static final String ENABLE_SECURE_DB_CONNECTION = "enableSecureDBConnection" ;

    private final boolean isSecureDBConnectionRequested =
        EnvironmentUtils.parseBooleanFromEnv(ENABLE_SECURE_DB_CONNECTION);

    @Value("${dbHost}")
    private String dbHost;

    @Value("${dbPort}")
    private int dbPort;

    @Value("${dbUsername:root}")
    private String dbUsername;

    @Value("${dbUserPassword:vmturbo}")
    private String dbUserPassword;

    @Value("${dbSchemaName}")
    private String dbSchemaName;

    @Value("${sqlDialect}")
    private String sqlDialectName;

    @Value("${authHost}")
    public String authHost;

    @Value("${serverHttpPort}")
    public int authPort;

    @Value("${authRetryDelaySecs}")
    public int authRetryDelaySecs;

    private static final Logger logger = LogManager.getLogger();

    @Bean
    @Primary
    public DataSource dataSource() {
        MariaDbDataSource dataSource = new MariaDbDataSource();
        try {
            dataSource.setUrl(getDbUrl());
            dataSource.setUser(dbUsername);
            dataSource.setPassword(dbUserPassword);
            return dataSource;
        } catch (SQLException e) {
            throw new BeanCreationException("Failed to initialize bean: " + e.getMessage());
        }
    }

    @Bean
    public LazyConnectionDataSourceProxy lazyConnectionDataSource() {
        return new LazyConnectionDataSourceProxy(dataSource());
    }

    @Bean
    public TransactionAwareDataSourceProxy transactionAwareDataSource() {
        return new TransactionAwareDataSourceProxy(lazyConnectionDataSource());
    }

    @Bean
    public DataSourceTransactionManager transactionManager() {
        return new DataSourceTransactionManager(lazyConnectionDataSource());
    }

    @Bean
    public DataSourceConnectionProvider connectionProvider() {
        return new DataSourceConnectionProvider(transactionAwareDataSource());
    }

    @Bean
    public JooqExceptionTranslator exceptionTranslator() {
        return new JooqExceptionTranslator();
    }

    @Bean
    public DefaultConfiguration configuration() {
        DefaultConfiguration jooqConfiguration = new DefaultConfiguration();

        jooqConfiguration.set(connectionProvider());
        jooqConfiguration.set(new Settings().withRenderNameStyle(RenderNameStyle.LOWER));
        jooqConfiguration.set(new DefaultExecuteListenerProvider(exceptionTranslator()));

        SQLDialect dialect = SQLDialect.valueOf(sqlDialectName);
        jooqConfiguration.set(dialect);

        return jooqConfiguration;
    }

    /**
     * Callbacks to be configured for our Flyway migrations.
     *
     * <p>These can be used to handle issues such as problematic migrations that have been released
     * to customers and thus cannot generally be either replaced or removed from the migration
     * sequence.</p>
     *
     * <p>A component should define a {@link Primary} bean elsewhere in order to override the
     * empty default.</p>
     *
     * @return array of callback objects, in order in which they should be invoked
     */
    @Bean
    public FlywayCallback[] flywayCallbacks() {
        return new FlywayCallback[0];
    }

    @Bean
    public Flyway flyway() {
        return new FlywayMigrator(Duration.ofMinutes(1),
            Duration.ofSeconds(5),
            dbSchemaName,
            dataSource(),
            flywayCallbacks()
        ).migrate();
    }

    @Bean
    public DSLContext dsl() {
        flyway(); // Force initialization of flyway before getting a reference to the database
        return new DefaultDSLContext(configuration());
    }

    /**
     * Returns database connection URL. If "enableSecureDBConnection" environment variable is set to
     * true, connection URL includes "?useSSL=true&trustServerCertificate=true".
     * TODO (Gary Zeng, Aug 20, 2019) remove parameter "trustServerCertificate=true".
     *
     * @return DB connection URL
     */
    @Nonnull
    @VisibleForTesting
    String getDbUrl() {
        final UriComponentsBuilder urlBuilder = UriComponentsBuilder.newInstance()
            .scheme("jdbc:mysql")
            .host(dbHost)
            .port(dbPort);
        if (isSecureDBConnectionRequested) {
            logger.info("Enabling secure DB connection with host: {}, port: {}", dbHost, dbPort);
        }
        return isSecureDBConnectionRequested ? urlBuilder
            .queryParam("useSSL", "true")
            .queryParam("trustServerCertificate", "true")
            .build().toUriString() : urlBuilder.build().toUriString();
    }

    /**
     * Get DTO with all the SQL connection parameters.
     *
     * @return {@link SQLConfigObject}
     */
    @Bean
    public SQLConfigObject getSQLConfigObject() {
        final Optional<UsernamePasswordCredentials> credentials = (dbUsername != null && dbUserPassword != null) ?
            Optional.ofNullable(new UsernamePasswordCredentials(dbUsername, dbUserPassword)) : Optional.empty();
        return new SQLConfigObject(dbHost, dbPort, credentials, sqlDialectName, getDbUrl(), isSecureDBConnectionRequested);
    }

    /**
     * A value object contains all the SQL connection parameters.
     */
    @Immutable
    public static class SQLConfigObject {
        private final String dbUrl;
        private final String dbHost;
        private final int dbPort;
        private final String sqlDialect;
        private final boolean isSecureDBConnectionRequested;

        private final Optional<UsernamePasswordCredentials> credentials;


        public SQLConfigObject(@Nonnull final String dbHost,
                               @Nonnull final int dbPort,
                               @Nonnull final Optional<UsernamePasswordCredentials> credentials,
                               @Nonnull final String sqlDialect,
                               @Nonnull final String dbUrl,
                               final boolean isSecureDBConnectionRequested) {
            this.dbHost = dbHost;
            this.dbPort = dbPort;
            this.credentials = credentials;
            this.sqlDialect = sqlDialect;
            this.dbUrl = dbUrl;
            this.isSecureDBConnectionRequested = isSecureDBConnectionRequested;
        }

        @Nonnull
        public String getDbUrl() {
            return dbUrl;
        }

        @Nonnull
        public String getDbHost() {
            return dbHost;
        }

        public int getDbPort() {
            return dbPort;
        }

        @Nonnull
        public Optional<UsernamePasswordCredentials> getCredentials() {
            return credentials;
        }

        @Nonnull
        public String getSqlDialect() {
            return sqlDialect;
        }

        public boolean isSecureDBConnectionRequested() {
            return isSecureDBConnectionRequested;
        }
    }
}

