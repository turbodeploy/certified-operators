package com.vmturbo.sql.utils;

import java.sql.SQLException;
import java.time.Duration;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
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

import com.vmturbo.auth.api.db.DBPasswordUtil;
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

    @Value("${dbUsername}")
    private String dbUsername;

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
        DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort,
            authRetryDelaySecs);
        try {
            dataSource.setUrl(getDbUrl());
            dataSource.setUser(dbUsername);
            dataSource.setPassword(dbPasswordUtil.getSqlDbRootPassword());
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

    @Bean
    public Flyway flyway() {
        return new FlywayMigrator(Duration.ofMinutes(1),
                Duration.ofSeconds(5),
                dbSchemaName,
                dataSource()
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
    protected String getDbUrl() {
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
}

