package com.vmturbo.sql.utils;

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
import org.mariadb.jdbc.MySQLDataSource;
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

/**
 * Configuration for interaction with database.
 *
 * Components that want to connect to the database should import this configuration into their
 * Spring context with an @Import annotation (please do not @ComponentScan the sql.utils package!).
 */
@Configuration
@EnableTransactionManagement
public class SQLDatabaseConfig {
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
        MySQLDataSource dataSource = new MySQLDataSource();
        DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort,
                authRetryDelaySecs);

        dataSource.setUrl(getDbUrl());
        dataSource.setUser(dbUsername);
        dataSource.setPassword(dbPasswordUtil.getSqlDbRootPassword());

        return dataSource;
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
     * Returns database connection URL.
     *
     * @return DB connection URL
     */
    @Nonnull
    protected String getDbUrl() {
        return UriComponentsBuilder.newInstance()
                .scheme("jdbc:mysql")
                .host(dbHost)
                .port(dbPort)
                .build()
                .toUriString();
    }
}

