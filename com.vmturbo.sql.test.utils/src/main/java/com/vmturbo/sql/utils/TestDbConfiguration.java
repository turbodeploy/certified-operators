package com.vmturbo.sql.utils;

import java.sql.SQLException;
import java.time.Instant;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderMapping;
import org.jooq.conf.Settings;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.mariadb.jdbc.MariaDbDataSource;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Class to perform configuration of the test DB.
 */
public class TestDbConfiguration {

    private final String testSchemaName;
    private final Flyway flyway;
    private final DSLContext dslContext;
    private final String dbUrl;
    private final Configuration configuration;

    /**
     * Constructor.
     *
     * @param originalSchemaName original schema name (which Jooq has generated sources
     *         against)
     * @param mariaDBProperties maria DB connection properties, if any.
     */
    public TestDbConfiguration(@Nonnull String originalSchemaName,
            @Nullable String mariaDBProperties) {
        testSchemaName = String.join("_", originalSchemaName, "test",
                String.valueOf(Instant.now().toEpochMilli()));
        dbUrl = createDbUrl(mariaDBProperties);
        final DataSource dataSource = dataSource(dbUrl);
        flyway = new Flyway();
        flyway.setSchemas(testSchemaName);
        flyway.setDataSource(dataSource);
        final LazyConnectionDataSourceProxy lazyConnectionDataSourceProxy =
                new LazyConnectionDataSourceProxy(dataSource);
        final TransactionAwareDataSourceProxy transactionAwareDataSourceProxy =
                new TransactionAwareDataSourceProxy(lazyConnectionDataSourceProxy);
        final DataSourceConnectionProvider connectionProvider =
                new DataSourceConnectionProvider(transactionAwareDataSourceProxy);
        configuration = createConfiguration(connectionProvider, originalSchemaName, testSchemaName);
        this.dslContext = new DefaultDSLContext(configuration);
    }

    private static DataSource dataSource(@Nonnull String dbUrl) {
        final MariaDbDataSource dataSource = new MariaDbDataSource();
        try {
            dataSource.setUrl(dbUrl);
            dataSource.setUser("root");
            dataSource.setPassword("vmturbo");
            return dataSource;
        } catch (SQLException e) {
            throw new BeanCreationException("Failed to initialize bean: " + e.getMessage());
        }
    }

    @Nonnull
    private static String createDbUrl(@Nullable String mariadbDriverProperties) {
        return UriComponentsBuilder.newInstance()
                .scheme("jdbc:mariadb")
                .host("localhost")
                .port(3306)
                .query(mariadbDriverProperties == null ? "" : mariadbDriverProperties)
                .build()
                .toUriString();
    }

    private static DefaultConfiguration createConfiguration(
            @Nonnull DataSourceConnectionProvider connectionProvider,
            @Nonnull String originalSchemaName, @Nonnull String testSchemaName) {
        DefaultConfiguration jooqConfiguration = new DefaultConfiguration();

        jooqConfiguration.set(connectionProvider);
        jooqConfiguration.set(new DefaultExecuteListenerProvider(new JooqExceptionTranslator()));
        // Mapping the original schema name to the test schema name, so that code generated
        // for the original schema produces SQL targetted at the test schema.
        // See: https://www.jooq.org/doc/3.10/manual/sql-building/dsl-context/custom-settings/settings-render-mapping/
        jooqConfiguration.set(new Settings().withRenderMapping(new RenderMapping().withSchemata(
                new MappedSchema().withInput(originalSchemaName).withOutput(testSchemaName))));

        jooqConfiguration.set(SQLDialect.MARIADB);

        return jooqConfiguration;
    }

    /**
     * Returns schema name that will be used for tests. It is usually based on original schema name
     * but is different in order not to interfere with any existing DBs.
     *
     * @return test schema name
     */
    @Nonnull
    public String getTestSchemaName() {
        return testSchemaName;
    }

    /**
     * Returns Flyway migration.
     *
     * @return flyway migrator
     */
    @Nonnull
    public Flyway getFlyway() {
        return flyway;
    }

    /**
     * Returns Jooq context.
     *
     * @return Jooq context
     */
    @Nonnull
    public DSLContext getDslContext() {
        return dslContext;
    }

    /**
     * Returns DB url.
     *
     * @return db url
     */
    @Nonnull
    public String getDbUrl() {
        return dbUrl;
    }

    /**
     * Returns Jooq configuration to use.
     *
     * @return Jooq configuration
     */
    public Configuration getConfiguration() {
        return configuration;
    }
}
