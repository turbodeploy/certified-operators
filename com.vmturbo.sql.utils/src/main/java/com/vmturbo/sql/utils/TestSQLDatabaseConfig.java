package com.vmturbo.sql.utils;

import java.sql.SQLException;
import java.time.Instant;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Test configuration for interacting with the DB.
 *
 * The name of the target schema requires setting a property in the test,
 * or the tests' configuration.
 *
 * --- IMPORTANT ---
 * The configuration will be configured for a "*_test" database, so that it does override any
 * existing databases in the local DB.  However, the input property should be the ORIGINAL
 * schema name used for JOOQ code generation.
 *
 * To set the property on the test directly, use the TestPropertySource annotation:
 *
 * @RunWith(...)
 * @ContextConfiguration(...)
 * @TestPropertySource(properties = {"originalSchemaName=name"})
 * public class TheTest { }
 *
 * To set the property in a configuration that imports {@link TestSQLDatabaseConfig}, create
 * a bean that defines the property:
 *
 * <code>
 *   @Bean
 *   public static PropertySourcesPlaceholderConfigurer properties() {
 *       final PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
 *       final Properties properties = new Properties();
 *       properties.setProperty("originalSchemaName", "name");
 *       pspc.setProperties(properties);
 *       return pspc;
 *    }
 * </code>
 *
 * To actually use the database in the test:
 *
 * <code>
 *     @Autowired
 *     private TestSQLDatabaseConfig dbConfig;
 *
 *     @Before
 *     public void setup() {
 *         dbConfig.prepareDatabase();
 *         ...
 *     }
 *
 *     @After
 *     public void teardown() {
 *         dbConfig.clean();
 *     }
 * </code>
 */
@Configuration
@EnableTransactionManagement
public class TestSQLDatabaseConfig {

    @Value("${originalSchemaName}")
    private String originalSchemaName;

    /**
     * Call this method in a @Before method to prepare the database.
     *
     * @return The {@link DSLContext} to use for jOOQ operations.
     */
    @Nonnull
    public DSLContext prepareDatabase() {
        // Clean the database and bring it up to the production configuration before running test
        clean();
        flyway().migrate();

        return dsl();
    }

    /**
     * Call this method in a @After method to clean the database after the test runs.
     */
    public void clean() {
        flyway().clean();
    }

    @Bean
    @Primary
    public DataSource dataSource() {
        final MariaDbDataSource dataSource = new MariaDbDataSource();
        try {
            dataSource.setUrl(getDbUrl());
            dataSource.setUser("root");
            dataSource.setPassword("vmturbo");
            return dataSource;
        } catch (SQLException e) {
            throw new BeanCreationException("Failed to initialize bean: " + e.getMessage()) ;
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
        jooqConfiguration.set(new DefaultExecuteListenerProvider(exceptionTranslator()));
        // Mapping the original schema name to the test schema name, so that code generated
        // for the original schema produces SQL targetted at the test schema.
        // See: https://www.jooq.org/doc/3.10/manual/sql-building/dsl-context/custom-settings/settings-render-mapping/
        jooqConfiguration.set(new Settings()
            .withRenderMapping(new RenderMapping()
                    .withSchemata(new MappedSchema()
                            .withInput(originalSchemaName)
                            .withOutput(testSchemaName()))));

        jooqConfiguration.set(SQLDialect.MARIADB);

        return jooqConfiguration;
    }

    @Bean
    public Flyway flyway() {
        Flyway flyway = new Flyway();
        flyway.setSchemas(testSchemaName());
        flyway.setDataSource(dataSource());

        return flyway;
    }

    @Bean
    public DSLContext dsl() {
        return new DefaultDSLContext(configuration());
    }


    /**
     * Convert an original schema name to the test schema name to use.
     *
     * @return The test schema name.
     */
    @Bean
    protected String testSchemaName() {
        return String.join("_", originalSchemaName, "test", String.valueOf(Instant.now().toEpochMilli()));
    }

    @Nonnull
    protected String getDbUrl() {
        return UriComponentsBuilder.newInstance()
            .scheme("jdbc:mysql")
            .host("localhost")
            .port(3306)
            .build()
            .toUriString();
    }
}
