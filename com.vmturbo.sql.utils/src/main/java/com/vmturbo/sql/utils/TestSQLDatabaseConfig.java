package com.vmturbo.sql.utils;

import javax.annotation.Nonnull;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

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

    @Value("mariadbDriverProperties")
    private String mariadbDriverProperties;

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
    public Flyway flyway() {
        return dbConfiguration().getFlyway();
    }

    @Bean
    public DSLContext dsl() {
        return dbConfiguration().getDslContext();
    }

    @Bean
    public TestDbConfiguration dbConfiguration() {
        return new TestDbConfiguration(originalSchemaName, mariadbDriverProperties);
    }

}
