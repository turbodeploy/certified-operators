package com.vmturbo.history.stats;

import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Properties;

import org.apache.http.auth.UsernamePasswordCredentials;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.Ordered;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.sql.utils.SQLDatabaseConfig.SQLConfigObject;

/**
 * Configuration for HistorydbIO for testing.
 *
 * Does not rely on a connection to AUTH component for fetching the root password.
 *
 * In order to generate a new DB name, each test that uses this config must be marked:
 * @DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
 **/
@Configuration
public class DbTestConfig {

    public static final String LOCALHOST = "localhost";
    public static final String MYSQL = "mysql";
    @Value("${dbSchemaName}")
    private String testDbName;

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertiesResolver() {
        final PropertySourcesPlaceholderConfigurer propertiesConfigurer
                = new PropertySourcesPlaceholderConfigurer();

        Properties properties = new Properties();
        properties.setProperty("dbSchemaName", "vmt_testdb_" + System.nanoTime());
        properties.setProperty("adapter", MYSQL);
        properties.setProperty("hostName", LOCALHOST);
        properties.setProperty("defaultBatchSize", "1000");
        properties.setProperty("maxBatchRetries", "10");
        properties.setProperty("maxBatchRetryTimeoutMsec", "60000");
        // there is a USERNAME system variable in Windows
        properties.setProperty("userName", "vmtplatform");

        propertiesConfigurer.setProperties(properties);
        // take precedence over env
        propertiesConfigurer.setLocalOverride(true);
        return propertiesConfigurer;
    }

    @Bean
    public HistorydbIO historydbIO() {
        // always return the default DB password for this test
        DBPasswordUtil dbPasswordUtilMock = Mockito.mock(DBPasswordUtil.class);
        when(dbPasswordUtilMock.getSqlDbRootPassword()).thenReturn(DBPasswordUtil.obtainDefaultPW());
        final SQLConfigObject sqlConfigObject = new SQLConfigObject(LOCALHOST, 3306,
            Optional.of(new UsernamePasswordCredentials("root", "vmturbo")),
            MYSQL, getRootConnectionUrl(), false);
        return new HistorydbIO(dbPasswordUtilMock, sqlConfigObject);
    }

    private String getRootConnectionUrl() {
        return "jdbc:" + MYSQL
            + "://"
            + LOCALHOST
            + ":"
            + "3306";
    }
    @Bean
    public String testDbName() {
        return testDbName;
    }
}
