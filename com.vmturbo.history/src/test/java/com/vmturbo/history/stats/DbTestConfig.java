package com.vmturbo.history.stats;

import static org.mockito.Mockito.when;

import java.util.Properties;

import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.history.db.HistorydbIO;

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

    @Value("${databaseName}")
    private String testDbName;

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertiesResolver() {
        final PropertySourcesPlaceholderConfigurer propertiesConfigureer
                = new PropertySourcesPlaceholderConfigurer();

        Properties properties = new Properties();
        properties.setProperty("databaseName", "vmt_testdb_" + System.nanoTime());
        properties.setProperty("adapter", "mysql");
        properties.setProperty("hostName", "localhost");

        propertiesConfigureer.setProperties(properties);
        return propertiesConfigureer;
    }

    @Bean
    public HistorydbIO historydbIO() {
        // always return the default DB password for this test
        DBPasswordUtil dbPasswordUtilMock = Mockito.mock(DBPasswordUtil.class);
        when(dbPasswordUtilMock.getRootPassword()).thenReturn(DBPasswordUtil.obtainDefaultPW());
        return new HistorydbIO(dbPasswordUtilMock);
    }

    @Bean
    public String testDbName() {
        return testDbName;
    }
}
