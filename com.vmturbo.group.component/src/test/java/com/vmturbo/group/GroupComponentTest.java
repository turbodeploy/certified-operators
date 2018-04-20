package com.vmturbo.group;

import com.arangodb.ArangoDB;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

public class GroupComponentTest {

    @Rule
    public TestName testName = new TestName();

    private IntegrationTestServer server;

    @Before
    public void startup() throws Exception {
        server = new IntegrationTestServer(testName, TestSQLDatabaseConfig.class);
    }

    @After
    public void cleanup() throws Exception {
        server.close();
    }

    @Test
    public void contextLoads() {
        // Intentionally empty. This test is to make sure the context is created properly.
    }

    @Configuration
    public static class ArangoDatabaseFactoryConfig {

        @Bean
        public ArangoDriverFactory arangoDriverFactory() {
            // mock the arangoDB instance that is returned from the factory
            // this config overrides the production one
            // note:
            // we cannot use @Before annotation here because the @PostConstruct is running before it
            return () -> {
                final ArangoDB arangoDB = Mockito.mock(ArangoDB.class);
                return arangoDB;
            };
        }
    }
}
