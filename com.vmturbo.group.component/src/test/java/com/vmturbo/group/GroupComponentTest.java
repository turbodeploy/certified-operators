package com.vmturbo.group;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.AbstractTestExecutionListener;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.arangodb.ArangoDB;

import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringRunner.class)
@SpringBootTest
@ContextConfiguration(
    loader = AnnotationConfigContextLoader.class,
    classes = {TestSQLDatabaseConfig.class}
)
public class GroupComponentTest extends AbstractTestExecutionListener {

    @Test
    public void contextLoads() {
        // Intentionally empty. This test is to make sure the context is created properly.
    }

    @TestConfiguration
    static class ArangoDatabaseFactoryConfig {

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
