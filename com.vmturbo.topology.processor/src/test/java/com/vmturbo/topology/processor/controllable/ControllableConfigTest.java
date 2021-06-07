package com.vmturbo.topology.processor.controllable;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.jooq.DSLContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.sql.utils.dbmonitor.ProcessListClassifier;
import com.vmturbo.topology.processor.TopologyProcessorDBConfig;

/**
 * Test {@link ControllableConfig}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(properties = {"dbPort=3306", "serverHttpPort=8080", "authRetryDelaySecs=10", "authHost=auth"})
@ContextConfiguration
public class ControllableConfigTest {

    @Autowired
    private ControllableConfig controllableConfig;

    /**
     * Test EntityActionDaoImp is built correctly with default spring injected values.
     */
    @Test
    public void testEntityActionDaoImp() {
        assertEquals(1800, controllableConfig.entityActionDaoImp().moveSucceedRecordExpiredSeconds);
        assertEquals(3600, controllableConfig.entityActionDaoImp().inProgressActionExpiredSeconds);
        assertEquals(14400, controllableConfig.entityActionDaoImp().activateSucceedExpiredSeconds);
        assertEquals(21600, controllableConfig.entityActionDaoImp().scaleSucceedRecordExpiredSeconds);
        assertEquals(14400, controllableConfig.entityActionDaoImp().resizeSucceedRecordExpiredSeconds);
    }

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    static class ContextConfiguration {

        @Bean
        public TopologyProcessorDBConfig topologyProcessorDBConfig() {
            final TopologyProcessorDBConfig topologyProcessorDBConfig =
                mock(TopologyProcessorDBConfig.class);
            when(topologyProcessorDBConfig.dsl()).thenReturn(mock(DSLContext.class));
            return topologyProcessorDBConfig;
        }

        @Bean
        public ProcessListClassifier processListClassifier() throws JsonProcessingException {
            return mock(ProcessListClassifier.class);
        }

        /**
         * Bean to be tested.
         * @return test bean
         */
        @Bean
        public ControllableConfig controllableConfig() {
            return new ControllableConfig();
        }
    }
}
