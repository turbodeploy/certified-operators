package com.vmturbo.components.common.health;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.ComponentController;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(loader = AnnotationConfigWebContextLoader.class)
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
@TestPropertySource(properties = {"server.grpcPort=9001"})
public class ComponentHealthCheckTest {

    private static final String API_PREFIX="/api/v2";

    private static final int MAX_WAIT_SECS = 20;

    protected MockMvc mockMvc;

    @Autowired
    private ContextConfiguration testConfig;

    private static BaseVmtComponent testComponent;
    private static ComponentController testController;

    @Autowired
    private WebApplicationContext wac;

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    static class ContextConfiguration extends WebMvcConfigurerAdapter {
        @Bean
        public BaseVmtComponent theComponent() {
            return new SimpleTestComponent();
        }

        @Bean
        public DiscoveryClient discoveryClient() {
            return Mockito.mock(DiscoveryClient.class);
        }
    }

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        testComponent = wac.getBean(SimpleTestComponent.class);
        testController = wac.getBean(ComponentController.class);
    }

    @AfterClass
    public static void tearDownStatics() {
        // the static objects hang around after the tests are over -- clear them out when the tests
        // in this class have run
        if (testComponent != null) {
            testComponent.stopComponent();
            testComponent = null;
        }
        if (testController != null) {
            testController = null;
        }
    }

    @Test
    public void testSimpleHealthEndpointJson() throws Exception {
        // initially the component will not be ready.
        Assert.assertFalse("Component should still be starting up",
                testComponent.getHealthMonitor().getHealthStatus().isHealthy());
        // verify that the health end point also returns an error
        ResponseEntity<?> response = testController.getHealth();
        int responseCode = response.getStatusCodeValue();
        Assert.assertFalse("/health endpoint should not return 2xx.",
                responseCode >= 200 && responseCode < 300);
        long startTime = System.nanoTime();
        testComponent.startComponent();
        // now wait for the component to finish starting
        waitUntilHealthy();
        // now verify that the component is reported as ready
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/health")
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE,MediaType.ALL_VALUE))
                .andExpect(status().isOk())
                .andReturn();
        responseCode = result.getResponse().getStatus();
        Assert.assertTrue("/health endpoint should return 2xx after startup completes",responseCode >= 200 && responseCode < 300);
    }

    @Test
    public void testSimpleHealthEndpointText() throws Exception {
        long startTime = System.nanoTime();
        testComponent.startComponent();
        // now wait for the component to finish starting
        waitUntilHealthy();
        // now verify that the component is reported as ready
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/health")
                .accept(MediaType.TEXT_PLAIN_VALUE,MediaType.ALL_VALUE))
                .andExpect(status().isOk())
                .andReturn();
    }

    /**
     * Test a health monitor with multiple dependencies in it.
     */
    @Test
    public void testHealthMonitorWithDependencies() {
        CompositeHealthMonitor monitor = new CompositeHealthMonitor("Test");
        SimpleHealthStatusProvider subcomponent1 = new SimpleHealthStatusProvider("subcomponent1");
        SimpleHealthStatusProvider subcomponent2 = new SimpleHealthStatusProvider("subcomponent2");
        monitor.addHealthCheck(subcomponent1);
        monitor.addHealthCheck(subcomponent2);
        // initial call -- all subcomponents should be unhealthy
        Assert.assertFalse(monitor.getHealthStatus().isHealthy());

        // set one subcomponent to healthy -- the component should still be unhealhty
        subcomponent1.reportHealthy();
        Assert.assertFalse("One dependency unhealthy --> component should be unhealthy.",
                monitor.getHealthStatus().isHealthy());

        // now set the other subcomponent to healthy -- the component should now be healthy
        subcomponent2.reportHealthy();
        Assert.assertTrue(monitor.getHealthStatus().isHealthy());
    }

    @Configuration("theComponent")
    public static class SimpleTestComponent extends BaseVmtComponent {

        @Override
        public String getComponentName() {
            return "SimpleTestComponent";
        }
    }

    /**
     * Wait until the component is 'healthy' by calling the healthMonitor and fail after 5 seconds.
     * There's a Thread.sleep() in the retry / wait loop.
     *
     * TODO - figure out how to not depend on timing, or change to Integration Test
     *
     * @throws InterruptedException if the Thread.sleep() is interrupted
     */
    private void waitUntilHealthy() throws InterruptedException {
        long startTime = System.nanoTime();
        while(! testComponent.getHealthMonitor().getHealthStatus().isHealthy()) {
            long elapsedTime = System.nanoTime() - startTime;
            if ( elapsedTime > TimeUnit.SECONDS.toNanos(5) ) {
                // if it took longer than 5 seconds to start up, something is wrong!
                Assert.fail("Test Component still isn't healthy after 5 seconds!");
                break;
            }
            Thread.sleep(100);
        }
    }
}