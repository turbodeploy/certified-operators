package com.vmturbo.components.common.health;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

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
@TestPropertySource(properties = {"server.grpcPort=9001", "spring.cloud.consul.host=consul",
        "spring.cloud.consul.port=8500", "spring.application.name=testApplication",
        "kvStoreRetryIntervalMillis=1000"})
@NotThreadSafe
public class ComponentHealthCheckTest {

    private static final String API_PREFIX="";

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
            System.out.println("Creating the simple test component.");
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
            if ( elapsedTime > TimeUnit.SECONDS.toNanos(MAX_WAIT_SECS) ) {
                // if it took longer than 5 seconds to start up, something is wrong!
                Assert.fail("Test Component still isn't healthy after "+ MAX_WAIT_SECS +" seconds!");
                break;
            }
            System.out.println("Test component not healthy yet: "+ testComponent.getHealthMonitor().getHealthStatus().getDetails());
            Thread.sleep(100);
        }
    }
}