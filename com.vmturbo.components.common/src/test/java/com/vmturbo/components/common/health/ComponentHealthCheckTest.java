package com.vmturbo.components.common.health;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.BaseVmtComponentConfig;
import com.vmturbo.components.common.ComponentController;
import com.vmturbo.components.common.ConsulDiscoveryManualConfig;

public class ComponentHealthCheckTest {

    private static final String API_PREFIX="";

    private static final int MAX_WAIT_SECS = 20;

    protected MockMvc mockMvc;

    private ContextConfiguration testConfig;

    private static BaseVmtComponent testComponent;
    private static ComponentController testController;

    @Rule
    public TestName testName = new TestName();

    private WebApplicationContext wac;
    private IntegrationTestServer server;

    /**
     * Nested configuration for Spring context.
     */
    @Configuration
    @EnableWebMvc
    @Import({BaseVmtComponentConfig.class})
    static class ContextConfiguration extends WebMvcConfigurerAdapter {
        @Bean
        public BaseVmtComponent theComponent() {
            System.out.println("Creating the simple test component.");
            return new SimpleTestComponent();
        }
    }

    @Before
    public void setup() throws Exception {
        final MockEnvironment env = new MockEnvironment();
        env.setProperty("server.grpcPort", "1");
        env.setProperty("kvStoreRetryIntervalMillis", "4");
        env.setProperty("consul_host", "consul");
        env.setProperty("consul_port", "5");
        env.setProperty(BaseVmtComponent.PROP_INSTANCE_ID, "instance");
        env.setProperty(BaseVmtComponent.PROP_COMPNENT_TYPE, "componentType");
        env.setProperty(BaseVmtComponent.PROP_SERVER_PORT, "8080");
        env.setProperty(ConsulDiscoveryManualConfig.DISABLE_CONSUL_REGISTRATION, "true");
        final IntegrationTestServer server =
                new IntegrationTestServer(testName, ContextConfiguration.class, env);

        wac = server.getApplicationContext();
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        testComponent = wac.getBean(SimpleTestComponent.class);
        testController = wac.getBean(ComponentController.class);
        testConfig = wac.getBean(ContextConfiguration.class);
    }

    @After
    public void stop() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Test
    public void testSimpleHealthEndpointJson() throws Exception {
        // initially the component will not be ready.
        Assert.assertTrue("Component should be already started",
                testComponent.getHealthMonitor().getHealthStatus().isHealthy());
        // verify that the health end point also returns an error
        ResponseEntity<?> response = testController.getHealth();
        int responseCode = response.getStatusCodeValue();
        Assert.assertTrue("/health endpoint should return 2xx.",
                responseCode >= 200 && responseCode < 300);
        long startTime = System.nanoTime();
        testComponent.stopComponent();
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/health")
                .accept(MediaType.APPLICATION_JSON_UTF8_VALUE,MediaType.ALL_VALUE))
                .andExpect(status().is5xxServerError())
                .andReturn();
        responseCode = result.getResponse().getStatus();
        Assert.assertTrue("/health endpoint should return 5xx after shutdown",responseCode >= 500);
    }

    @Test
    public void testSimpleHealthEndpointText() throws Exception {
        long startTime = System.nanoTime();
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
}