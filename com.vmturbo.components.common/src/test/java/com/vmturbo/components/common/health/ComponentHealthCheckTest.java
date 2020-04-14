package com.vmturbo.components.common.health;

import static com.vmturbo.components.common.BaseVmtComponent.PROP_STANDALONE;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.rules.TestName;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.WebApplicationContext;

import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.ComponentController;
import com.vmturbo.components.common.ConsulRegistrationConfig;

public class ComponentHealthCheckTest {

    private static final String API_PREFIX="";

    private static final int MAX_WAIT_SECS = 20;

    protected MockMvc mockMvc;

    private static BaseVmtComponent testComponent;
    private static ComponentController testController;

    ConfigurableWebApplicationContext context;

    @Rule
    public TestName testName = new TestName();

    private WebApplicationContext wac;
    private IntegrationTestServer server;

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Before
    public void setup() throws Exception {
        environmentVariables.set(BaseVmtComponent.PROP_COMPONENT_TYPE, "SimpleTestComponent");
        environmentVariables.set(PROP_STANDALONE, "true");
        environmentVariables.set("connRetryIntervalSeconds", "10");
        environmentVariables.set(BaseVmtComponent.PROP_PROPERTIES_YAML_PATH,
            "configmap/empty_properties.yaml");
        environmentVariables.set(BaseVmtComponent.PROP_serverHttpPort, "8282");
        environmentVariables.set(BaseVmtComponent.PROP_INSTANCE_ID, "instance");
        environmentVariables.set(BaseVmtComponent.PROP_INSTANCE_IP, "10.10.10.10");
        environmentVariables.set("serverGrpcPort", "9001");
        environmentVariables.set("consul_host", "consul");
        environmentVariables.set("consul_port", "5");
        environmentVariables.set("kvStoreTimeoutSeconds", "5");
        environmentVariables.set(BaseVmtComponent.ENV_CLUSTERMGR_PORT, "8889");
        environmentVariables.set(BaseVmtComponent.ENV_CLUSTERMGR_RETRY_S, "10");
        environmentVariables.set(BaseVmtComponent.ENV_CLUSTERMGR_HOST, "clustermgr");
        environmentVariables.set(ConsulRegistrationConfig.ENABLE_CONSUL_REGISTRATION, "false");
        context = SimpleTestComponent.start();
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
        testComponent = context.getBean(SimpleTestComponent.class);
        testController = context.getBean(ComponentController.class);
    }

    @After
    public void stop() throws Exception {
        testComponent.stopComponent();
        context.close();
        context.stop();
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
    }

    @Test
    public void testSimpleHealthEndpointText() throws Exception {
        long startTime = System.nanoTime();
        // now verify that the component is reported as ready
        MvcResult result = mockMvc.perform(get(API_PREFIX + "/health")
                .accept(MediaType.TEXT_PLAIN_VALUE, MediaType.ALL_VALUE))
                .andExpect(status().isOk())
                .andReturn();
    }

    @Configuration("theComponent")
    public static class SimpleTestComponent extends BaseVmtComponent {

        @Override
        public String getComponentName() {
            return "SimpleTestComponent";
        }

        static ConfigurableWebApplicationContext start() {
            return startContext(SimpleTestComponent.class);
        }
    }
}