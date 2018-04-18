package com.vmturbo.components.common.health;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.AnnotationConfigWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.BaseVmtComponentConfig;
import com.vmturbo.components.common.ComponentController;

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

    private ContextConfiguration testConfig;

    private static BaseVmtComponent testComponent;
    private static ComponentController testController;

    private WebApplicationContext wac;
    private Server server;

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

        @Bean
        public DiscoveryClient discoveryClient() {
            return Mockito.mock(DiscoveryClient.class);
        }
    }

    @Before
    public void setup() throws Exception {
        final Server server = new Server();

        final AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
        context.register(ContextConfiguration.class);
        final MockEnvironment env = new MockEnvironment();
        env.setProperty("server.grpcPort", "1");
        env.setProperty("spring.cloud.consul.host", "ddd");
        env.setProperty("spring.cloud.consul.port", "2");
        env.setProperty("spring.application.name", "3");
        env.setProperty("kvStoreRetryIntervalMillis", "4");
        env.setProperty("spring.cloud.consul.config.enabled", "false");
        env.setProperty("spring.cloud.service-registry.enabled", "false");
        env.setProperty("spring.cloud.service-registry.auto-registration.enabled", "false");
        context.setEnvironment(env);

        final ServletContextHandler handler = new ServletContextHandler();
        final DispatcherServlet servlet = new DispatcherServlet(context);
        final ServletHolder holder = new ServletHolder(servlet);
        handler.addServlet(holder, "/*");
        final ContextLoaderListener springListener = new ContextLoaderListener(context);
        handler.addEventListener(springListener);
        server.setHandler(handler);
        server.start();

        wac = context;
        mockMvc = MockMvcBuilders.webAppContextSetup(wac).build();
        testComponent = wac.getBean(SimpleTestComponent.class);
        testController = wac.getBean(ComponentController.class);
        testConfig = wac.getBean(ContextConfiguration.class);
    }

    @After
    public void stop() throws Exception {
        if (server != null) {
            server.stop();
            server.join();
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