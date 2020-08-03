package com.vmturbo.components.common.config;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;
import static org.springframework.test.util.AssertionErrors.assertEquals;

import java.io.IOException;
import java.util.Properties;

import javax.annotation.Nonnull;
import javax.servlet.Servlet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

import com.vmturbo.components.common.BaseVmtComponent.ContextConfigurer;

/**
 * Test that config properties read from a YAML file are injected propertly for different
 * data types, including int, boolean, String, and multi-line String.
 */
public class ConfigMapSpringValueTest {

    private static final String TEST_CONFIG_FILE = "configmap/multiple-datatype_properties.yaml";

    private static final Logger logger = LogManager.getLogger();

    // port 0 for Jetty means assign a random port
    private static final String TEST_SERVER_PORT = "0";

    /**
     * Test that a value injected using a properties source correctly set for different data types.
     * @throws IOException - if there is an error reading the test properties.yaml - should never
     * happen
     */
    @Test
    public void testConfigMapSpringValueInjectionSystemProperty() throws IOException {
        // arrange
        final Properties yamlProperties = ConfigMapPropertiesReader.readConfigMap("test",
            TEST_CONFIG_FILE);
        yamlProperties.forEach((key, value) -> {
            logger.info("key: {}, value: {}, value class: {}", key, value,
                value.getClass().getCanonicalName());
            System.setProperty((String)key, (String)value);
        });
        final PropertySource propertiesYamlPropertySource = new PropertiesPropertySource(
            "empty properties", new Properties());
        ConfigurableWebApplicationContext context = null;
        try {
            // act
            context = startWebserver(propertiesYamlPropertySource);
            checkValues(context);
        } finally {
            if (context != null) {
                context.close();
                context.stop();
            }
        }
    }

    /**
     * Test that a value injected using a properties source correctly set for different data types.
     * @throws IOException - if there is an error reading the test properties.yaml - should never
     * happen
     */
    @Test
    public void testConfigMapSpringValueInjectionPropertySource() throws IOException {
        // arrange
        final Properties yamlProperties = ConfigMapPropertiesReader.readConfigMap("test",
            TEST_CONFIG_FILE);
        yamlProperties.forEach((key, value) -> {
            logger.info("key: {}, value: {}, value class: {}", key, value,
                value.getClass().getCanonicalName());
            System.setProperty((String)key, (String)value);
        });
        final PropertySource propertiesYamlPropertySource = new PropertiesPropertySource(
            "yaml-properties", yamlProperties);
        ConfigurableWebApplicationContext context = null;
        try {
            // act
            context = startWebserver(propertiesYamlPropertySource);
            // assert
            checkValues(context);
        } finally {
            if (context != null) {
                context.close();
                context.stop();
            }
        }
    }

    private void checkValues(final ConfigurableWebApplicationContext context) {
        Object componentObject = context.getBean("theComponent");
        assertTrue("extpected SimpleTestComponent instance",
            componentObject instanceof SimpleTestComponent);
        SimpleTestComponent theComponent = (SimpleTestComponent)componentObject;
        assertEquals("int prop value", theComponent.intPropValue, 123);
        assertEquals("boolean prop value", theComponent.booleanPropValue, true);
        assertEquals("string prop value", theComponent.stringPropValue, "abc");
    }

    /**
     * A test component with @Value statements for int, boolean, and String types.
     */
    @Configuration("theComponent")
    public static class SimpleTestComponent {

        @Value("${int_prop}")
        protected int intPropValue;

        @Value("${boolean_prop}")
        protected boolean booleanPropValue;

        @Value("${string_prop}")
        protected String stringPropValue;
    }

    @Nonnull
    private ConfigurableWebApplicationContext startWebserver(
        @Nonnull final PropertySource testPropertySource) {
        org.eclipse.jetty.server.Server server =
            new org.eclipse.jetty.server.Server(Integer.parseInt(TEST_SERVER_PORT));

        final ContextConfigurer contextConfigurer = ConfigMapSpringValueTest::attachSpringContext;
        final ServletContextHandler contextServer =
            new ServletContextHandler(ServletContextHandler.SESSIONS);
        ConfigurableWebApplicationContext context = null;
        try {
            server.setHandler(contextServer);
            context = contextConfigurer.configure(contextServer);
            context.getEnvironment().getPropertySources().addFirst(testPropertySource);
            context.getEnvironment().getPropertySources().forEach(propertySource ->
                logger.info("property source: {} -> {}",
                    propertySource.getName(), propertySource.getSource()));
            server.start();
            if (!context.isActive()) {
                logger.error("Spring context failed to start. Shutting down.");
                fail("Spring context failed to start");
            }
            return context;
        } catch (Exception e) {
            logger.error("Web server failed to start. Shutting down.", e);
            fail("Web server failed to start");
        }
        return context;
    }

    @Nonnull
    private static ConfigurableWebApplicationContext attachSpringContext(
        @Nonnull ServletContextHandler contextServer) {
        final AnnotationConfigWebApplicationContext applicationContext =
            new AnnotationConfigWebApplicationContext();
        applicationContext.register(SimpleTestComponent.class);
        final Servlet dispatcherServlet = new DispatcherServlet(applicationContext);
        final ServletHolder servletHolder = new ServletHolder(dispatcherServlet);
        contextServer.addServlet(servletHolder, "/*");
        // Setup Spring context
        final ContextLoaderListener springListener = new ContextLoaderListener(applicationContext);
        contextServer.addEventListener(springListener);
        applicationContext.isActive();
        return applicationContext;
    }


}
