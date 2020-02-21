package com.vmturbo.mediation.client.it;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.prometheus.client.CollectorRegistry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.stereotype.Component;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.components.common.BaseVmtComponentConfig;
import com.vmturbo.components.common.ConsulRegistrationConfig;
import com.vmturbo.components.common.migration.MigrationFramework;
import com.vmturbo.mediation.client.MediationComponentConfig;
import com.vmturbo.mediation.client.MediationComponentMain;
import com.vmturbo.mediation.common.tests.util.IRemoteMediation;
import com.vmturbo.mediation.common.tests.util.IntegrationTestProbeConfiguration;
import com.vmturbo.mediation.common.tests.util.ProbeCompiler;
import com.vmturbo.mediation.common.tests.util.SdkProbe;
import com.vmturbo.mediation.common.tests.util.TestConstants;
import com.vmturbo.mediation.common.tests.util.ThreadNaming;
import com.vmturbo.mediation.common.tests.util.WebsocketServer;
import com.vmturbo.topology.processor.communication.RemoteMediation;
import com.vmturbo.topology.processor.communication.SdkServerConfig;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Abstract class for all the integration tests, that point generic-mediation and mediation-common
 * interactions.
 */
public abstract class AbstractIntegrationTest {

    protected static final long TIMEOUT = TestConstants.TIMEOUT;

    private ExecutorService threadPool;

    // Server-side objects
    /**
     * Jetty server, running RemoteMediation onboard (server-side).
     */
    private WebsocketServer webSocketServer;

    /**
     * Application context of generic mediation.
     */
    private AnnotationConfigWebApplicationContext applicationContext;
    /**
     * Collection of registered SDK containers. Used to close them all automatically.
     */
    private final Collection<SdkContainer> containers = new ArrayList<>();

    private Logger logger = LogManager.getLogger();
    @Rule
    public TestName testName = new TestName();
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();
    private AtomicInteger instanceCounter;
    private static AtomicInteger jmxCounter = new AtomicInteger();
    @Rule
    public ThreadNaming threadNaming = new ThreadNaming();

    @Configuration
    @Import({BaseVmtComponentConfig.class, MediationComponentConfig.class})
    @Component("theComponent")
    static class ContextConfiguration extends MediationComponentMain {

        @Primary
        @Bean
        public MigrationFramework migrationFramework() {
            return Mockito.mock(MigrationFramework.class);
        }
    }

    @Before
    public final void init() throws Exception {
        logger = LogManager.getLogger(getClass().getName() + "." + testName.getMethodName());
        logger.debug("start @before");
        instanceCounter = new AtomicInteger();
        threadPool = threadNaming.createThreadPool("it");
        initWebSocketEndpoint();

        logger.debug("end @before");
    }

    @After
    public final void stopAllServers() throws Exception {
        logger.debug("start @after");
        for (SdkContainer container : containers) {
            container.close();
        }
        stopWebSocketEndpoint();
        threadPool.shutdownNow();
        // Clear the prometheus collector registry between runs or else we can get a duplicate
        // registration
        CollectorRegistry.defaultRegistry.clear();
        logger.debug("end @after");
        logger = LogManager.getLogger();
    }

        /**
     * Intialize a web socket endpoint in a jetty server.
     *
     * @throws Exception on exceptions initializing endpoint
     */
    private void initWebSocketEndpoint() throws Exception {
        logger.trace("Starting web socket endpoint on port...");
        if (webSocketServer != null) {
            throw new IllegalStateException(
                            "Websocket server should not be started before it is stopped");
        }
        final MockEnvironment environment = new MockEnvironment();
        environment.setProperty(TestMediationCommonConfig.FIELD_TEST_NAME,
                testName.getMethodName());
        environment.setProperty("instance_id", testName.getMethodName());
        environment.setProperty("instance_ip", "10.10.10.10");
        environment.setProperty("identityGeneratorPrefix", "0");
        environment.setProperty("kvStoreTimeoutSeconds", "5");
        environment.setProperty("websocket.pong.timeout", "10000");
        environment.setProperty("serverGrpcPort", "0");
        environment.setProperty("consul_port", "0");
        environment.setProperty("consul_host", "consul");
        environment.setProperty(ConsulRegistrationConfig.ENABLE_CONSUL_REGISTRATION, "false");
        environment.setProperty("standalone", "true");
        environment.setProperty("clustermgr_port", "0");
        System.setProperty("standalone", "true");
        System.setProperty("connRetryIntervalSeconds", "10");

        applicationContext = new AnnotationConfigWebApplicationContext();
        applicationContext.setEnvironment(environment);
        applicationContext.register(TestMediationCommonConfig.class);

        // Setup Spring context
        final ContextLoaderListener springListener = new ContextLoaderListener(applicationContext);

        webSocketServer = new WebsocketServer(springListener, testName.getMethodName());
        logger.trace("Started server " + webSocketServer);
    }

    /**
     * Stop the web socket end point.
     */
    private void stopWebSocketEndpoint() {
        logger.info("Stopping the web socket endpoint and jetty server " + webSocketServer);
        if (webSocketServer != null) {
            try {
                webSocketServer.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
            webSocketServer = null;
        }
    }

    protected SdkContainer startSdkComponent(SdkProbe probe) throws Exception {
        final SdkContainer container = new SdkContainer(probe);
        containers.add(container);
        return container;
    }

    protected SdkProbe createSdkProbe(IntegrationTestProbeConfiguration probeConfig, String probeType)
                    throws Exception {
        return createSdkProbe(probeConfig, probeType, true);
    }

    protected SdkProbe createSdkProbe(IntegrationTestProbeConfiguration probeConfig, String probeType,
                                      boolean awaitRegistered) throws Exception {
        final SdkProbe probe = new SdkProbe(probeConfig, probeType);
        final SdkContainer container = startSdkComponent(probe);
        if (awaitRegistered) {
            container.awaitRegistered();
        }
        return probe;
    }

    private ClassLoader createClassLoader(String instanceId, IntegrationTestProbeConfiguration probeConfig)
                    throws IOException {
        final File componentDir = tmpFolder.newFolder("component-" + instanceId);
        final URL filename = componentDir.toURI().toURL();
        final ClassLoader appClassLoader =
                        new URLClassLoader(new URL[] { filename }, Thread.currentThread()
                                        .getContextClassLoader());
        writeSpringConfigForProbe(new File(componentDir, "probe-spring-config.xml"), probeConfig);
        return appClassLoader;
    }

    /**
     * Method writes basic probe Spring context configuration into a specified file.
     *
     * @param destFile file to write Spring beans config into
     * @param probeConfig probe configuration to get probe and executor classes from
     * @throws IOException on exception occur.
     */
    private static void writeSpringConfigForProbe(File destFile, IntegrationTestProbeConfiguration probeConfig)
                    throws IOException {
        try (final Writer writer = new FileWriter(destFile)) {
            writer.write("<beans xmlns=\"http://www.springframework.org/schema/beans\"\n");
            writer.write("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n");
            writer.write("xmlns:context=\"http://www.springframework.org/schema/context\"\n");
            writer.write("xsi:schemaLocation=\"http://www.springframework.org/schema/beans ");
            writer.write("http://www.springframework.org/schema/beans/spring-beans-4.1.xsd http://www.springframework.org/schema/context http://www.springframework.org/schema/context/spring-context.xsd\">\n");
            writer.write("\n");
            writer.write("<bean id=\"probeInstance\" class=\"" + probeConfig.getProbeClass()
                            + "\"/>\n");
            writer.write("</beans>\n");
        }
    }

    /**
     * Returns thread pool to use, if needed.
     *
     * @return thread pool
     */
    public ExecutorService getThreadPool() {
        return threadPool;
    }

    protected RemoteMediation getRemoteMediation() {
        return applicationContext.getBean(RemoteMediation.class);
    }

    protected IRemoteMediation getRemoteMediationInterface() {
        return applicationContext.getBean(IRemoteMediation.class);
    }

    public Logger getLogger() {
        return logger;
    }

    protected ProbeStore getProbeStore() {
        return applicationContext.getBean(ProbeStore.class);
    }

    private Optional<Long> findProbeId(SdkProbe probe) {
        return getProbeStore().getProbeIdForType(probe.getType());
    }

    /**
     * Returns probe id from probe storage.
     *
     * @param probe probe to get id for
     * @return probe id
     */
    protected long getProbeId(SdkProbe probe) {
        return findProbeId(probe).get();
    }

    /**
     * Returns whether the specified probe is registered or not.
     *
     * @param probe probe to check
     * @return whether probe is registered.
     */
    protected boolean isProbeRegistered(SdkProbe probe) {
        return findProbeId(probe).isPresent();
    }

    /**
     * Class represents SDK container.
     */
    protected class SdkContainer implements AutoCloseable {

        private final IntegrationTestServer testServer;
        private final SdkProbe probe;
        private final File probeHome;
        private final File probeJarsDir;


        private SdkContainer(SdkProbe probe) throws Exception {
            if (webSocketServer == null) {
                throw new IllegalStateException(
                                "Websocket server should be started before container");
            }
            this.probe = probe;
            this.probeJarsDir = tmpFolder.newFolder();
            this.probeHome =
                    new File(probeJarsDir, probe.getType().replaceAll("[^\\p{Alnum}]", "X"));
            if (!probeHome.mkdir()) {
                throw new IllegalStateException("Could not create directory " + probeHome);
            }
            final MockEnvironment environment = new MockEnvironment() {
                @Override
                protected void customizePropertySources(MutablePropertySources propertySources) {
                    propertySources.addLast(new MapPropertySource(
                                    StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME,
                                    getSystemProperties()));
                    propertySources.addLast(new SystemEnvironmentPropertySource(
                                    StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                                    getSystemEnvironment()));
                    super.customizePropertySources(propertySources);
                }
            };
            environment.setProperty(TestMediationCommonConfig.FIELD_TEST_NAME,
                    testName.getMethodName());
            environment.setProperty(ConsulRegistrationConfig.ENABLE_CONSUL_REGISTRATION, "false");
            environment.setProperty("consul_host", "consul");
            environment.setProperty("consul_port", "0");
            environment.setProperty("spring.application.name", "the-component");
            environment.setProperty("kvStoreTimeoutSeconds", "5");

            // Alter JMX domain in order to start multiple spring-boot applications inside one
            // JVM
            environment.setProperty("spring.jmx.default-domain",
                            "sdk-container-" + jmxCounter.getAndIncrement());
            environment.setProperty("serverHttpPort", "0");
            environment.setProperty("serverGrpcPort", "1");

            final String instanceId =
                            testName.getMethodName() + "-" + instanceCounter.getAndIncrement();
            environment.setProperty("instance_id", instanceId);
            environment.setProperty("instance_ip", "10.10.10.10");
            environment.setProperty("component_type", "sdk-test-" + instanceId);
            environment.setProperty("probe-directory", probeJarsDir.toString());
            environment.setProperty("serverAddress",
                    webSocketServer.getServerURI(SdkServerConfig.REMOTE_MEDIATION_PATH).toString());
            environment.setProperty("instances." + instanceId + ".identityGeneratorPrefix", "0");
            final Thread currentThread = Thread.currentThread();
            final ClassLoader currentClassLoader = currentThread.getContextClassLoader();
            ProbeCompiler.configureProbe(probeHome, probe);
            final ApplicationContext context;
            try {
                final ClassLoader appClassLoader =
                                createClassLoader(instanceId, probe.getProbeConfig());
                currentThread.setContextClassLoader(appClassLoader);
                testServer = new IntegrationTestServer(testName, ContextConfiguration.class,
                        environment);
            } finally {
                currentThread.setContextClassLoader(currentClassLoader);
            }
        }

        public void stop() throws Exception {
            logger.debug("Stopping mediation container " + this);
            final Callable<?> task = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    testServer.close();
                    return null;
                }
            };
            getThreadPool().submit(task).get(30, TimeUnit.SECONDS);
            logger.debug("Stopped mediation container " + this);
        }

        /**
         * Closes sdk container and releases all its resources. Container becomes unusable after
         * this call.
         *
         * @throws Exception on error freeing port
         */
        public void close() throws Exception {
            stop();
        }

        /**
         * Method blocks until one container is unregistered from remote mediation server.
         *
         * @throws Exception on exception occurs
         */
        public void awaitUnregistered() throws Exception {
            if (testServer.getApplicationContext().isRunning()) {
                throw new IllegalStateException(
                                "Container is not stopped. Unable to wait until it is terminated");
            }
            applicationContext.getBean(TestRemoteMediationServer.class).awaitContainerClosed();
        }

        public void awaitRegistered() throws InterruptedException {
            applicationContext.getBean(TestRemoteMediationServer.class).awaitTransportRegistered();
        }

        @Override
        public String toString() {
            return "SDK container probe: " + probe + " path: " + probeHome;
        }
    }
}
