package com.vmturbo.mediation.client.it;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import io.prometheus.client.CollectorRegistry;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.components.common.ConsulRegistrationConfig;
import com.vmturbo.mediation.client.it.AbstractIntegrationTest.ContextConfiguration;
import com.vmturbo.mediation.common.ProbeInstanceRegistry;
import com.vmturbo.mediation.common.tests.util.IRemoteMediation;
import com.vmturbo.mediation.common.tests.util.ISdkContainer;
import com.vmturbo.mediation.common.tests.util.ISdkEngine;
import com.vmturbo.mediation.common.tests.util.IntegrationTestProbeConfiguration;
import com.vmturbo.mediation.common.tests.util.ProbeCompiler;
import com.vmturbo.mediation.common.tests.util.SdkProbe;
import com.vmturbo.mediation.common.tests.util.WebsocketServer;
import com.vmturbo.sdk.server.common.CentralizedPropertiesProvider;
import com.vmturbo.sdk.server.common.DiscoveryDumper;
import com.vmturbo.topology.processor.communication.SdkServerConfig;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * SDK engine for testing SDK client-server communications in XL.
 */
public class XlSdkEngine implements ISdkEngine {

    private static final AtomicInteger jmxCounter = new AtomicInteger();

    private final Logger logger = LogManager.getLogger(getClass());
    /**
     * Jetty server, running RemoteMediation onboard (server-side).
     */
    private WebsocketServer webSocketServer;

    /**
     * Application context of generic mediation.
     */
    private AnnotationConfigWebApplicationContext applicationContext;
    private DiscoveryDumper discoveryDumper = Mockito.mock(DiscoveryDumper.class);
    /**
     * Collection of registered SDK containers. Used to close them all automatically.
     */
    private final Collection<SdkContainer> containers = new ArrayList<>();

    private final AtomicInteger instanceCounter;

    private final ExecutorService threadPool;
    private final TemporaryFolder tmpFolder;
    private final TestName testName;

    /**
     * Constructs XL SDK engine and starts SDK server side.
     *
     * @param threadPool thread pool to use
     * @param tmpFolder temporary folder rule to create new files
     * @param testName test name rule
     * @throws Exception on exceptions occurred.
     */
    public XlSdkEngine(@Nonnull ExecutorService threadPool, @Nonnull TemporaryFolder tmpFolder,
            @Nonnull TestName testName) throws Exception {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.tmpFolder = Objects.requireNonNull(tmpFolder);
        this.testName = Objects.requireNonNull(testName);
        this.instanceCounter = new AtomicInteger();

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
        environment.setProperty("server.grpcPort", "0");
        environment.setProperty("consul_port", "0");
        environment.setProperty("consul_host", "consul");
        environment.setProperty(ConsulRegistrationConfig.ENABLE_CONSUL_REGISTRATION, "false");

        applicationContext = new AnnotationConfigWebApplicationContext();
        applicationContext.setEnvironment(environment);
        applicationContext.register(TestMediationCommonConfig.class);

        // Setup Spring context
        final ContextLoaderListener springListener = new ContextLoaderListener(applicationContext);

        webSocketServer = new WebsocketServer(springListener, testName.getMethodName());
        logger.trace("Started server " + webSocketServer);
    }

    @Nonnull
    @Override
    public SdkProbe createSdkProbe(@Nonnull IntegrationTestProbeConfiguration probeConfig,
            @Nonnull String probeId, @Nullable Integer fullDiscoveryInterval) {
        return new SdkProbe(probeConfig, probeId, fullDiscoveryInterval);
    }

    @Override
    public boolean isProbeRegistered(@Nonnull SdkProbe probe) {
        return findProbeId(probe).isPresent();
    }

    private Optional<Long> findProbeId(SdkProbe probe) {
        return getProbeStore().getProbeIdForType(probe.getType());
    }

    private ProbeStore getProbeStore() {
        return applicationContext.getBean(ProbeStore.class);
    }

    @Nonnull
    @Override
    public IRemoteMediation getRemoteMediationInterface() {
        return applicationContext.getBean(IRemoteMediation.class);
    }

    @Override
    public void startServer() throws Exception {
        webSocketServer.start();
    }

    @Override
    public void stopServer() throws Exception {
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

    @Nonnull
    @Override
    public DiscoveryDumper getDiscoveryDumper() {
        return discoveryDumper;
    }

    @Nonnull
    @Override
    public CentralizedPropertiesProvider getPropertiesProvider() {
        throw new NotImplementedException(
                "This functionality is not implemented for tests is XL yet");
    }

    @Override
    public ProbeInstanceRegistry getProbeInstanceRegistry() {
        throw new NotImplementedException(
                "This functionality is not implemented for tests is XL yet");
    }

    @Nonnull
    @Override
    public <T> T getBean(Class<T> clazz) {
        return applicationContext.getBean(clazz);
    }

    @Nonnull
    @Override
    public ISdkContainer createMediationContainer() throws Exception {
        return new SdkContainer();
    }

    @Nonnull
    @Override
    public Collection<ISdkContainer> getAllContainers() {
        return ImmutableSet.copyOf(containers);
    }

    private ClassLoader createClassLoader(String instanceId,
            IntegrationTestProbeConfiguration probeConfig) throws IOException {
        final File componentDir = tmpFolder.newFolder("component-" + instanceId);
        final URL filename = componentDir.toURI().toURL();
        final ClassLoader appClassLoader = new URLClassLoader(new URL[]{filename},
                Thread.currentThread().getContextClassLoader());
        return appClassLoader;
    }

    @Override
    public void close() throws Exception {
        for (SdkContainer container : containers) {
            container.close();
        }
        logger.info("Stopping the web socket endpoint and jetty server " + webSocketServer);
        webSocketServer.close();
        CollectorRegistry.defaultRegistry.clear();
    }

    /**
     * Class represents SDK container.
     */
    protected class SdkContainer implements ISdkContainer {

        private IntegrationTestServer testServer;
        private SdkProbe probe;
        private final File probeHome;
        private final File probeJarsDir;

        public SdkContainer() throws IOException {
            this.probeJarsDir = tmpFolder.newFolder("probe-jars");
            this.probeHome = tmpFolder.newFolder("probe-jars", "testprobe");
        }

        @Override
        public void start() throws Exception {
            if (webSocketServer == null) {
                throw new IllegalStateException(
                        "Websocket server should be started before container");
            }
            if (probe == null) {
                throw new IllegalStateException("Could not start sdk container without probe");
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
            environment.setProperty(ConsulRegistrationConfig.ENABLE_CONSUL_REGISTRATION,
                    "true");
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
            testServer =
                    new IntegrationTestServer(testName, ContextConfiguration.class, environment);
        }

        public void stop() throws Exception {
            if (testServer == null) {
                throw new IllegalStateException("Could not stop container. It is already stopped");
            }
            logger.debug("Stopping mediation container " + this);
            final Callable<?> task = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    testServer.close();
                    return null;
                }
            };
            threadPool.submit(task).get(30, TimeUnit.SECONDS);
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
        public void createProbeJar(@Nonnull SdkProbe probe) throws Exception {
            if (this.probe != null) {
                throw new IllegalStateException(
                        "Probe is already set to value " + this.probe.getType()
                                + " while trying to add probe " + probe.getType());
            }
            this.probe = Objects.requireNonNull(probe);
            ProbeCompiler.configureProbe(probeHome, probe);
        }

        @Nonnull
        @Override
        public File getJarDestination(@Nonnull SdkProbe probe) {
            if (!probe.getType().equals(this.probe.getType())) {
                throw new IllegalStateException("Unsupported probe requested");
            }
            return probeHome;
        }

        @Override
        public String toString() {
            return "SDK container probe: " + probe + " path: " + probeHome;
        }
    }
}
