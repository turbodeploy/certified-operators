package com.vmturbo.mediation.udt.config;

import static com.vmturbo.components.common.BaseVmtComponent.ContextConfigurationException;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.components.common.config.PropertiesLoader;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Class responsible for reading component`s properties required for gRPC connections.
 */
public class PropertyReader {

    private final AnnotationConfigWebApplicationContext context;

    /**
     * Constructor.
     *
     * @param context - Spring application context.
     */
    public PropertyReader(@Nonnull AnnotationConfigWebApplicationContext context) {
        this.context = context;
    }

    /**
     * Reads connection configuration.
     *
     * @return {@link ConnectionProperties} instance.
     * @throws ContextConfigurationException - in case of any exception regarding to the file 'properties.yam'
     */
    @Nonnull
    public ConnectionConfiguration getConnectionConfiguration() throws ContextConfigurationException {
        registerConfiguration(context, PropertyConfiguration.class);
        registerConfiguration(context, TopologyProcessorClientConfig.class);
        PropertiesLoader.addConfigurationPropertySources(context);
        final TopologyProcessorClientConfig tpConfig = context.getBean(TopologyProcessorClientConfig.class);
        final ConfigurableEnvironment env = context.getEnvironment();
        final ConnectionProperties properties = new ConnectionProperties(
                env.getProperty("groupHost"),
                env.getProperty("repositoryHost"),
                env.getProperty("topologyProcessorHost"),
                env.getProperty("serverGrpcPort", Integer.class),
                env.getProperty("grpcPingIntervalSeconds", Integer.class));
        return new ConnectionConfiguration(properties, tpConfig);
    }

    @ParametersAreNonnullByDefault
    private void registerConfiguration(AnnotationConfigWebApplicationContext context, Class cls) {
        context.register(cls);
        context.refresh();
    }
}
