package com.vmturbo.mediation.udt.config;

import static com.vmturbo.components.common.BaseVmtComponent.ContextConfigurationException;

import javax.annotation.Nonnull;
import javax.annotation.ParametersAreNonnullByDefault;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

import com.vmturbo.components.common.config.PropertiesLoader;

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
     * Reads connection properties.
     *
     * @return {@link ConnectionProperties} instance.
     * @throws ContextConfigurationException - in case of any exception regarding to the file 'properties.yam'
     */
    @Nonnull
    public ConnectionProperties getConnectionProperties() throws ContextConfigurationException {
        registerConfiguration(context, PropertyConfiguration.class);
        PropertiesLoader.addConfigurationPropertySources(context);
        final ConfigurableEnvironment env = context.getEnvironment();
        return new ConnectionProperties(
                env.getProperty("groupHost"),
                env.getProperty("repositoryHost"),
                env.getProperty("topologyProcessorHost"),
                env.getProperty("serverGrpcPort", Integer.class),
                env.getProperty("grpcPingIntervalSeconds", Integer.class));
    }

    @ParametersAreNonnullByDefault
    private void registerConfiguration(AnnotationConfigWebApplicationContext context, Class cls) {
        context.register(cls);
        context.refresh();
    }
}
