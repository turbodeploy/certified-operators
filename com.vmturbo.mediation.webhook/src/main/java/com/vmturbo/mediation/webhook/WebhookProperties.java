package com.vmturbo.mediation.webhook;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.platform.sdk.probe.properties.PropertySpec;

/**
 * Webhook probe properties.
 */
public class WebhookProperties {

    /**
     * Property provider.
     */
    private final IPropertyProvider propertyProvider;

    /**
     * Connection timeout.
     */
    public static final IProbePropertySpec<Integer> CONNECTION_TIMEOUT_MSEC = new PropertySpec<>(
            "connection_timeout", Integer::valueOf, 30000);

    /**
     * Constructor.
     *
     * @param propertyProvider property provider
     */
    public WebhookProperties(@Nonnull IPropertyProvider propertyProvider) {
        this.propertyProvider = propertyProvider;
    }

    @Nonnull
    public Integer getConnectionTimeout() {
        return propertyProvider.getProperty(CONNECTION_TIMEOUT_MSEC);
    }
}
