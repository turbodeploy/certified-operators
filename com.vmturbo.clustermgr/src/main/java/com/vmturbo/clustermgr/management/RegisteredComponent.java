package com.vmturbo.clustermgr.management;

import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.Nonnull;

import org.apache.http.client.utils.URIBuilder;

import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentInfo;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.UriInfo;

/**
 * Information about a component registered in the {@link ComponentRegistry}.
 */
public class RegisteredComponent {

    private final ComponentInfo componentInfo;

    private final ComponentHealth componentHealth;

    /**
     * Create a new {@link RegisteredComponent} POJO.
     *
     * @param componentInfo Information about the component.
     * @param componentHealth Component's current health.
     */
    public RegisteredComponent(@Nonnull final ComponentInfo componentInfo,
                               @Nonnull final ComponentHealth componentHealth) {
        this.componentInfo = componentInfo;
        this.componentHealth = componentHealth;
    }

    /**
     * Get a URI that can be used to issue REST requests to the component.
     *
     * @param requestPath REST path the URI should point to within the component. e.g. "/health".
     * @return The {@link URI}.
     * @throws URISyntaxException If there is an issue with constructing a valid URI.
     */
    @Nonnull
    public URI getUri(final String requestPath) throws URISyntaxException {
        // create a request from the given path and the target component instance properties
        URI requestUri;
        UriInfo uriInfo = componentInfo.getUriInfo();
        requestUri = new URIBuilder()
            .setHost(uriInfo.getIpAddress())
            .setPort(uriInfo.getPort())
            .setScheme("http")
            .setPath(uriInfo.getRoute() + requestPath)
            .build();
        return requestUri;
    }

    /**
     * Get the component's health status.
     *
     * @return The {@link ComponentHealth}.
     */
    @Nonnull
    public ComponentHealth getComponentHealth() {
        return componentHealth;
    }

    /**
     * Get the {@link ComponentInfo} the component registered with.
     *
     * @return The {@link ComponentInfo}.
     */
    @Nonnull
    public ComponentInfo getComponentInfo() {
        return componentInfo;
    }
}
