package com.vmturbo.extractor.grafana.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.http.client.utils.URIBuilder;

/**
 * All external configuration required to initialize the {@link GrafanaClient}.
 */
public class GrafanaClientConfig {
    private String adminUser = "admin";
    private String adminPassword = "admin";

    private String grafanaHost = "localhost";
    private int grafanaPort = 3000;

    /**
     * Set the administrator username.
     *
     * @param adminUser New admin user.
     * @return The config, for method chaining.
     */
    @Nonnull
    public GrafanaClientConfig setAdminUser(@Nonnull final String adminUser) {
        this.adminUser = adminUser;
        return this;
    }

    /**
     * Set the administrator password.
     *
     * @param adminPassword New admin password.
     * @return The config, for method chaining.
     */
    @Nonnull
    public GrafanaClientConfig setAdminPassword(@Nonnull final String adminPassword) {
        this.adminPassword = adminPassword;
        return this;
    }

    /**
     * Set the grafana hostname.
     *
     * @param grafanaHost New grafana hostname.
     * @return The config, for method chaining.
     */
    @Nonnull
    public GrafanaClientConfig setGrafanaHost(@Nonnull final String grafanaHost) {
        this.grafanaHost = grafanaHost;
        // Validate that we can still get a valid root URL to grafana with this host.
        getGrafanaUrl();
        return this;
    }

    /**
     * Set the grafana port.
     *
     * @param grafanaPort New grafna port.
     * @return The config, for method chaining.
     */
    @Nonnull
    public GrafanaClientConfig setGrafanaPort(@Nonnull final int grafanaPort) {
        this.grafanaPort = grafanaPort;
        // Validate that we can still get a valid root URL to grafana with this port.
        getGrafanaUrl();
        return this;
    }

    /**
     * Get the admin user.
     *
     * @return The admin username.
     */
    @Nonnull
    public String getAdminUser() {
        return adminUser;
    }

    /**
     * Get the admin password.
     *
     * @return The admin password.
     */
    @Nonnull
    public String getAdminPassword() {
        return adminPassword;
    }

    /**
     * Get a Grafana URL.
     *
     * @param queryParams Query parameters: (name) -> (value)
     * @param pathSegments The path segments for the particular API path. No "api" prefix required.
     *                     e.g. to go to "grafanaurl/api/dashboards" use "dashboards".
     * @return The URI.
     */
    public URI getGrafanaUrl(Map<String, String> queryParams, String... pathSegments) {
        try {
            List<String> path = new ArrayList<>();
            // All Grafana API paths start with "/api/.
            path.add("api");
            for (String segment : pathSegments) {
                path.add(segment);
            }

            final URIBuilder bldr = new URIBuilder()
                    .setScheme("http")
                    .setHost(grafanaHost)
                    .setPort(grafanaPort)
                    .setPathSegments(path);
            queryParams.forEach(bldr::addParameter);
            return bldr.build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URI syntax", e);
        }
    }

    /**
     * Get a Grafana URI for a particular API sub-path.
     * @param pathSegments See {@link GrafanaClientConfig#getGrafanaUrl(Map, String...)}.
     * @return The URI.
     */
    @Nonnull
    public URI getGrafanaUrl(String... pathSegments) {
        return getGrafanaUrl(Collections.emptyMap(), pathSegments);
    }

}
