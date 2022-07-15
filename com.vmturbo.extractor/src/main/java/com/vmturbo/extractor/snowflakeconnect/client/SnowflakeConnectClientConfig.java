package com.vmturbo.extractor.snowflakeconnect.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.http.client.utils.URIBuilder;

/**
 * This object encapsulates the uri generation for {@link SnowflakeConnectClient}.
 */
public class SnowflakeConnectClientConfig {

    private String host = "localhost";
    private int port = 8083;

    /**
     * Sets the hostname of snowflake connector.
     *
     * @param host the hostname
     * @return the config object
     */
    public SnowflakeConnectClientConfig setHost(String host) {
        this.host = host;
        return this;
    }

    /**
     * Sets the port of snowflake connector.
     *
     * @param port the port opened for http, by default 8083
     * @return the config object
     */
    public SnowflakeConnectClientConfig setPort(int port) {
        this.port = port;
        return this;
    }

    /**
     * Returns the correct URI based on the host & port.
     *
     * @param pathSegments the endpoint routes for the REST call
     * @return the config object
     */
    public URI getConnectorUrl(String... pathSegments) {
        try {
            List<String> path = new ArrayList<>();
            Collections.addAll(path, pathSegments);
            final URIBuilder bldr = new URIBuilder()
                    .setScheme("http")
                    .setHost(this.host)
                    .setPort(this.port)
                    .setPathSegments(path);
            return bldr.build();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URI syntax", e);
        }
    }
}
