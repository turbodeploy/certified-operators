package com.vmturbo.components.api.client;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The root class API Client implementations should extend.
 *
 * @param <RestClient> The REST client used for RPC's to the server.
 */
public abstract class ComponentApiClient<RestClient extends ComponentRestClient> {

    protected final Logger logger = LogManager.getLogger();

    /**
     * The client used for REST API calls.
     */
    protected final RestClient restClient;

    protected ComponentApiClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        this.restClient = createRestClient(connectionConfig);
    }

    @Nonnull
    protected abstract RestClient createRestClient(
            @Nonnull final ComponentApiConnectionConfig connectionConfig);
}
