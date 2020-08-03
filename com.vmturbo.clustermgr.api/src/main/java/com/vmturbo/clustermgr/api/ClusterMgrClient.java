package com.vmturbo.clustermgr.api;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;

/**
 * ClusterMgrClient provides the client-side functionality necccesary to interact with the Cluster
 * Mgr API.
 **/
public class ClusterMgrClient {

    public static final String COMPONENT_VERSION_KEY = "component.version";
    public static final String UNKNOWN_VERSION_STRING = "<Unknown Version>";

    private ClusterMgrClient() {}

    @Nonnull
    public static ClusterMgrRestClient createClient(
            @Nonnull ComponentApiConnectionConfig connectionConfig) {
        return new ClusterMgrRestClient(connectionConfig);
    }
}
