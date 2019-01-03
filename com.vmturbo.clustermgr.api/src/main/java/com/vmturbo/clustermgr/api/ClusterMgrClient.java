package com.vmturbo.clustermgr.api;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;

/**
 * ClusterMgrClient provides the client-side functionality necccesary to interact with the Cluster
 * Mgr API.
 **/
public class ClusterMgrClient {

    private ClusterMgrClient() {}

    @Nonnull
    public static ClusterMgrRestClient createClient(
            @Nonnull ComponentApiConnectionConfig connectionConfig) {
        return new ClusterMgrRestClient(connectionConfig);
    }
}
