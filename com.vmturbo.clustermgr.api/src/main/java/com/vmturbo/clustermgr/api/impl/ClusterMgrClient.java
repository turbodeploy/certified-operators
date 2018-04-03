package com.vmturbo.clustermgr.api.impl;

import javax.annotation.Nonnull;

import com.vmturbo.api.serviceinterfaces.IClusterService;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;

/**
 * ClusterMgrClient provides the client-side functionality necccesary to interact with the Cluster
 * Mgr API.
 **/
public class ClusterMgrClient {

    private ClusterMgrClient() {}

    @Nonnull
    public static IClusterService createClient(
            @Nonnull ComponentApiConnectionConfig connectionConfig) {
        return new ClusterMgrRestClient(connectionConfig);
    }
}
