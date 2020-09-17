package com.vmturbo.mediation.udt.explore;

import io.grpc.ManagedChannel;

import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * A holder class for gRpc channel connections.
 */
public class Connection {

    private final ManagedChannel groupChannel;
    private final ManagedChannel repositoryChannel;
    private final ManagedChannel topologyProcessorChannel;
    private final TopologyProcessor topologyProcessorApi;

    /**
     * Constructor.
     *
     * @param groupChannel         - gRpc channel for the Group Component.
     * @param repositoryChannel    - gRpc channel for the Repository Component.
     * @param tpChannel            - gRpc channel for the TopologyProcessor Component.
     * @param topologyProcessorApi - TopologyProcessor API.
     */
    public Connection(ManagedChannel groupChannel, ManagedChannel repositoryChannel,
                      ManagedChannel tpChannel, TopologyProcessor topologyProcessorApi) {
        this.groupChannel = groupChannel;
        this.repositoryChannel = repositoryChannel;
        this.topologyProcessorChannel = tpChannel;
        this.topologyProcessorApi = topologyProcessorApi;
    }

    /**
     * The method for closing gRpc channels.
     */
    public void release() {
        groupChannel.shutdownNow();
        repositoryChannel.shutdownNow();
        topologyProcessorChannel.shutdownNow();
    }

    public ManagedChannel getGroupChannel() {
        return groupChannel;
    }

    public ManagedChannel getRepositoryChannel() {
        return repositoryChannel;
    }

    public ManagedChannel getTopologyProcessorChannel() {
        return topologyProcessorChannel;
    }

    public TopologyProcessor getTopologyProcessorApi() {
        return topologyProcessorApi;
    }
}
