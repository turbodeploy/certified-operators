package com.vmturbo.mediation.udt.explore;

import io.grpc.ManagedChannel;

/**
 * A holder class for gRpc channel connections.
 */
public class Connection {

    private final ManagedChannel groupChannel;
    private final ManagedChannel repositoryChannel;

    /**
     * Constructor.
     *
     * @param groupChannel      - gRpc channel for the Group Component.
     * @param repositoryChannel - gRpc channel for the Repository Component.
     */
    public Connection(ManagedChannel groupChannel, ManagedChannel repositoryChannel) {
        this.groupChannel = groupChannel;
        this.repositoryChannel = repositoryChannel;
    }

    /**
     * The method for closing gRpc channels.
     */
    public void release() {
        groupChannel.shutdownNow();
        repositoryChannel.shutdownNow();
    }

    public ManagedChannel getGroupChannel() {
        return groupChannel;
    }

    public ManagedChannel getRepositoryChannel() {
        return repositoryChannel;
    }
}
