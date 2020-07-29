package com.vmturbo.mediation.udt.config;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Data class for storing connection properties.
 */
public class ConnectionProperties {

    private final String groupHost;
    private final String repositoryHost;
    private final String topologyProcessorHost;
    private final int gRpcPort;
    private final int gRpcPingIntervalSeconds;

    /**
     * Constructor.
     *
     * @param groupHost               - Group Component host.
     * @param repositoryHost          - Repository Component host.
     * @param topologyProcessorHost   - Topology Processor Component host.
     * @param gRpcPort                - gRPC server port.
     * @param gRpcPingIntervalSeconds - ping interval (sec).
     */

    @ParametersAreNonnullByDefault
    public ConnectionProperties(String groupHost, String repositoryHost, String topologyProcessorHost,
                                int gRpcPort, int gRpcPingIntervalSeconds) {
        this.groupHost = groupHost;
        this.repositoryHost = repositoryHost;
        this.topologyProcessorHost = topologyProcessorHost;
        this.gRpcPort = gRpcPort;
        this.gRpcPingIntervalSeconds = gRpcPingIntervalSeconds;
    }

    public String getGroupHost() {
        return groupHost;
    }

    public String getRepositoryHost() {
        return repositoryHost;
    }

    public String getTopologyProcessorHost() {
        return topologyProcessorHost;
    }

    /**
     * Port of gPRC.
     *
     * @return port number.
     */
    public int getgRpcPort() {
        return gRpcPort;
    }

    /**
     * Ping interval for gRPC (sec).
     *
     * @return seconds.
     */
    public int getgRpcPingIntervalSeconds() {
        return gRpcPingIntervalSeconds;
    }
}
