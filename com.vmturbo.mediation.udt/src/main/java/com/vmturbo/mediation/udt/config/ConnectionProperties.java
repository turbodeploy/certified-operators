package com.vmturbo.mediation.udt.config;

import javax.annotation.ParametersAreNonnullByDefault;

/**
 * Data class for storing connection properties.
 */
public class ConnectionProperties {

    private final String groupHost;
    private final String repositoryHost;
    private final int gRpcPort;
    private final int gRpcPingIntervalSeconds;

    /**
     * Constructor.
     *
     * @param groupHost               - Group Component host.
     * @param repositoryHost          - Repository Component host.
     * @param gRpcPort                - gRPC server port.
     * @param gRpcPingIntervalSeconds - ping interval (sec).
     */

    @ParametersAreNonnullByDefault
    public ConnectionProperties(String groupHost, String repositoryHost, int gRpcPort, int gRpcPingIntervalSeconds) {
        this.groupHost = groupHost;
        this.repositoryHost = repositoryHost;
        this.gRpcPort = gRpcPort;
        this.gRpcPingIntervalSeconds = gRpcPingIntervalSeconds;
    }

    public String getGroupHost() {
        return groupHost;
    }

    public String getRepositoryHost() {
        return repositoryHost;
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
