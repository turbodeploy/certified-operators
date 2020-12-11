package com.vmturbo.topology.processor.operation.discovery;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.sdk.common.MediationMessage.DiscoveryRequest;

/**
 * Wrap the information that TransportDiscoveryWorker needs to launch a discovery and have it
 * properly tracked by OperationManager.
 */
public class DiscoveryBundle {

    private final Discovery discovery;

    private final DiscoveryRequest discoveryRequest;

    private final DiscoveryMessageHandler discoveryMessageHandler;

    private Exception exception;

    /**
     * Create an instance of DiscoveryBundle.
     *
     * @param discovery the {@link Discovery} to encapsulate.
     * @param discoveryRequest the {@link DiscoveryRequest} to encapsulate.
     * @param discoveryMessageHandler the {@link DiscoveryMessageHandler} to encapsulate.
     */
    public DiscoveryBundle(@Nonnull Discovery discovery,
            @Nullable DiscoveryRequest discoveryRequest,
            @Nullable DiscoveryMessageHandler discoveryMessageHandler) {
        this.discovery = Objects.requireNonNull(discovery);
        this.discoveryRequest = discoveryRequest;
        this.discoveryMessageHandler = discoveryMessageHandler;
    }

    /**
     * Get the Discovery that is in this DiscoveryBundle.
     *
     * @return Discovery in this DiscoveryBundle.
     */
    public Discovery getDiscovery() {
        return discovery;
    }

    /**
     * Get the {@link DiscoveryRequest} that is in this DiscoveryBundle.
     *
     * @return DiscoveryRequest that is in this DiscoveryBundle.
     */
    @Nullable
    public DiscoveryRequest getDiscoveryRequest() {
        return discoveryRequest;
    }

    /**
     * Get the {@link DiscoveryMessageHandler} that is in this DiscoveryBundle.
     * @return DiscoveryMessageHandler that is in this DiscoveryBundle.
     */
    @Nullable
    public DiscoveryMessageHandler getDiscoveryMessageHandler() {
        return discoveryMessageHandler;
    }

    /**
     * Get any exception that occurred while trying to run this discovery.
     *
     * @return Exception that occurred when running the discovery or null.
     */
    @Nullable
    public Exception getException() {
        return exception;
    }

    /**
     * Set the Exception that occurred when this discovery was attempted.
     *
     * @param exception Exception that occurred when trying to run this discovery.
     */
    public void setException(Exception exception) {
        this.exception = exception;
    }

    @Override
    public String toString() {
        StringBuilder builder =  new StringBuilder();
        if (discovery != null) {
            builder.append("Discovery: " + discovery.toString());
        }
        if (discoveryRequest != null) {
            builder.append("  DiscoveryRequest: " + discoveryRequest.toString());
        }
        if (discoveryMessageHandler != null) {
            builder.append("  DiscoveryMessageHandler: " + discoveryMessageHandler.toString());
        }
        if (exception != null) {
            builder.append("  Exception: " + exception.toString());
        }
        return builder.toString();
    }
}
