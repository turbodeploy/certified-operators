package com.vmturbo.matrix.component;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.matrix.component.external.WeightClass;

/**
 * The {@link OverlayVertex} implements a single vertex in the graph.
 * Each vertex has a collection of ingressing and egressing edges.
 * Each such edge contains the other vertex.
 * <h4>Hash and Equality calculation</h4>
 * <pre>
 * Hash calculation: IP only
 * Equality calculation:
 * In case of different IP: non-equal.
 * In case of the same IP:
 * If one of the ports is 0 - equal
 * If both ports are non-0, then the result is the result of comparison of two ports.
 * </pre>
 * <h4>Explanation</h4>
 * We must calculate the hash only for the IP adress, since we consider {@code ip:0}
 * and {@code ip:some_non_0_port} to be equal.
 * We do not consider {@code ip:some_non_0_port_1} and {@code ip:some_non_0_port_2} to be
 * equals in case of the same ip, but different some_non_0_port_1 and some_non_0_port_2.
 * <h4>Hash:</h4>
 * The reason for hash being calculated only for IP is the way HashMap will calculate buckets,
 * so if we include port, then two vertices might never match (due to the fact they will most likely
 * fall into different HashMap buckets).
 */
@NotThreadSafe public class OverlayVertex extends BaseVertex {
    /**
     * The map of neighbors.
     * The map of {@code Source/Sink, Edge Flow amount} of the edges this vertex connects to.
     */
    private transient Map<OverlayVertex, Double> neighbours_;

    /**
     * The normalized IP address.
     */
    private String normalizedIpAddress_;

    /**
     * The listening port if any.
     */
    private int port_;

    /**
     * The hex array.
     */
    private static final char[] HEX_ = {'0', '1', '2', '3', '4', '5', '6', '7',
                                        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    /**
     * The weight classes length.
     */
    static final int WEIGHT_CLASSES_SIZE = WeightClass.values().length;

    /**
     * The OID.
     * Use java.lang.Long, as it is used directly in HashMap lookups, and thus we avoid the
     * excessive auto-boxing.
     * We also access it directly for improved performance in the {@link OverlayNetworkGraph}.
     */
    @VisibleForTesting
    VolatileLong oid_;

    /**
     * The underlay OID.
     */
    @VisibleForTesting
    VolatileLong underlayOid_;

    /**
     * Construct default instance of a matrix edge.
     */
    private OverlayVertex() {
    }

    /**
     * Constructs the vertex.
     *
     * @param ipAddress The IP address.
     * @param port      The server port if present. {@code 0} if not.
     */
    OverlayVertex(final @Nonnull String ipAddress, final int port) {
        // Check the port.
        if (port < 0 || port > 65536) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }
        port_ = port;
        normalizedIpAddress_ = normalizeIPAddress(ipAddress);
        init(new VertexID(normalizedIpAddress_, port_));
        // Fill in the details.
        neighbours_ = new HashMap<>();
    }

    /**
     * Adds an incoming vertex.
     *
     * @param edge The edge.
     */
    final void addIncoming(final @Nonnull OverlayEdge edge) {
        neighbours_.put(edge.getSource(), edge.getFlow());
    }

    /**
     * Adds an outgoing vertex.
     *
     * @param edge The edge.
     */
    final void addOutgoing(final @Nonnull OverlayEdge edge) {
        neighbours_.put(edge.getSink(), edge.getFlow());
    }

    /**
     * Converts IPv4 or IPv6 address to a form that can be comparable.
     * For IPv6 addresses, the address may have full and short form, which represent the same
     * address. We need to be able to handle it.
     * One more thing: When obtaining a shallow copy, the normalized IP address will work just fine,
     * since it is an IPv4 original or a hexadecimal string representation of an IPv6 address.
     * The {@link #equals(Object)} and {@link #hashCode()} are calculated off it.
     *
     * @param ipAddress The string representation of an IP address.
     * @return The normalized IP address.
     */
    static @Nonnull String normalizeIPAddress(final @Nonnull String ipAddress) {
        // IPv4 addresses are simple.
        // Since IPv4 addresses constitute the majority at the moment, provide a special case for
        // them. The IPv4 addresses are case-agnostic, in that they contain no characters that
        // have upper and lower case.
        if (ipAddress.indexOf(':') == -1) {
            return ipAddress;
        }
        // IPv6 addresses.
        // Here we have to deal with the full and short form representing the same address, so we
        // have to take a hit.
        InetAddress address;
        try {
            address = InetAddress.getByName(ipAddress);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(e);
        }
        final byte[] bytes = address.getAddress();
        final char[] chars = new char[bytes.length + bytes.length];
        for (int i = 0, j = 0; i < bytes.length; i++) {
            chars[j++] = HEX_[(bytes[i] & 0xFF) >>> 4];
            chars[j++] = HEX_[bytes[i] & 0xF];
        }
        return new String(chars);
    }

    /**
     * Returns the normalized IP address.
     *
     * @return The normalized IP address.
     */
    final @Nonnull String getNormalizedIpAddress() {
        return normalizedIpAddress_;
    }

    /**
     * Returns the port.
     *
     * @return The port.
     */
    public final int getPort() {
        return port_;
    }

    /**
     * Returns a new instance of the Node or its derived class.
     *
     * @return A new instance of the Node or its derived class.
     */
    @Override
    protected @Nonnull OverlayVertex newInstance() {
        return new OverlayVertex();
    }

    /**
     * Returns the a shallow copy of this vertex.
     *
     * @return The shallow copy of a vertex.
     */
    @Override
    @Nonnull
    OverlayVertex shallowCopy() {
        OverlayVertex vertex = (OverlayVertex)super.shallowCopy();
        vertex.neighbours_ = new HashMap<>();
        vertex.port_ = port_;
        vertex.normalizedIpAddress_ = normalizedIpAddress_;
        return vertex;
    }

    /**
     * Returns the flows per weight class.
     *
     * @param provider        The projected provider.
     * @param underlayNetwork The underlay network.
     * @return The flows per weight class.
     */
    final @Nonnull double[] getFlows(final VolatileLong provider,
                                     final @Nonnull UnderlayNetworkGraph underlayNetwork) {
        final double[] flows = new double[WEIGHT_CLASSES_SIZE];
        for (Map.Entry<OverlayVertex, Double> entry : neighbours_.entrySet()) {
            final VolatileLong dst = entry.getKey().underlayOid_;
            final WeightClass distance = underlayNetwork.computeDistance(provider, dst);
            if (distance != null) {
                flows[distance.ordinal()] += entry.getValue();
            }
        }
        return flows;
    }

    /**
     * Returns the OID.
     *
     * @return The oid.
     */
    @Nonnull VolatileLong getOid() {
        return oid_;
    }

    /**
     * Sets the OID.
     *
     * @param oid The oid.
     */
    void setOid(final VolatileLong oid) {
        oid_ = oid;
    }

    /**
     * Sets the underlay OID.
     *
     * @param oid The underlay OID.
     */
    void setUnderlayOid(final VolatileLong oid) {
        underlayOid_ = oid;
    }

    /**
     * Returns neighbours.
     * We use that for testing.
     *
     * @return The neighbours.
     */
    @VisibleForTesting
    @Nonnull Map<OverlayVertex, Double> getNeighbours() {
        return neighbours_;
    }

    /**
     * Restores dummy OIDs to {@code null} if necessary.
     */
    void restoreInvalidOids() {
        if (oid_ != null && oid_.value < 0) {
            oid_ = null;
        }
        if (underlayOid_ != null && underlayOid_.value < 0) {
            underlayOid_ = null;
        }
    }

    /**
     * The ID for the Vertex.
     */
    @VisibleForTesting
    static class VertexID extends ID {
        /**
         * The IP address.
         */
        private final String ip_;

        /**
         * The port.
         */
        private final int port_;

        /**
         * The precomputed hash.
         */
        private int hash_;

        /**
         * Constructs the VertexID.
         * The hash is computed from IP address only, since we might have multiple flows from the
         * same vertex with different ports. It is still a single vertex.
         *
         * @param ip   The IP address.
         * @param port The port.
         */
        VertexID(final @Nonnull String ip, final int port) {
            ip_ = ip;
            port_ = port;
            hash_ = ip_.hashCode();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null) {
                return false;
            } else if (getClass() == o.getClass()) {
                final VertexID other = (VertexID)o;
                // We check for "any port" for equality, since we don't always know
                // the port, or are dealing with the callers that do not respect the containers
                // or individual servers.
                return port_ == other.port_ &&
                       ip_.equals(other.ip_);
            } else {
                return false;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            return hash_;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            if (ip_.indexOf(':') != -1) {
                return "[" + ip_ + "]:" + port_;
            }
            return ip_ + ":" + port_;
        }
    }
}
