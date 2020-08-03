package com.vmturbo.matrix.component.external;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;

import com.vmturbo.matrix.component.ServiceEntityOIDMapper;
import com.vmturbo.platform.common.dto.CommonDTO.FlowDTO;

/**
 * The {@link MatrixInterface} represents the interface from the Topology to the Communication
 * Matrix.
 */
public interface MatrixInterface {
    /**
     * Obtains deep copy of the Matrix.
     *
     * @return The deep copy of this matrix.
     */
    @Nonnull MatrixInterface copy();

    /**
     * Dumps the content of the Matrix to a file.
     *
     * @param fileName The file to dump the data to.
     * @throws IOException In case of an error persisting matrix.
     */
    void dump(@Nonnull String fileName) throws IOException;

    /**
     * Restores the content of the Matrix from a file.
     *
     * @param fileName The file to restore the data from.
     * @throws IOException In case of an error restoring matrix.
     */
    void restore(@Nonnull String fileName) throws IOException;

    /**
     * Excludes the supplied IP addresses from the Matrix.
     * Please note that this is an expensive operation, so should be called for all known IP
     * addresses.
     * The consumer/provider and the underlay graph are unaffected.
     *
     * @param ips The set of IP addresses to be excluded.
     */
    void exclude(@Nonnull Set<String> ips);

    /**
     * Add all flows.
     * We first add all the new flows, then re-apply the old flows which we don't have now.
     * We identify the flow by pair of its vertices.
     * When we re-apply a previous flow, we fade its flow amount.
     *
     * @param flows The flow DTOs.
     */
    void update(@Nonnull Collection<FlowDTO> flows);

    /**
     * Resets underlay to be reused by the post-processor.
     */
    void resetUnderlay();

    /**
     * Populates the underlay network.
     * The OIDs reflect current topology, so that they can be updated as many times as needed,
     * for as long as the corresponding {@link #place(long, long)} call is also made before
     * anything is queried.
     *
     * @param consumerOID The consumer OID.
     * @param providerOID The provider OID.
     */
    void populateUnderlay(long consumerOID, long providerOID);

    /**
     * Populates the DPoD for the underlying network.
     *
     * @param oids The OIDs of Physical Hosts/Switches/Routers belonging to a single DPoD.
     */
    void populateDpod(@Nonnull Set<Long> oids);

    /**
     * Returns the collection of all VPoD.
     * Each VPoD is represented by a collection of the IP addresses.
     *
     * @return The collection of VPoDs.
     */
    @Nonnull Collection<Collection<String>> getVpods();

    /**
     * Links the Service Entity represented by an OID with the endpoint.
     * We update both current and update state in the overlay graph.
     * We can afford doing this any time we want to reflect the current state of Topology,
     * since we take a "current snapshot of Topology" at the time of the call.
     * The care must be taken to ensure {@link #place(long, long)} and
     * {@link #populateUnderlay(long, long)} calls are also made as needed.
     *
     * @param oid The consumer oid.
     * @param ip  The consumer IP address.
     */
    void setEndpointOID(long oid, @Nonnull String ip);

    /**
     * Place consumer on a provider.
     * NB! Must be invoked after the underlay has been populated and all IP associated with OIDs.
     *
     * @param consumerOID The consumer OID.
     * @param providerOID The provider OID.
     */
    void place(long consumerOID, long providerOID);

    /**
     * Returns the projected flows per weight class for a given endpoint to be located in a new
     * location.
     *
     * @param oid      The OID of the entity.
     * @param location The projected location.
     * @return result. Empty if no flows for an endpoint.
     */
    @Nonnull double[] getProjectedFlows(@Nonnull Long oid,
                                        @Nonnull Long location);

    /**
     * Returns the flows per weight class for a give endpoint.
     *
     * @param oid The OID of the entity.
     * @return result. Empty if no flows for an endpoint.
     */
    @Nonnull double[] getEndpointFlows(@Nonnull Long oid);

    /**
     * Returns the associated SEtoOID mapper.
     *
     * @return The associated SEtoOID mapper.
     */
    @Nonnull ServiceEntityOIDMapper getMapper();

    /**
     * Calculates sum of prices of all flows for a given consumer
     * based on its neighbors' distances from given provider.
     *
     * @param oid      Consumer oid
     * @param location provider oid where consumer is trying to be placed
     *                 Returns sum of prices of all flows of given consumer on provider
     * @return The price.
     */
    double calculatePrice(@Nonnull Long oid, @Nonnull Long location);

    /**
     * Sets the capacities for a provider.
     *
     * @param oid            The provider OID.
     * @param capacities     The capacities.
     * @param utilThresholds The utilization thresholds.
     */
    void setCapacities(@Nonnull Long oid, @Nonnull double[] capacities,
                       double[] utilThresholds);

    /**
     * Checks whether Matrix is empty.
     *
     * @return {@code true} iff the Matrix is empty.
     */
    boolean isEmpty();

    /**
     * Exports the matrix to the DTO.
     *
     * @param exporter The exporter.
     */
    void exportMatrix(@Nonnull Codec exporter);

    /**
     * Imports the Matrix from the DTO.
     * Will return the importer, which then can be used to import the matrix.
     * The importer will be successful iff all components it expects are received.
     * It is up to the caller to deal with the timeouts or missed messages,
     *
     * @return The importer.
     */
    @Nonnull Codec getMatrixImporter();

    /**
     * The Matrix component.
     */
    enum Component {
        OVERLAY,
        UNDERLAY,
        CONSUMER_2_PROVIDER
    }

    /**
     * The imported/exporter interface.
     * It will process one component at a time.
     * Out-of-order messages will be considered an error.
     */
    interface Codec {
        /**
         * Start the export.
         *
         * @param component The matrix component.
         * @throws IllegalStateException In case of an out-of-order or wrong message.
         */
        void start(@Nonnull Component component) throws IllegalStateException;

        /**
         * The next chunk.
         *
         * @param <T> The message type.
         * @param msg The message.
         * @throws IllegalStateException In case of an out-of-order or wrong message.
         */
        <T extends AbstractMessage> void next(@Nonnull T msg) throws IllegalStateException;

        /**
         * Finish the export.
         *
         * @throws IllegalStateException In case of an out-of-order or wrong message.
         */
        void finish() throws IllegalStateException;
    }
}
