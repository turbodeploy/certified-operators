package com.vmturbo.extractor.topology;

import java.io.IOException;
import java.sql.SQLException;
import java.util.function.Consumer;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.search.metadata.SearchEntityMetadata;
import com.vmturbo.search.metadata.SearchGroupMetadata;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Interface for a writer participating in topology processing.
 */
public interface ITopologyWriter {

    /** ingestion phase label suffix. */
    String INGESTION_PHASE = "ingestion";
    /** finish phase label suffix. */
    String FINISH_PHASE = "finish";

    /**
     * Start processing a new topology ingestion.
     *
     * @param topologyInfo the {@link TopologyInfo} for the topology
     * @param writerConfig writer config properties
     * @param timer        a {@link MultiStageTimer} to collect overall timing information
     * @return a consumer to which topology entities will be delivered for processing by this writer
     * @throws IOException                 if there's a problem setting up for processing
     * @throws UnsupportedDialectException if our database endpoint is mis-configured
     * @throws SQLException                if there's a problem establishing database access
     * @throws InterruptedException        if we're interrupted
     */
    Consumer<TopologyEntityDTO> startTopology(
            TopologyInfo topologyInfo, WriterConfig writerConfig, MultiStageTimer timer)
            throws IOException, UnsupportedDialectException, SQLException, InterruptedException;

    /**
     * Whether or not we need the supply chain for all entities in the topology. This is needed
     * for performance improvement. If search is enabled but reporting is not, which is usually
     * the case for large customers, we only need to calculate supply chain for those entities
     * defined in {@link SearchEntityMetadata} and {@link SearchGroupMetadata}, not all entities
     * in the topology.
     *
     * @return false by default; true if the writer (like reporting) requires all
     */
    default boolean requireSupplyChainForAllEntities() {
        return false;
    }

    /**
     * Perform any final processing required after the whole topology has been processed.
     *
     * @param dataProvider object containing all different aspects of data for entity and group
     *                     needed for ingestion, like supply chain, actions, severity, etc.
     * @return number of entities processed
     * @throws InterruptedException        if the operation is interrupted
     * @throws UnsupportedDialectException if the db endpoint is mis-configured
     * @throws SQLException                if there's a problem using the db endpoint
     */
    default int finish(DataProvider dataProvider)
            throws InterruptedException, UnsupportedDialectException, SQLException {
        return 0;
    }

    /**
     * Return a label for ingestion processing by the given writer, for logging.
     *
     * @param writer the writer
     * @return ingestion phase label
     */
    static String getIngestionPhaseLabel(ITopologyWriter writer) {
        return writer.getClass().getSimpleName() + " " + INGESTION_PHASE;
    }

    /**
     * Return a label for finish processing by the given writer, for logging.
     *
     * @param writer the writer
     * @return finish phase label
     */
    static String getFinishPhaseLabel(ITopologyWriter writer) {
        return writer.getClass().getSimpleName() + " " + FINISH_PHASE;
    }
}
