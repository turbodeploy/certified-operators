package com.vmturbo.extractor.topology;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
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
     * @param topologyInfo   the {@link TopologyInfo} for the topology
     * @param entityToGroups map linking each entity to the groups to which it belongs
     * @param writerConfig   writer config properties
     * @param timer          a {@link MultiStageTimer} to collect overall timing information
     * @return a consumer to which topology entities will be delivered for processing by this writer
     * @throws IOException                 if there's a problem setting up for processing
     * @throws UnsupportedDialectException if our database endpoint is mis-configured
     * @throws SQLException                if there's a problem establishing database access
     * @throws InterruptedException if interrupted
     */
    InterruptibleConsumer<TopologyEntityDTO> startTopology(
            TopologyInfo topologyInfo, Map<Long, List<Grouping>> entityToGroups,
            WriterConfig writerConfig, MultiStageTimer timer)
            throws IOException, UnsupportedDialectException, SQLException, InterruptedException;

    /**
     * Perform any final processing required after the whole topology has been processed.
     *
     * @param entityToRelatedEntities map relating each entity to to members of its "enhanced"
     *                                supply chain, including related groups
     * @return number of entities processed
     * @throws InterruptedException        if the operation is interrupted
     * @throws UnsupportedDialectException if the db endpoint is mis-configured
     * @throws SQLException                if there's a problem using the db endpoint
     */
    int finish(Map<Long, Set<Long>> entityToRelatedEntities)
            throws InterruptedException, UnsupportedDialectException, SQLException;

    /**
     * A {@link java.util.function.Consumer} that can throw {@link InterruptedException}.
     *
     * @param <T> consumed value type
     */
    @FunctionalInterface
    interface InterruptibleConsumer<T> {
        void accept(T item) throws InterruptedException;
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
