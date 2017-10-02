package com.vmturbo.repository.graph.operator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs;
import com.vmturbo.repository.exception.GraphDatabaseExceptions;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriver;
import com.vmturbo.repository.graph.parameter.VertexParameter;

/**
 * The PriceIndexGrapher implements updating of the graph database vertices
 * with the current and projected price index.
 */
public class TopologyGraphUpdator {

    private static final Logger logger = LoggerFactory.getLogger(TopologyGraphUpdator.class);

    /**
     * The database driver connector.
     */
    private final GraphDatabaseDriver graphDatabaseDriver;

    /**
     * The vertex collection to be passed to the driver connector calls.
     */
    private final VertexParameter vertexParameter;

    /**
     * The batch size.
     */
    static final int BATCH_SIZE = 100;

    /**
     * Constructs the the price index graph database interface (in the traditional sense, not Java).
     *
     * @param graphDatabaseDriver The database driver
     * @param graphDefinition     The graph definition
     */
    public TopologyGraphUpdator(final GraphDatabaseDriver graphDatabaseDriver,
                                final GraphDefinition graphDefinition) {
        this.graphDatabaseDriver =
                Objects.requireNonNull(Objects.requireNonNull(graphDatabaseDriver));
        GraphDefinition graphDefinition_ = Objects.requireNonNull(graphDefinition);
        this.vertexParameter =
                new VertexParameter.Builder(graphDefinition_.getServiceEntityVertex()).build();
    }

    /**
     * Updates price index data of entities in the associated graph
     *
     * @param marketID       The market id (not used yet)
     * @param topologyID     The topology id
     * @param priceIndexData The price index data to be applied
     */
    public void updateGraph(final long marketID, final long topologyID, final @Nonnull
            List<PriceIndexDTOs.PriceIndexMessagePayload> priceIndexData) {
        // TODO Refactor the code according to the new ArangoDB version
        Map<String, Map<String, Object>> values = new HashMap<>();
        int count = 0;
        for (PriceIndexDTOs.PriceIndexMessagePayload pi : priceIndexData) {
            String oid = String.valueOf(pi.getOid());
            Map<String, Object> value = values.get(oid);
            if (value == null) {
                value = new HashMap<>();
                values.put(oid, value);
            }
            value.put("priceIndex", pi.getPriceindexCurrent());
            value.put("priceIndexProjected", pi.getPriceindexProjected());
            if (count++ > BATCH_SIZE) {
                try {
                    graphDatabaseDriver.updateDocuments(vertexParameter, values);
                } catch (GraphDatabaseExceptions.GraphDatabaseException e) {
                    logger.error("Unable to update one or more the entries for topology " + topologyID, e);
                }
                count = 0;
                values = new HashMap<>();
            }
        }

        // Update the remainder
        if (!values.isEmpty()) {
            try {
                graphDatabaseDriver.updateDocuments(vertexParameter, values);
            } catch (GraphDatabaseExceptions.GraphDatabaseException e) {
                logger.error("Unable to update one or more the entries", e);
            }
        }
    }
}
