package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.schema.Tables.TOPOLOGY_STATS;

import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.JSONB;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Writer for recording statistics of the topology (such as number of entities) to the topology_stats
 * table of the reporting database.
 */
public class TopologyStatsWriter extends TopologyWriterBase {

    private static final Logger logger = LogManager.getLogger();

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        // we want key order to be retained in attrs conversions, to prevent unneeded hash changes
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    /**
     * Create a new instance.
     *
     * @param dbEndpoint a {@link DbEndpoint} for the database where extracted data will be
     *                   persisted
     * @param pool       thread pool for parallel operations
     */
    public TopologyStatsWriter(final DbEndpoint dbEndpoint, final ExecutorService pool) {
        super(dbEndpoint, pool);
    }

    @Override
    protected void writeEntity(final TopologyEntityDTO entity) {
        // noop
    }

    @Override
    public int finish(final DataProvider dataProvider)
            throws UnsupportedDialectException, SQLException, InterruptedException {
        DSLContext dsl = dbEndpoint.dslContext();

        OffsetDateTime topologyTime = OffsetDateTime.ofInstant(
                Instant.ofEpochMilli(topologyInfo.getCreationTime()), ZoneOffset.UTC);
        String attrs = getTopologyAttrs(dataProvider.getTopologyGraph());

        dsl.insertInto(TOPOLOGY_STATS, TOPOLOGY_STATS.TIME, TOPOLOGY_STATS.ATTRS)
                .values(topologyTime, JSONB.valueOf(attrs))
                .execute();

        return dataProvider.getTopologyGraph().size();
    }

    /**
     * Takes the topology graph and return a JSON with the stats (or attributes) of the topology.
     * Currently we are only getting the total number of entities.
     *
     * @param graph topology graph
     * @return JSON string with the stats of the topology
     */
    @VisibleForTesting
    @Nullable
    String getTopologyAttrs(TopologyGraph<SupplyChainEntity> graph) {
        Map<String, Object> attrs = new HashMap<>();
        int numberOfEntities = graph.size();
        attrs.put("numberOfEntities", numberOfEntities);

        String attrStr = null;
        try {
            attrStr = mapper.writeValueAsString(attrs);
        } catch (JsonProcessingException e) {
            logger.error("Failed to create JSON string for topology attributes.", e);
        }

        return attrStr;
    }
}
