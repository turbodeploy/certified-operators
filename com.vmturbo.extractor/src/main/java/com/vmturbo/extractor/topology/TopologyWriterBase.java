package com.vmturbo.extractor.topology;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.Model;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Base class for a topology writer.
 */
public abstract class TopologyWriterBase implements ITopologyWriter {
    protected final ExecutorService pool;

    protected TopologyInfo topologyInfo;
    protected final DbEndpoint dbEndpoint;
    protected Map<Long, List<Grouping>> entityToGroups;
    protected MultiStageTimer timer;
    private final Model model;
    protected WriterConfig config;

    /**
     * Create a new instance.
     *
     * @param dbEndpoint a {@link DbEndpoint} for the database where extracted data will be
     *                   persisted
     * @param model      model containing tables that will take part
     * @param pool       thread pool for parallel operations
     */
    public TopologyWriterBase(DbEndpoint dbEndpoint, Model model, ExecutorService pool) {
        this.dbEndpoint = dbEndpoint;
        this.model = model;
        this.pool = pool;
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(
            final TopologyInfo topologyInfo, final Map<Long, List<Grouping>> entityToGroups,
            WriterConfig config, MultiStageTimer timer)
            throws IOException, UnsupportedDialectException, SQLException {
        this.topologyInfo = topologyInfo;
        this.entityToGroups = entityToGroups;
        this.config = config;
        this.timer = timer;
        return this::writeEntity;
    }

    protected abstract void writeEntity(TopologyEntityDTO entity);

    @Override
    public int finish(final Map<Long, Set<Long>> entityToRelated)
            throws UnsupportedDialectException, SQLException {
        return 0;
    }
}
