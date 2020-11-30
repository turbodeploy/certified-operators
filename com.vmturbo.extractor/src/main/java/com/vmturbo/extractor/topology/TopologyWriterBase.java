package com.vmturbo.extractor.topology;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Base class for a topology writer.
 */
public abstract class TopologyWriterBase implements ITopologyWriter {
    protected final ExecutorService pool;

    protected TopologyInfo topologyInfo;
    protected String topologyLabel;
    protected final DbEndpoint dbEndpoint;
    protected MultiStageTimer timer;
    protected WriterConfig config;

    /**
     * Create a new instance.
     *
     * @param dbEndpoint a {@link DbEndpoint} for the database where extracted data will be
     *                   persisted
     * @param pool       thread pool for parallel operations
     */
    public TopologyWriterBase(DbEndpoint dbEndpoint, ExecutorService pool) {
        this.dbEndpoint = dbEndpoint;
        this.pool = pool;
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(
            final TopologyInfo topologyInfo, WriterConfig config, MultiStageTimer timer)
            throws IOException, UnsupportedDialectException, SQLException, InterruptedException {
        this.topologyInfo = topologyInfo;
        this.topologyLabel = TopologyDTOUtil.getSourceTopologyLabel(topologyInfo);
        this.config = config;
        this.timer = timer;
        return this::writeEntity;
    }

    protected abstract void writeEntity(TopologyEntityDTO entity);

    @Override
    public int finish(final DataProvider entityToRelated)
            throws UnsupportedDialectException, SQLException, InterruptedException {
        return 0;
    }
}
