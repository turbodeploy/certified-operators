package com.vmturbo.cost.calculation.topology;

import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.group.api.GroupMemberRetriever;

/**
 * A factory to create instances of {@link TopologyEntityCloudTopology}.
 */
public interface TopologyEntityCloudTopologyFactory {

    /**
     * Create a new {@link TopologyEntityCloudTopology} out of a stream of {@link TopologyEntityDTO}s.
     *
     * @param entities The entities in the cloud topology. The factory may filter out non-cloud
     *                 entities.
     * @return A {@link TopologyEntityCloudTopology} containing the cloud subset of the entities.
     */
    @Nonnull
    TopologyEntityCloudTopology newCloudTopology(@Nonnull Stream<TopologyEntityDTO> entities);

    /**
     * Create a new {@link TopologyEntityCloudTopology} out of a {@link RemoteIterator}.
     *
     * @param topologyContextId The topology context Id
     * @param entities The {@link RemoteIterator} over the entities in the cloud topology.
     *                 The factory may filter out non-cloud entities.
     * @return A {@link TopologyEntityCloudTopology} containing the cloud subset of the entities.
     */
    @Nonnull
    TopologyEntityCloudTopology newCloudTopology(long topologyContextId, @Nonnull RemoteIterator<TopologyDTO.Topology.DataSegment> entities);

    /**
     * The default implementation of {@link TopologyEntityCloudTopologyFactory}.
     */
    class DefaultTopologyEntityCloudTopologyFactory implements TopologyEntityCloudTopologyFactory {

        private static final Logger logger = LogManager.getLogger();

        private final GroupMemberRetriever groupMemberRetriever;

        /**
         * Creates an instance of DefaultTopologyEntityCloudTopologyFactory.
         *
         * @param groupMemberRetriever service end point to retrieve billing family group
         *                             information.
         */
        public DefaultTopologyEntityCloudTopologyFactory(
                final GroupMemberRetriever groupMemberRetriever) {
            this.groupMemberRetriever = groupMemberRetriever;
        }

        /**
         *  {@inheritDoc}
         */
        @Nonnull
        @Override
        public TopologyEntityCloudTopology newCloudTopology(
                @Nonnull final Stream<TopologyEntityDTO> entities) {
            return new TopologyEntityCloudTopology(
                    entities.filter(this::isCloudEntity), groupMemberRetriever);
        }

        /**
         *  {@inheritDoc}
         */
        @Nonnull
        @Override
        public TopologyEntityCloudTopology newCloudTopology(final long topologyContextId,
                                                            @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entities) {
            final Stream.Builder<TopologyEntityDTO> streamBuilder = Stream.builder();
            try {
                while (entities.hasNext()) {
                    entities.nextChunk().stream()
                            .filter(TopologyDTO.Topology.DataSegment::hasEntity)
                            .map(TopologyDTO.Topology.DataSegment::getEntity)
                            .filter(this::isCloudEntity)
                            .forEach(streamBuilder);
                }
            } catch (TimeoutException | CommunicationException  e) {
                logger.error("Error retrieving topology in context " + topologyContextId, e);
            } catch (InterruptedException ie) {
                logger.error("Thread interrupted while processing topology in context " + topologyContextId, ie);
            }
            return new TopologyEntityCloudTopology(streamBuilder.build(), groupMemberRetriever);
        }

        private boolean isCloudEntity(@Nonnull final TopologyEntityDTO entity) {
            return entity.getEnvironmentType() == EnvironmentType.CLOUD;
        }
    }
}
