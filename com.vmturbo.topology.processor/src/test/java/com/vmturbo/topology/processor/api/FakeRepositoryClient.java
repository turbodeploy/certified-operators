package com.vmturbo.topology.processor.api;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.mockito.Mockito;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.repository.api.RepositoryClient;

public class FakeRepositoryClient extends RepositoryClient {

    private final Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();

    // The  realtime topology context Id.
    private static final Long realtimeTopologyContextId = 777777L;

    // Create a fake repository client, using a mock repository channel.
    // Pass in the real-time context id.
    public FakeRepositoryClient() {
        super(Mockito.mock(Channel.class), realtimeTopologyContextId);
    }

    @Override
    public Iterator<RetrieveTopologyResponse> retrieveTopology(final long topologyId) {
        List<RetrieveTopologyResponse> response = Lists.newArrayList();
        response.add(RetrieveTopologyResponse.newBuilder()
                // Retrieve just the single entity for now -- update as needed for new tests
                .addEntities(PartialEntity.newBuilder().setFullEntity(entityMap.get(topologyId)))
                .build());
        return response.iterator();
    }

    /**
     * Retrieve real time topology entities with provided OIDs
     *
     * @param oids              OIDs to retrieve topology entities
     * @param realtimeContextId real time context id
     * @return a stream of {@link TopologyEntityDTO}
     */
    @Override
    public Stream<TopologyEntityDTO> retrieveTopologyEntities(@Nonnull final List<Long> oids, final long realtimeContextId) {
        return oids.stream()
                .map(oid -> entityMap.get(oid));
    }

    @Override
    public RepositoryOperationResponse deleteTopology(final long topologyId,
                                                      final long topologyContextId,
                                                      final TopologyType topologyType) {
        entityMap.put(topologyId, null);
        return RepositoryOperationResponse.newBuilder()
                .setResponseCode(RepositoryOperationResponseCode.OK)
                .build();
    }

    /**
     * Add entities to this fake repository client, so that they will be found when searched for
     *
     * @param topologyEntityDTOs a list of Topology entities to use as search responses
     */
    public void addEntities(List<TopologyEntityDTO> topologyEntityDTOs) {
        topologyEntityDTOs.stream()
                .forEach(topologyEntityDTO ->
                        entityMap.put(topologyEntityDTO.getOid(), topologyEntityDTO));
    }
}
