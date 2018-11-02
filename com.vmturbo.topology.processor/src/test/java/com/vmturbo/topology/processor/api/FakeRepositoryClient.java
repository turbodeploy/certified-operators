package com.vmturbo.topology.processor.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.mockito.Mockito;

import com.google.common.collect.Lists;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesResponse.Builder;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.repository.api.RepositoryClient;

public class FakeRepositoryClient extends RepositoryClient {

    private final Map<Long, TopologyEntityDTO> entityMap = new HashMap<>();

    // Create a fake repository client, using a mock repository channel
    public FakeRepositoryClient() {
        super(Mockito.mock(Channel.class));
    }

    @Override
    public Iterator<RetrieveTopologyResponse> retrieveTopology(final long topologyId) {
        List<RetrieveTopologyResponse> response = Lists.newArrayList();
        response.add(RetrieveTopologyResponse.newBuilder()
                // Retrieve just the single entity for now -- update as needed for new tests
                .addEntities(entityMap.get(topologyId))
                .build());
        return response.iterator();
    }

    /**
     * Retrieve real time topology entities with provided OIDs
     *
     * @param oids              OIDs to retrieve topology entities
     * @param realtimeContextId real time context id
     * @return RetrieveTopologyEntitiesResponse
     */
    @Override
    public RetrieveTopologyEntitiesResponse retrieveTopologyEntities(@Nonnull final List<Long> oids, final long realtimeContextId) {
        Builder builder = RetrieveTopologyEntitiesResponse.newBuilder();
        oids.stream()
                .forEach(oid -> builder.addEntities(entityMap.get(oid)));
        return builder.build();
    }

    @Override
    public RepositoryOperationResponse deleteTopology(final long topologyId, final long topologyContextId) {
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
