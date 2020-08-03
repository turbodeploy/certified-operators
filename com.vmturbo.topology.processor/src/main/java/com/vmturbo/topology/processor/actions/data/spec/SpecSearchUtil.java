package com.vmturbo.topology.processor.actions.data.spec;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * A utility class to help make topology searches using the SearchRpcService
 * The constants defined in this class are mirrors of what is also defined in the API package --
 * we can't access those definitions here, without adding a dependency.
 * TODO: Move this code to some common component that both Topology Processor and the API can access
 */
public class SpecSearchUtil {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Search for TopologyEntityDTOs for a given request.
     */
    public static List<TopologyEntityDTO> searchTopologyEntityDTOs(
            @Nonnull final SearchParameters params,
            @Nonnull final SearchServiceBlockingStub searchServiceRpc) {
        try {
            return RepositoryDTOUtil.topologyEntityStream(searchServiceRpc.searchEntitiesStream(
                SearchEntitiesRequest.newBuilder()
                    .setReturnType(Type.FULL)
                    .setSearch(SearchQuery.newBuilder()
                        .addSearchParameters(params))
                    .build()))
                .map(PartialEntity::getFullEntity)
                .collect(Collectors.toList());
        } catch (StatusRuntimeException e) {
            logger.error("Error when getting TopologyEntityDTOs for request: {}", params, e);
            return Collections.emptyList();
        }
    }
}
