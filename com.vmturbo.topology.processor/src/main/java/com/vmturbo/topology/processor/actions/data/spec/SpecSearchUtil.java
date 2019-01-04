package com.vmturbo.topology.processor.actions.data.spec;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.search.Search.SearchTopologyEntityDTOsRequest;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * A utility class to help make topology searches using the SearchRpcService
 * The constants defined in this class are mirrors of what is also defined in the API package --
 * we can't access those definitions here, without adding a dependency.
 * TODO: Move this code to some common component that both Topology Processor and the API can access
 */
public class SpecSearchUtil {

    private static final Logger logger = LogManager.getLogger();

    // Constants used for searching the topology

    // This is defined in SearchMapper.DISPLAY_NAME_PROPERTY, but that is not reachable from the
    // Topology Processor
    public static final String DISPLAY_NAME = "displayName";

    // This is defined in SearchMapper.ENTITY_TYPE_PROPERTY, but that is not reachable from the
    // Topology Processor
    public static final String ENTITY_TYPE = "entityType";

    // This is defined in GroupMapper.OID, but that is not reachable from the Topology Processor
    public static final String OID = "oid";

    /**
     * Search for TopologyEntityDTOs for a given request.
     */
    public static List<TopologyEntityDTO> searchTopologyEntityDTOs(
            @Nonnull final SearchTopologyEntityDTOsRequest request,
            @Nonnull final SearchServiceBlockingStub searchServiceRpc) {
        try {
            return searchServiceRpc.searchTopologyEntityDTOs(request).getTopologyEntityDtosList();
        } catch (Exception e) {
            logger.error("Error when getting TopologyEntityDTOs for request: {}", request, e);
            return Collections.emptyList();
        }
    }
}
