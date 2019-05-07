package com.vmturbo.repository.graph.result;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;


import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.components.common.mapping.UIEntityState;
import com.vmturbo.repository.constant.RepoObjectType.RepoEntityType;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph.ResultVertex;

public class SubgraphResultUtilities {

    /**
     * Build a Resultvertex object.
     *
     * @param oid the oid of the entity.
     * @param entityType the type of the entity.
     * @param entityState State of the entity
     * @param provider the oid of the provider entity.
     * @param consumer the oid of the consumer entity.
     * @return the built entity.
     */

    public static ResultVertex createResultVertex(String oid,
                                                  String entityType,
                                                  String entityState,
                                                  String provider,
                                                  String consumer) {

        return new ResultVertex(oid, entityType, entityState, provider, consumer);
    }

    /**
     * Build a VM vertex.
     *
     * @param oid the oid of the entity.
     * @param provider the oid of the provider entity.
     * @param consumer the oid of the consumer entity.
     * @return the built entity.
     */
    public static ResultVertex vm(String oid,
                                  String provider,
                                  String consumer) {
        return new ResultVertex (oid,
                RepoEntityType.VIRTUAL_MACHINE.getValue(),
                UIEntityState.ACTIVE.getValue(),
                provider,
                consumer);
    }

    /**
     * Build a physical host vertex.
     *
     * @param oid the oid of the entity.
     * @param provider the oid of the provider entity.
     * @param consumer the oid of the consumer entity.
     * @return the built entity.
     */
    public static ResultVertex host(String oid,
                                  String provider,
                                  String consumer) {
        return new ResultVertex (oid,
                RepoEntityType.PHYSICAL_MACHINE.getValue(),
                UIEntityState.ACTIVE.getValue(),
                provider,
                consumer);
    }

    /**
     * Build a data center vertex.
     *
     * @param oid the oid of the entity.
     * @param provider the oid of the provider entity.
     * @param consumer the oid of the consumer entity.
     * @return the built entity.
     */
    public static ResultVertex dc(String oid,
                                  String provider,
                                  String consumer) {
        return new ResultVertex (oid,
                RepoEntityType.DATACENTER.getValue(),
                UIEntityState.ACTIVE.getValue(),
                provider,
                consumer);
    }

    /**
     * Build a storage vertex.
     *
     * @param oid the oid of the entity.
     * @param provider the oid of the provider entity.
     * @param consumer the oid of the consumer entity.
     * @return the built entity.
     */
    public static ResultVertex storage(String oid,
                                  String provider,
                                  String consumer) {
        return new ResultVertex (oid,
                RepoEntityType.STORAGE.getValue(),
                UIEntityState.ACTIVE.getValue(),
                provider,
                consumer);
    }

    /**
     * Build a disk array vertex.
     *
     * @param oid the oid of the entity.
     * @param provider the oid of the provider entity.
     * @param consumer the oid of the consumer entity.
     * @return the built entity.
     */
    public static ResultVertex da(String oid,
                                String provider,
                                String consumer) {
        return new ResultVertex (oid,
                RepoEntityType.DISKARRAY.getValue(),
                UIEntityState.ACTIVE.getValue(),
                provider,
                consumer);
    }

    /**
     * Build a logical pool vertex.
     *
     * @param oid the oid of the entity.
     * @param provider the oid of the provider entity.
     * @param consumer the oid of the consumer entity.
     * @return the built entity.
     */
    public static ResultVertex lp(String oid,
                                  String provider,
                                  String consumer) {
        return new ResultVertex (oid,
                RepoEntityType.LOGICALPOOL.getValue(),
                UIEntityState.ACTIVE.getValue(),
                provider,
                consumer);
    }

    public static Map<String, SupplyChainNode> nodeMapFor(@Nonnull final SupplyChainSubgraph subgraph) {
        return subgraph.toSupplyChainNodes().stream()
            .collect(Collectors.toMap(SupplyChainNode::getEntityType, Function.identity()));
    }
}
