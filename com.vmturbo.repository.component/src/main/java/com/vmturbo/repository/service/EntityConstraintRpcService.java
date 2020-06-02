package com.vmturbo.repository.service;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.repository.EntityConstraintOuterClass.CurrentPlacement;
import com.vmturbo.common.protobuf.repository.EntityConstraintOuterClass.EntityConstraint;
import com.vmturbo.common.protobuf.repository.EntityConstraintOuterClass.EntityConstraintRequest;
import com.vmturbo.common.protobuf.repository.EntityConstraintOuterClass.EntityConstraintResponse;
import com.vmturbo.common.protobuf.repository.EntityConstraintOuterClass.PotentialPlacements;
import com.vmturbo.common.protobuf.repository.EntityConstraintOuterClass.RelationType;
import com.vmturbo.common.protobuf.repository.EntityConstraintServiceGrpc.EntityConstraintServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This service provides RPCs for getting entity constraints.
 */
public class EntityConstraintRpcService extends EntityConstraintServiceImplBase {

    /**
     * A service that is used to get constraints by entity oid.
     *
     * @param request an entity constraint request that contains an entity oid
     * @param responseObserver An observer on which to write entity constraints.
     */
    @Override
    public void getConstraintsByEntityOid(EntityConstraintRequest request,
                                          StreamObserver<EntityConstraintResponse> responseObserver) {
        // The following code is used only for test purpose. Will be replaced by the correct logic.
        final EntityConstraint.Builder entityConstraint = EntityConstraint.newBuilder();
        entityConstraint.setEntityType(EntityType.PHYSICAL_MACHINE_VALUE);
        entityConstraint.setRelationType(RelationType.BOUGHT);
        entityConstraint.setNumPotentialPlacements(10);
        entityConstraint.setCurrentPlacement(CurrentPlacement.newBuilder().setDisplayName("host1").build());
        entityConstraint.addPotentialPlacements(PotentialPlacements.newBuilder()
            .setScopeDisplayName("HawthorneSales\\Turbonomic SE Lab")
            .setNumPotentialPlacements(1)
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.CLUSTER_VALUE).setKey("cluster")));
        entityConstraint.addPotentialPlacements(PotentialPlacements.newBuilder()
            .setScopeDisplayName("NetworkCommodity/cert16 - ACE Pro Source")
            .setNumPotentialPlacements(2)
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.NETWORK_VALUE).setKey("network")));
        entityConstraint.addPotentialPlacements(PotentialPlacements.newBuilder()
            .setScopeDisplayName("HawthorneSales")
            .setNumPotentialPlacements(3)
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.DATACENTER_VALUE).setKey("datacenter")));

        final EntityConstraintResponse.Builder response = EntityConstraintResponse.newBuilder();
        response.addEntityConstraint(entityConstraint);
        response.addEntityConstraint(entityConstraint);
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }
}
