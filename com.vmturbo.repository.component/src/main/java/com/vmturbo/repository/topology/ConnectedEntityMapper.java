package com.vmturbo.repository.topology;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.repository.dto.ConnectedEntityRepoDTO;

/**
 * Map between {@link ConnectedEntity} protobuf and the ArangoDB bean {@link ConnectedEntityRepoDTO}
 **/
class ConnectedEntityMapper {

    public static ConnectedEntityRepoDTO convertToConnectedEntityRepoDTO(ConnectedEntity connectedEntity) {
        ConnectedEntityRepoDTO connectedEntityRepoDTO = new ConnectedEntityRepoDTO();

        if (connectedEntity.hasConnectedEntityId()) {
            connectedEntityRepoDTO.setConnectedEntityId(connectedEntity.getConnectedEntityId());
        }

        if (connectedEntity.hasConnectedEntityType()) {
            connectedEntityRepoDTO.setConnectedEntityType(connectedEntity.getConnectedEntityType());
        }

        if (connectedEntity.hasConnectionType()) {
            connectedEntityRepoDTO.setConnectionType(connectedEntity.getConnectionType().getNumber());
        }

        return connectedEntityRepoDTO;
    }

    public static ConnectedEntity convertToConnectedEntity(ConnectedEntityRepoDTO connectedEntityRepoDTO) {
        ConnectedEntity.Builder builder = ConnectedEntity.newBuilder();

        if (connectedEntityRepoDTO.getConnectedEntityId() != null) {
            builder.setConnectedEntityId(connectedEntityRepoDTO.getConnectedEntityId());
        }

        if (connectedEntityRepoDTO.getConnectedEntityType() != null) {
            builder.setConnectedEntityType(connectedEntityRepoDTO.getConnectedEntityType());
        }

        if (connectedEntityRepoDTO.getConnectionType() != null) {
            builder.setConnectionType(ConnectionType.forNumber(
                    connectedEntityRepoDTO.getConnectionType()));
        }

        return builder.build();
    }
}
