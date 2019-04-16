package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.tag.Tag.Tags;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.components.common.mapping.UIEntityState;
import com.vmturbo.components.common.mapping.UIEnvironmentType;
import com.vmturbo.repository.constant.RepoObjectType;
import com.vmturbo.repository.dto.CommoditiesBoughtRepoFromProviderDTO;
import com.vmturbo.repository.dto.CommoditySoldRepoDTO;
import com.vmturbo.repository.dto.ConnectedEntityRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.dto.TypeSpecificInfoRepoDTO;

/**
 * A Mapper class to convert {@link ServiceEntityRepoDTO} back to {@link TopologyEntityDTO}.
 * Because {@link ServiceEntityRepoDTO} only keep part of TopologyEntityDTO fields, returned
 * {@link TopologyEntityDTO} will also contains partial fields.
 */
public class ServiceEntityRepoDTOConverter {

    public static Set<TopologyEntityDTO> convertToTopologyEntityDTOs(
        Collection<ServiceEntityRepoDTO> serviceEntities) {
        return serviceEntities.stream()
            .map(ServiceEntityRepoDTOConverter::convertToTopologyEntityDTO)
            .collect(Collectors.toSet());
    }


    public static TopologyEntityDTO convertToTopologyEntityDTO(ServiceEntityRepoDTO serviceEntityRepoDTO) {
        TopologyEntityDTO.Builder topologyEntityBuilder = TopologyEntityDTO.newBuilder();
        topologyEntityBuilder.setOid(Long.valueOf(serviceEntityRepoDTO.getOid()));
        topologyEntityBuilder.setDisplayName(serviceEntityRepoDTO.getDisplayName());
        topologyEntityBuilder.setEntityType(RepoObjectType.toTopologyEntityType(
                serviceEntityRepoDTO.getEntityType()));
        topologyEntityBuilder.setEnvironmentType(
            UIEnvironmentType.fromString(serviceEntityRepoDTO.getEnvironmentType())
                .toEnvType()
                // We don't expect to have a HYBRID topology entity, so if we get a hybrid
                // environment type, assume something's wrong and put it as "UNKNOWN".
                .orElse(EnvironmentType.UNKNOWN_ENV));
        if (serviceEntityRepoDTO.getState() != null) {
            topologyEntityBuilder.setEntityState(
                UIEntityState.fromString(serviceEntityRepoDTO.getState()).toEntityState());
        }

        final Map<String, List<String>> tags = serviceEntityRepoDTO.getTags();
        if (tags != null) {
            final Tags.Builder tagsBuilder = Tags.newBuilder();
            tags.forEach((key, value) ->
                tagsBuilder.putTags(key, TagValuesDTO.newBuilder().addAllValues(value).build()));
            topologyEntityBuilder.setTags(tagsBuilder.build());
        }

        final List<CommoditySoldRepoDTO> soldCommodities = serviceEntityRepoDTO.getCommoditySoldList();
        if (soldCommodities != null) {
            topologyEntityBuilder.addAllCommoditySoldList(
                    soldCommodities.stream()
                            .map(CommodityMapper::convertToCommoditySoldDTO)
                            .collect(Collectors.toList()));
        }

        final List<CommoditiesBoughtRepoFromProviderDTO> boughtCommodities =
                serviceEntityRepoDTO.getCommoditiesBoughtRepoFromProviderDTOList();
        if (boughtCommodities != null) {
            topologyEntityBuilder.addAllCommoditiesBoughtFromProviders(
                    boughtCommodities.stream()
                            .map(CommodityMapper::convertToCommoditiesBoughtFromProvider)
                            .collect(Collectors.toList()));
        }

        final List<ConnectedEntityRepoDTO> connectedEntities = serviceEntityRepoDTO.getConnectedEntityList();
        if (connectedEntities != null) {
            topologyEntityBuilder.addAllConnectedEntityList(
                    connectedEntities.stream()
                            .map(ConnectedEntityMapper::convertToConnectedEntity)
                            .collect(Collectors.toList()));
        }

        // TODO: collapse separate typeSpecific InfoRepoDTO fields into a single field
        TypeSpecificInfoRepoDTO typeSpecificInfoRepoDTO = null;
        if (serviceEntityRepoDTO.getApplicationInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getApplicationInfoRepoDTO();
        } else if (serviceEntityRepoDTO.getDatabaseInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getDatabaseInfoRepoDTO();
        } else if (serviceEntityRepoDTO.getComputeTierInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getComputeTierInfoRepoDTO();
        } else if (serviceEntityRepoDTO.getVirtualMachineInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getVirtualMachineInfoRepoDTO();
        } else if (serviceEntityRepoDTO.getPhysicalMachineInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getPhysicalMachineInfoRepoDTO();
        } else if (serviceEntityRepoDTO.getStorageInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getStorageInfoRepoDTO();
        } else if (serviceEntityRepoDTO.getDiskArrayInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getDiskArrayInfoRepoDTO();
        } else if (serviceEntityRepoDTO.getLogicalPoolInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getLogicalPoolInfoRepoDTO();
        } else if (serviceEntityRepoDTO.getStorageControllerInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getStorageControllerInfoRepoDTO();
        } else if (serviceEntityRepoDTO.getVirtualVolumeInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getVirtualVolumeInfoRepoDTO();
        } else if (serviceEntityRepoDTO.getBusinessAccountInfoRepoDTO() != null) {
            typeSpecificInfoRepoDTO = serviceEntityRepoDTO.getBusinessAccountInfoRepoDTO();
        }

        // if present, convert this RepoDTO type specific info into Topology TypeSpecificInfo oneof
        // and store in the topologyEntityBuilder
        if (typeSpecificInfoRepoDTO != null) {
            topologyEntityBuilder.setTypeSpecificInfo(typeSpecificInfoRepoDTO
                    .createTypeSpecificInfo());
        }

        // set DiscoveryOrigin if any
        Optional.ofNullable(serviceEntityRepoDTO.getTargetIds()).ifPresent(targetIds ->
            topologyEntityBuilder.setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                    .addAllDiscoveringTargetIds(
                        serviceEntityRepoDTO.getTargetIds().stream()
                            .map(Long::valueOf)
                            .collect(Collectors.toList()))
                    .build())
                .build())
        );

        return topologyEntityBuilder.build();
    }

}
