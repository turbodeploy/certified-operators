package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.TypeCase;
import com.vmturbo.components.common.mapping.UIEntityState;
import com.vmturbo.components.common.mapping.UIEnvironmentType;
import com.vmturbo.repository.constant.RepoObjectType;
import com.vmturbo.repository.dto.ApplicationInfoRepoDTO;
import com.vmturbo.repository.dto.BusinessAccountInfoRepoDTO;
import com.vmturbo.repository.dto.CommoditiesBoughtRepoFromProviderDTO;
import com.vmturbo.repository.dto.ComputeTierInfoRepoDTO;
import com.vmturbo.repository.dto.DatabaseInfoRepoDTO;
import com.vmturbo.repository.dto.DiskArrayInfoRepoDTO;
import com.vmturbo.repository.dto.LogicalPoolInfoRepoDTO;
import com.vmturbo.repository.dto.PhysicalMachineInfoRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.dto.StorageControllerInfoRepoDTO;
import com.vmturbo.repository.dto.StorageInfoRepoDTO;
import com.vmturbo.repository.dto.TypeSpecificInfoRepoDTO;
import com.vmturbo.repository.dto.VirtualMachineInfoRepoDTO;
import com.vmturbo.repository.dto.VirtualVolumeInfoRepoDTO;

/**
 * Map TopologyEntityDTO to ServiceEntityRepoDTO.
 **/
class TopologyEntityDTOConverter {

    private final static Logger logger = LogManager.getLogger();

    private static final Map<TypeCase, Class> TYPE_TO_REPO_DTO_CLASS_MAP =
            new ImmutableMap.Builder<TypeCase, Class>()
                    .put(TypeCase.APPLICATION, ApplicationInfoRepoDTO.class)
                    .put(TypeCase.BUSINESS_ACCOUNT, BusinessAccountInfoRepoDTO.class)
                    .put(TypeCase.DATABASE, DatabaseInfoRepoDTO.class)
                    .put(TypeCase.COMPUTE_TIER, ComputeTierInfoRepoDTO.class)
                    .put(TypeCase.STORAGE, StorageInfoRepoDTO.class)
                    .put(TypeCase.DISK_ARRAY, DiskArrayInfoRepoDTO.class)
                    .put(TypeCase.LOGICAL_POOL, LogicalPoolInfoRepoDTO.class)
                    .put(TypeCase.STORAGE_CONTROLLER, StorageControllerInfoRepoDTO.class)
                    .put(TypeCase.VIRTUAL_VOLUME, VirtualVolumeInfoRepoDTO.class)
                    .put(TypeCase.VIRTUAL_MACHINE, VirtualMachineInfoRepoDTO.class)
                    .put(TypeCase.PHYSICAL_MACHINE, PhysicalMachineInfoRepoDTO.class)
                    .build();


    public static Set<ServiceEntityRepoDTO> convertToServiceEntityRepoDTOs(
            Collection<TopologyEntityDTO> topologyDTOs) {
        return topologyDTOs.stream()
                .map(TopologyEntityDTOConverter::convertToServiceEntityRepoDTO)
                .collect(Collectors.toSet());
    }

    public static ServiceEntityRepoDTO convertToServiceEntityRepoDTO(TopologyEntityDTO t) {
        String seOid = Long.toString(t.getOid());
        ServiceEntityRepoDTO se = new ServiceEntityRepoDTO();
        se.setOid(seOid);
        se.setDisplayName(t.getDisplayName());
        se.setEntityType(RepoObjectType.mapEntityType(t.getEntityType()));
        se.setEnvironmentType(UIEnvironmentType.fromEnvType(t.getEnvironmentType()).getApiEnumStringValue());
        se.setUuid(String.valueOf(t.getOid()));
        se.setState(UIEntityState.fromEntityState(t.getEntityState()).getValue());
        se.setTags(new HashMap<>());
        t.getTags().getTagsMap().forEach((key, value) -> se.getTags().put(key, value.getValuesList()));

        // Commodities bought list
        List<CommoditiesBoughtRepoFromProviderDTO> commoditiesBoughtRepoFromProviderDTOList =
                Lists.newArrayList();
        t.getCommoditiesBoughtFromProvidersList().forEach(commoditiesBoughtFromProvider ->
                commoditiesBoughtRepoFromProviderDTOList.add(
                        CommodityMapper.convertToRepoBoughtFromProviderDTO(commoditiesBoughtFromProvider, seOid)));

        se.setCommoditiesBoughtRepoFromProviderDTOList(commoditiesBoughtRepoFromProviderDTOList);

        // connected entity list
        se.setConnectedEntityList(t.getConnectedEntityListList().stream()
                .map(ConnectedEntityMapper::convertToConnectedEntityRepoDTO)
                .collect(Collectors.toList()));

        // Only set the valid provider list
        se.setProviders(commoditiesBoughtRepoFromProviderDTOList.stream().filter(
                commoditiesBoughtRepoFromProviderDTO ->
                        commoditiesBoughtRepoFromProviderDTO.getProviderId() != null)
                .map(grouping -> String.valueOf(grouping.getProviderId()))
                .collect(Collectors.toList()));

        // Commodities sold list
        se.setCommoditySoldList(t.getCommoditySoldListList().stream().map(comm ->
                CommodityMapper.convertToCommoditySoldRepoDTO(seOid, comm)).collect(Collectors.toList()));

        // save discovering target ids
        if (t.hasOrigin() && t.getOrigin().hasDiscoveryOrigin()) {
            se.setTargetIds(t.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList().stream()
                .map(String::valueOf)
                .collect(Collectors.toList()));
        }

        if (t.hasTypeSpecificInfo()) {
            final TypeSpecificInfo typeSpecificInfo = t.getTypeSpecificInfo();
            Optional.ofNullable(TYPE_TO_REPO_DTO_CLASS_MAP.get(typeSpecificInfo.getTypeCase()))
                .ifPresent(infoRepoDtoClass -> {
                    try {
                        ((TypeSpecificInfoRepoDTO) infoRepoDtoClass.newInstance())
                            .fillFromTypeSpecificInfo(typeSpecificInfo, se);
                    } catch (InstantiationException | IllegalAccessException e) {
                        logger.warn("Error instantiating TypeSpecificInfoRepoDTO for: " +
                            typeSpecificInfo);
                    }
                });
        }
        return se;
    }
}
