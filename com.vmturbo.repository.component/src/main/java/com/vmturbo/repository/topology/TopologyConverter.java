package com.vmturbo.repository.topology;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.TagValuesDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.IpAddressInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.repository.constant.RepoObjectState;
import com.vmturbo.repository.constant.RepoObjectType;
import com.vmturbo.repository.dto.CommoditiesBoughtRepoFromProviderDTO;
import com.vmturbo.repository.dto.CommodityBoughtRepoDTO;
import com.vmturbo.repository.dto.CommoditySoldRepoDTO;
import com.vmturbo.repository.dto.ConnectedEntityRepoDTO;
import com.vmturbo.repository.dto.IpAddressRepoDTO;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.dto.VirtualMachineInfoRepoDTO;

/**
 * Convert topology DTOs to repository DTOs. And also convert repository DTOs to topology DTOs.
 */
public class TopologyConverter {

    public static Set<ServiceEntityRepoDTO> convert(Collection<TopologyEntityDTO> topologyDTOs) {
        return topologyDTOs.stream().map(ServiceEntityMapper::convert).collect(Collectors.toSet());
    }

    public static Set<TopologyEntityDTO> convertToTopologyEntity(Collection<ServiceEntityRepoDTO> serviceEntities) {
        return serviceEntities.stream().map(TopologyEntityMapper::convert).collect(Collectors.toSet());
    }

    /**
     * A Mapper class to convert {@link ServiceEntityRepoDTO} back to {@link TopologyEntityDTO}.
     * Because {@link ServiceEntityRepoDTO} only keep part of TopologyEntityDTO fields, returned
     * {@link TopologyEntityDTO} will also contains partial fields.
     */
    static class TopologyEntityMapper {
        static TopologyEntityDTO convert(ServiceEntityRepoDTO serviceEntityDTO) {
            TopologyEntityDTO.Builder topologyEntityBuilder = TopologyEntityDTO.newBuilder();
            topologyEntityBuilder.setOid(Long.valueOf(serviceEntityDTO.getOid()));
            topologyEntityBuilder.setDisplayName(serviceEntityDTO.getDisplayName());
            topologyEntityBuilder.setEntityType(mapEntityType(serviceEntityDTO.getEntityType()));
            topologyEntityBuilder.setEntityState(
                    EntityState.forNumber(mapEntityState(serviceEntityDTO.getState())));
            serviceEntityDTO.getTags().entrySet().forEach(
                    t ->
                            topologyEntityBuilder.putTags(
                                    t.getKey(),
                                    TagValuesDTO.newBuilder().addAllValues(t.getValue()).build()));
            topologyEntityBuilder.addAllCommoditySoldList(
                    serviceEntityDTO.getCommoditySoldList().stream()
                            .map(CommodityMapper::convert)
                            .collect(Collectors.toList()));
            topologyEntityBuilder.addAllCommoditiesBoughtFromProviders(
                    serviceEntityDTO.getCommoditiesBoughtRepoFromProviderDTOList().stream()
                            .map(CommodityMapper::convert)
                            .collect(Collectors.toList()));
            topologyEntityBuilder.addAllConnectedEntityList(
                    serviceEntityDTO.getConnectedEntityList().stream()
                            .map(ConnectedEntityMapper::convert)
                            .collect(Collectors.toList()));

            Optional.ofNullable(serviceEntityDTO.getVirtualMachineInfo()).ifPresent(
                    virtualMachineInfoRepoDTO -> {
                        VirtualMachineInfo.Builder vmBuilder = VirtualMachineInfo.newBuilder();
                        TypeSpecificInfo.Builder typeSpecificInfoBuilder =
                                TypeSpecificInfo.newBuilder();
                        if (virtualMachineInfoRepoDTO.getIpAddressInfoList() != null) {
                            virtualMachineInfoRepoDTO
                                    .getIpAddressInfoList().stream()
                                    .filter(ipAddressRepoDTO ->
                                            ipAddressRepoDTO.getIpAddress() != null)
                                    .map(ipAddressDTO -> IpAddressInfo.newBuilder()
                                            .setIpAddress(ipAddressDTO.getIpAddress())
                                            .setElastic(ipAddressDTO.getElastic())
                                            .build())
                                    .forEach(ipAddressInfo ->
                                            vmBuilder.addIpAddresses(ipAddressInfo));
                        }
                        if (vmBuilder.getGuestOsType() != null) {
                            vmBuilder.setGuestOsType(OSType.valueOf(
                                    virtualMachineInfoRepoDTO.getGuestOsType()));
                        }
                        if (virtualMachineInfoRepoDTO.getTenancy() != null) {
                            vmBuilder.setTenancy(Tenancy.valueOf(
                                    virtualMachineInfoRepoDTO.getTenancy()));
                        }
                        topologyEntityBuilder
                                .setTypeSpecificInfo(typeSpecificInfoBuilder
                                        .setVirtualMachine(vmBuilder));

                    });

            // set DiscoveryOrigin if any
            Optional.ofNullable(serviceEntityDTO.getTargetIds()).ifPresent(targetIds ->
                    topologyEntityBuilder.setOrigin(Origin.newBuilder()
                            .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                                    .addAllDiscoveringTargetIds(serviceEntityDTO.getTargetIds())
                                    .build())
                            .build())
            );

            return topologyEntityBuilder.build();
        }

        static int mapEntityType(String type) {
            return RepoObjectType.toTopologyEntityType(type);
        }

        static int mapEntityState(String state) {
            return RepoObjectState.toTopologyEntityState(state);
        }
    }

    static class ServiceEntityMapper {

        private static ServiceEntityRepoDTO convert(TopologyEntityDTO t) {
            String seOid = Long.toString(t.getOid());
            ServiceEntityRepoDTO se = new ServiceEntityRepoDTO();
            se.setOid(seOid);
            se.setDisplayName(t.getDisplayName());
            se.setEntityType(mapEntityType(t.getEntityType()));
            se.setUuid(String.valueOf(t.getOid()));
            se.setState(mapEntityState(t.getEntityState()));
            se.setTags(new HashMap<>());
            t.getTagsMap().entrySet().forEach(
                    tag -> se.getTags().put(tag.getKey(), tag.getValue().getValuesList()));

            // Commodities bought list
            List<CommoditiesBoughtRepoFromProviderDTO> commoditiesBoughtRepoFromProviderDTOList = Lists.newArrayList();
            t.getCommoditiesBoughtFromProvidersList().forEach(commoditiesBoughtFromProvider -> {
                commoditiesBoughtRepoFromProviderDTOList.add(
                    CommodityMapper.convert(commoditiesBoughtFromProvider, seOid));
            });

            se.setCommoditiesBoughtRepoFromProviderDTOList(commoditiesBoughtRepoFromProviderDTOList);

            // connected entity list
            se.setConnectedEntityList(t.getConnectedEntityListList().stream()
                    .map(ConnectedEntityMapper::convert)
                    .collect(Collectors.toList()));

            // Only set the valid provider list
            se.setProviders(commoditiesBoughtRepoFromProviderDTOList.stream().filter(
                    commoditiesBoughtRepoFromProviderDTO -> commoditiesBoughtRepoFromProviderDTO.getProviderId() != null)
                    .map(grouping -> String.valueOf(grouping.getProviderId()))
                    .collect(Collectors.toList()));

            // Commodities sold list
            se.setCommoditySoldList(t.getCommoditySoldListList().stream().map(comm ->
                    CommodityMapper.convert(seOid, seOid, comm)).collect(Collectors.toList()));

            // save discovering target ids
            if (t.hasOrigin() && t.getOrigin().hasDiscoveryOrigin()) {
                se.setTargetIds(t.getOrigin().getDiscoveryOrigin().getDiscoveringTargetIdsList());
            }

            // save VirtualMachineInfo
            if (t.hasTypeSpecificInfo() && t.getTypeSpecificInfo().hasVirtualMachine()) {
                VirtualMachineInfo vmInfo = t.getTypeSpecificInfo().getVirtualMachine();
                se.setVirtualMachineInfo(new VirtualMachineInfoRepoDTO(
                        vmInfo.hasGuestOsType() ? vmInfo.getGuestOsType().toString() : null,
                        vmInfo.hasTenancy() ? vmInfo.getTenancy().toString() : null,
                        vmInfo.getIpAddressesList().stream()
                        .map(ipAddrInfo -> new IpAddressRepoDTO(ipAddrInfo.getIpAddress(),
                                ipAddrInfo.getElastic()))
                        .collect(Collectors.toList())));
            }
            return se;
        }

        static String mapEntityType(int type) {
            return RepoObjectType.mapEntityType(type);
        }

        /**
         * Maps the entity state from the one used in TopologyDTO to
         * the one expected in UI.
         * TODO: This is a temporary fix, see OM-11305.
         *
         * @param topologyEntityState
         * @return A string of entity state that UI expects
         */
        static String mapEntityState(EntityState topologyEntityState) {
            return RepoObjectState.toRepoEntityState(topologyEntityState);
        }
    }


    static class CommodityMapper {
        private static CommodityBoughtRepoDTO convert(
                String ownerOid,
                String providerOid,
                CommodityBoughtDTO comm) {
            CommodityBoughtRepoDTO commRepo = new CommodityBoughtRepoDTO();

            commRepo.setUuid(UUID.randomUUID().toString());
            commRepo.setProviderOid(providerOid);
            commRepo.setOwnerOid(ownerOid);
            commRepo.setType(mapCommodityType(comm.getCommodityType().getType()));

            commRepo.setKey(comm.getCommodityType().getKey());
            commRepo.setUsed(comm.getUsed());
            commRepo.setPeak(comm.getPeak());

            return commRepo;
        }

        private static CommodityBoughtDTO convert(CommodityBoughtRepoDTO commodityBoughtRepoDTO) {
            CommodityBoughtDTO.Builder commodityBoughtBuilder = CommodityBoughtDTO.newBuilder();
            commodityBoughtBuilder.setUsed(commodityBoughtRepoDTO.getUsed());
            commodityBoughtBuilder.setPeak(commodityBoughtRepoDTO.getPeak());
            CommodityType.Builder commodityTypeBuilder = CommodityType.newBuilder();
            if (commodityBoughtRepoDTO.getType() != null) {
                commodityTypeBuilder.setType(mapCommodityType(commodityBoughtRepoDTO.getType()));
            }
            if (commodityBoughtRepoDTO.getKey() != null) {
                commodityTypeBuilder.setKey(commodityBoughtRepoDTO.getKey());
            }
            commodityBoughtBuilder.setCommodityType(commodityTypeBuilder);
            return commodityBoughtBuilder.build();
        }

        private static CommoditySoldRepoDTO convert(
                String ownerOid,
                String providerOid,
                CommoditySoldDTO comm) {
            CommoditySoldRepoDTO commRepo = new CommoditySoldRepoDTO();

            commRepo.setUuid(UUID.randomUUID().toString());
            commRepo.setProviderOid(providerOid);
            commRepo.setOwnerOid(ownerOid);
            commRepo.setType(mapCommodityType(comm.getCommodityType().getType()));

            commRepo.setKey(comm.getCommodityType().getKey());
            commRepo.setUsed(comm.getUsed());
            commRepo.setPeak(comm.getPeak());

            commRepo.setCapacity(comm.getCapacity());
            commRepo.setEffectiveCapacityPercentage(comm.getEffectiveCapacityPercentage());
            commRepo.setReservedCapacity(comm.getReservedCapacity());
            commRepo.setResizeable(comm.getIsResizeable());
            commRepo.setThin(comm.getIsThin());
            commRepo.setCapacityIncrement(comm.getCapacityIncrement());
            commRepo.setMaxQuantity(comm.getMaxQuantity());
            return commRepo;
        }

        private static CommoditySoldDTO convert(CommoditySoldRepoDTO commoditySoldRepoDTO) {
            CommoditySoldDTO.Builder commoditySoldDTOBuilder = CommoditySoldDTO.newBuilder();
            commoditySoldDTOBuilder.setUsed(commoditySoldRepoDTO.getUsed());
            commoditySoldDTOBuilder.setPeak(commoditySoldRepoDTO.getPeak());
            commoditySoldDTOBuilder.setCapacity(commoditySoldRepoDTO.getCapacity());
            commoditySoldDTOBuilder.setEffectiveCapacityPercentage(
                    commoditySoldRepoDTO.getEffectiveCapacityPercentage());
            commoditySoldDTOBuilder.setReservedCapacity(commoditySoldRepoDTO.getReservedCapacity());
            commoditySoldDTOBuilder.setIsResizeable(commoditySoldRepoDTO.isResizeable());
            commoditySoldDTOBuilder.setIsThin(commoditySoldRepoDTO.isThin());
            CommodityType.Builder commodityTypeBuilder = CommodityType.newBuilder();

            if (commoditySoldRepoDTO.getType() != null) {
                commodityTypeBuilder.setType(mapCommodityType(commoditySoldRepoDTO.getType()));
            }
            if (commoditySoldRepoDTO.getKey() != null) {
                commodityTypeBuilder.setKey(commoditySoldRepoDTO.getKey());
            }

            commoditySoldDTOBuilder.setCommodityType(commodityTypeBuilder.build());
            return commoditySoldDTOBuilder.build();
        }

        private static String mapCommodityType(int type) {
            return RepoObjectType.mapCommodityType(type);
        }

        private static int mapCommodityType(String type) {
            return RepoObjectType.mapCommodityType(type);
        }

        private static CommoditiesBoughtRepoFromProviderDTO convert(
            CommoditiesBoughtFromProvider commoditiesBoughtFromProvider, String seOid) {
            final String provId = commoditiesBoughtFromProvider.hasProviderId() ?
                Long.toString(commoditiesBoughtFromProvider.getProviderId())
                : null;
            CommoditiesBoughtRepoFromProviderDTO commoditiesBoughtRepoFromProviderDTO =
                new CommoditiesBoughtRepoFromProviderDTO();
            commoditiesBoughtRepoFromProviderDTO.setCommodityBoughtRepoDTOs(
                commoditiesBoughtFromProvider.getCommodityBoughtList().stream()
                    .map(comm -> CommodityMapper.convert(seOid, provId, comm))
                    .collect(Collectors.toList()));
            commoditiesBoughtRepoFromProviderDTO.setProviderId(commoditiesBoughtFromProvider.hasProviderId() ?
                commoditiesBoughtFromProvider.getProviderId() : null);
            commoditiesBoughtRepoFromProviderDTO.setProviderEntityType(commoditiesBoughtFromProvider.hasProviderEntityType() ?
                commoditiesBoughtFromProvider.getProviderEntityType() : null);
            return commoditiesBoughtRepoFromProviderDTO;
        }

        private static CommoditiesBoughtFromProvider convert(
                CommoditiesBoughtRepoFromProviderDTO commoditiesBoughtRepoFromProviderDTO) {
            CommoditiesBoughtFromProvider.Builder commodityBoughtFromProviderBuilder =
                    CommoditiesBoughtFromProvider.newBuilder();
            commodityBoughtFromProviderBuilder.addAllCommodityBought(
                    commoditiesBoughtRepoFromProviderDTO.getCommodityBoughtRepoDTOs().stream()
                            .map(CommodityMapper::convert)
                            .collect(Collectors.toList()));
            if (commoditiesBoughtRepoFromProviderDTO.getProviderId() != null) {
                commodityBoughtFromProviderBuilder.setProviderId(
                        commoditiesBoughtRepoFromProviderDTO.getProviderId());
            }
            if (commoditiesBoughtRepoFromProviderDTO.getProviderEntityType() != null) {
                commodityBoughtFromProviderBuilder.setProviderEntityType(
                        commoditiesBoughtRepoFromProviderDTO.getProviderEntityType());
            }
            return commodityBoughtFromProviderBuilder.build();
        }
    }

    static class ConnectedEntityMapper {

        private static ConnectedEntityRepoDTO convert(ConnectedEntity connectedEntity) {
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

        private static ConnectedEntity convert(ConnectedEntityRepoDTO connectedEntityRepoDTO) {
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
}
