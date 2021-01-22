package com.vmturbo.repository.topology;

import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.repository.dto.CommoditiesBoughtRepoFromProviderDTO;
import com.vmturbo.repository.dto.CommodityBoughtRepoDTO;
import com.vmturbo.repository.dto.CommoditySoldRepoDTO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Convert between CommodityBoughtRepoDTO and CommodityBoughtDTO.
 **/
    class CommodityMapper {

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * Convert from {@link CommodityBoughtDTO} protobuf to {@link CommodityBoughtRepoDTO}.
     *
     * @param ownerOid the OID of the SE buying this commodity
     * @param providerOid the OID of the SE selling this commodity - may be null if unsatisfied
     * @param comm the {@link CommodityBoughtDTO} being bought
     * @return a {@link CommodityBoughtRepoDTO} initialized from the given {@link CommodityBoughtDTO}
     */
    public static CommodityBoughtRepoDTO convertToCommodityBoughtRepoDTO(
            @Nonnull String ownerOid,
            @Nullable String providerOid,
            @Nonnull CommodityBoughtDTO comm) {
        CommodityBoughtRepoDTO commRepo = new CommodityBoughtRepoDTO();

        commRepo.setUuid(UUID.randomUUID().toString());
        commRepo.setProviderOid(providerOid);
        commRepo.setOwnerOid(ownerOid);
        commRepo.setType(UICommodityType.fromType(comm.getCommodityType().getType()).apiStr());

        commRepo.setKey(comm.getCommodityType().getKey());
        commRepo.setUsed(comm.getUsed());
        commRepo.setPeak(comm.getPeak());
        commRepo.setScalingFactor(comm.getScalingFactor());
        commRepo.setDisplayName(comm.getDisplayName());
        commRepo.setAggregates(comm.getAggregatesList());

        return commRepo;
    }

    /**
     * Convert from the {@link CommodityBoughtRepoDTO} to the protobuf {@link CommodityBoughtDTO}
     *
     * @param commodityBoughtRepoDTO the {@link CommodityBoughtRepoDTO} to be converted
     * @return a {@link CommodityBoughtDTO} initialized from the given RepoDTO
     */
    public static CommodityBoughtDTO convertToCommodityBoughtDTO(CommodityBoughtRepoDTO commodityBoughtRepoDTO) {
        CommodityBoughtDTO.Builder commodityBoughtBuilder = CommodityBoughtDTO.newBuilder();
        commodityBoughtBuilder.setUsed(commodityBoughtRepoDTO.getUsed());
        commodityBoughtBuilder.setPeak(commodityBoughtRepoDTO.getPeak());
        CommodityType.Builder commodityTypeBuilder = CommodityType.newBuilder();
        if (commodityBoughtRepoDTO.getType() != null) {
            commodityTypeBuilder.setType(UICommodityType.fromString(commodityBoughtRepoDTO.getType()).typeNumber());
        }
        if (commodityBoughtRepoDTO.getKey() != null) {
            commodityTypeBuilder.setKey(commodityBoughtRepoDTO.getKey());
        }
        commodityBoughtBuilder.setCommodityType(commodityTypeBuilder);
        commodityBoughtBuilder.setScalingFactor(commodityBoughtRepoDTO.getScalingFactor());

        if (commodityBoughtRepoDTO.getDisplayName() != null) {
            commodityBoughtBuilder.setDisplayName(commodityBoughtRepoDTO.getDisplayName());
        }

        if (commodityBoughtRepoDTO.getAggregates() != null &&
            !commodityBoughtRepoDTO.getAggregates().isEmpty()) {
            commodityBoughtBuilder.addAllAggregates(commodityBoughtRepoDTO.getAggregates());
        }

        return commodityBoughtBuilder.build();
    }

    /**
     * Convert from a {@link CommoditySoldDTO} protobuf to a {@link CommoditySoldRepoDTO}.
     *
     * @param ownerOid the OID of the SE that sells this commodity
     * @param comm the commodity being sold
     * @return a {@link CommoditySoldRepoDTO} initialized from the given {@link CommoditySoldDTO}
     */
    public static CommoditySoldRepoDTO convertToCommoditySoldRepoDTO(String ownerOid,
            CommoditySoldDTO comm) {
        CommoditySoldRepoDTO commRepo = new CommoditySoldRepoDTO();

        commRepo.setUuid(UUID.randomUUID().toString());
        commRepo.setProviderOid(ownerOid);
        commRepo.setOwnerOid(ownerOid);
        commRepo.setType(UICommodityType.fromType(comm.getCommodityType().getType()).apiStr());

        commRepo.setKey(comm.getCommodityType().getKey());
        commRepo.setUsed(comm.getUsed());
        commRepo.setPeak(comm.getPeak());

        commRepo.setCapacity(comm.getCapacity());
        commRepo.setEffectiveCapacityPercentage(comm.getEffectiveCapacityPercentage());
        commRepo.setReservedCapacity(comm.getReservedCapacity());
        commRepo.setResizeable(comm.getIsResizeable());
        commRepo.setThin(comm.getIsThin());
        commRepo.setCapacityIncrement(comm.getCapacityIncrement());
        // TODO should repo DTO contain all kinds of historical values? or any?
        // histUtilization is already not in there
        if (comm.hasHistoricalUsed() && comm.getHistoricalUsed().hasMaxQuantity()) {
            commRepo.setMaxQuantity(comm.getHistoricalUsed().getMaxQuantity());
        }
        commRepo.setScalingFactor(comm.getScalingFactor());

        if (comm.hasHotResizeInfo()) {
            HotResizeInfo hotResizeInfo = comm.getHotResizeInfo();
            if (hotResizeInfo.hasHotReplaceSupported()) {
                commRepo.setHotReplaceSupported(hotResizeInfo.getHotReplaceSupported());
            }
            if (hotResizeInfo.hasHotAddSupported()) {
                commRepo.setHotAddSupported(hotResizeInfo.getHotAddSupported());
            }
            if (hotResizeInfo.hasHotRemoveSupported()) {
                commRepo.setHotRemoveSupported(hotResizeInfo.getHotRemoveSupported());
            }
        }
        commRepo.setHotAddSupported(comm.getHotResizeInfo().getHotAddSupported());
        commRepo.setHotRemoveSupported(comm.getHotResizeInfo().getHotRemoveSupported());
        commRepo.setDisplayName(comm.getDisplayName());
        commRepo.setAggregates(comm.getAggregatesList());

        return commRepo;
    }

    /**
     * Convert a {@link CommoditySoldRepoDTO} to a {@link CommoditySoldDTO} protobuf.
     *
     * @param commoditySoldRepoDTO the {@link CommoditySoldRepoDTO} to convert
     * @return a new {@link CommoditySoldDTO} initialized from the given {@link CommoditySoldRepoDTO}
     */
    public static CommoditySoldDTO convertToCommoditySoldDTO(CommoditySoldRepoDTO commoditySoldRepoDTO) {
        CommoditySoldDTO.Builder commoditySoldDTOBuilder = CommoditySoldDTO.newBuilder();
        commoditySoldDTOBuilder.setUsed(commoditySoldRepoDTO.getUsed());
        commoditySoldDTOBuilder.setPeak(commoditySoldRepoDTO.getPeak());
        commoditySoldDTOBuilder.setCapacity(commoditySoldRepoDTO.getCapacity());
        commoditySoldDTOBuilder.setEffectiveCapacityPercentage(
            commoditySoldRepoDTO.getEffectiveCapacityPercentage());
        commoditySoldDTOBuilder.setReservedCapacity(commoditySoldRepoDTO.getReservedCapacity());
        commoditySoldDTOBuilder.setIsResizeable(commoditySoldRepoDTO.isResizeable());
        commoditySoldDTOBuilder.setIsThin(commoditySoldRepoDTO.isThin());
        commoditySoldDTOBuilder.setScalingFactor(commoditySoldRepoDTO.getScalingFactor());
        CommodityType.Builder commodityTypeBuilder = CommodityType.newBuilder();

        if (commoditySoldRepoDTO.getType() != null) {
            commodityTypeBuilder.setType(UICommodityType.fromString(commoditySoldRepoDTO.getType()).typeNumber());
        }
        if (commoditySoldRepoDTO.getKey() != null) {
            commodityTypeBuilder.setKey(commoditySoldRepoDTO.getKey());
        }

        commoditySoldDTOBuilder.setCommodityType(commodityTypeBuilder.build());

        commoditySoldDTOBuilder.setHotResizeInfo(HotResizeInfo.newBuilder()
            .setHotReplaceSupported(commoditySoldRepoDTO.isHotReplaceSupported())
            .setHotAddSupported(commoditySoldRepoDTO.isHotAddSupported())
            .setHotRemoveSupported(commoditySoldRepoDTO.isHotRemoveSupported()).build());

        if (commoditySoldRepoDTO.getDisplayName() != null) {
            commoditySoldDTOBuilder.setDisplayName(commoditySoldRepoDTO.getDisplayName());
        }

        if (commoditySoldRepoDTO.getAggregates() != null &&
            !commoditySoldRepoDTO.getAggregates().isEmpty()) {
            commoditySoldDTOBuilder.addAllAggregates(commoditySoldRepoDTO.getAggregates());
        }

        return commoditySoldDTOBuilder.build();
    }

    public static CommoditiesBoughtRepoFromProviderDTO convertToRepoBoughtFromProviderDTO(
            CommoditiesBoughtFromProvider commoditiesBoughtFromProvider, String seOid) {
        final String provId = commoditiesBoughtFromProvider.hasProviderId() ?
                Long.toString(commoditiesBoughtFromProvider.getProviderId())
                : null;
        CommoditiesBoughtRepoFromProviderDTO commoditiesBoughtRepoFromProviderDTO =
                new CommoditiesBoughtRepoFromProviderDTO();
        commoditiesBoughtRepoFromProviderDTO.setCommodityBoughtRepoDTOs(
                commoditiesBoughtFromProvider.getCommodityBoughtList().stream()
                        .map(comm -> convertToCommodityBoughtRepoDTO(seOid, provId, comm))
                        .collect(Collectors.toList()));
        commoditiesBoughtRepoFromProviderDTO.setProviderId(commoditiesBoughtFromProvider.hasProviderId() ?
                commoditiesBoughtFromProvider.getProviderId() : null);
        commoditiesBoughtRepoFromProviderDTO.setProviderEntityType(commoditiesBoughtFromProvider.hasProviderEntityType() ?
                commoditiesBoughtFromProvider.getProviderEntityType() : null);
        commoditiesBoughtRepoFromProviderDTO.setVolumeId(commoditiesBoughtFromProvider.hasVolumeId() ?
                commoditiesBoughtFromProvider.getVolumeId() : null);
        return commoditiesBoughtRepoFromProviderDTO;
    }

    public static CommoditiesBoughtFromProvider convertToCommoditiesBoughtFromProvider(
            CommoditiesBoughtRepoFromProviderDTO commoditiesBoughtRepoFromProviderDTO) {
        CommoditiesBoughtFromProvider.Builder commodityBoughtFromProviderBuilder =
                CommoditiesBoughtFromProvider.newBuilder();
        if (commoditiesBoughtRepoFromProviderDTO.getCommodityBoughtRepoDTOs() != null) {
            commodityBoughtFromProviderBuilder.addAllCommodityBought(
                    commoditiesBoughtRepoFromProviderDTO.getCommodityBoughtRepoDTOs().stream()
                            .map(CommodityMapper::convertToCommodityBoughtDTO)
                            .collect(Collectors.toList()));
        } else {
            LOGGER.info("{}'s CommodityBoughtRepoDTOs is null, it means it's buying from another entity, " +
                    "but not declaring which commodity buying from it. This should not happen.", commoditiesBoughtRepoFromProviderDTO);
        }
        if (commoditiesBoughtRepoFromProviderDTO.getProviderId() != null) {
            commodityBoughtFromProviderBuilder.setProviderId(
                    commoditiesBoughtRepoFromProviderDTO.getProviderId());
        }
        if (commoditiesBoughtRepoFromProviderDTO.getProviderEntityType() != null) {
            commodityBoughtFromProviderBuilder.setProviderEntityType(
                    commoditiesBoughtRepoFromProviderDTO.getProviderEntityType());
        }
        if (commoditiesBoughtRepoFromProviderDTO.getVolumeId() != null) {
            commodityBoughtFromProviderBuilder.setVolumeId(
                    commoditiesBoughtRepoFromProviderDTO.getVolumeId());
        }
        return commodityBoughtFromProviderBuilder.build();
    }
}
