package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.api.constraints.ConstraintApiDTO;
import com.vmturbo.api.constraints.PlacementOptionApiDTO;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.enums.RelationType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Convert commodity constraints into API constraints objects.
 */
public class ConstraintsMapper {


    /**
     *  Create ConstraintApiDTO objects from the given TopologyEntityDTOs.
     *
     * @param entityDtos List of TopologyEntityDTOs.
     * @param serviceEntityApiDTOMap Mapping from EntityOid -> ServiceEntityApiDTO
     * @param relationType Type of relation.
     * @return List of ConstraintApiDTO objects
     */
    public static List<ConstraintApiDTO> createConstraintApiDTOs(@Nonnull List<TopologyEntityDTO> entityDtos,
                                                          @Nonnull Map<Long, ServiceEntityApiDTO> serviceEntityApiDTOMap,
                                                          @Nonnull RelationType relationType) {

        List<ConstraintApiDTO> constraintApiDtos = new ArrayList<>();

        for (TopologyEntityDTO entityDto : entityDtos) {
            for (CommoditiesBoughtFromProvider commoditiesBoughtFromProvider :
                    entityDto.getCommoditiesBoughtFromProvidersList()) {
                ConstraintApiDTO constraintApiDTO = new ConstraintApiDTO();
                constraintApiDtos.add(constraintApiDTO);
                constraintApiDTO.setRelation(relationType);
                if (relationType == RelationType.bought) {
                    constraintApiDTO.setEntityType(ApiEntityType.fromType(
                            commoditiesBoughtFromProvider.getProviderEntityType()).apiStr());
                    constraintApiDTO.setRelatedEntities(Collections.singletonList(
                            serviceEntityApiDTOMap.get(commoditiesBoughtFromProvider.getProviderId())));
                } else {
                    constraintApiDTO.setEntityType(ApiEntityType.fromType(
                            entityDto.getEntityType()).apiStr());
                    constraintApiDTO.setRelatedEntities(Collections.singletonList(
                            serviceEntityApiDTOMap.get(entityDto.getOid())));
                }
                constraintApiDTO.setPlacementOptions(
                        createPlacementOptionApiDto(
                                commoditiesBoughtFromProvider.getCommodityBoughtList()));
            }
        }
        return constraintApiDtos;
    }

    /**
     * Create PlacementOptionApiDTO objects from the CommodityBoughDTOs.
     *
     * @param commodityBoughtDTOS The list of commodities bought by an entity from a provider.
     * @return List of PlacementOptionApiDTO objects
     */
    public static List<PlacementOptionApiDTO> createPlacementOptionApiDto(
            List<CommodityBoughtDTO> commodityBoughtDTOS) {

        // The entityIDs are embedded in the constraints(Commodity Keys). In classic, the api resolves this to the entity type
        // and returns the entityDetails. In XL, we have decided not to implement full parity with classic as the parsing rules
        // within the key are not well-defined and it is expensive to query all these entities.
        List<PlacementOptionApiDTO> placementOptionApiDTOs = new ArrayList<>();
        for (CommodityBoughtDTO commodityBought : commodityBoughtDTOS) {
            if (commodityBought.getCommodityType().hasKey()) {
                PlacementOptionApiDTO placementOptionApiDTO =
                        new PlacementOptionApiDTO();
                placementOptionApiDTO.setConstraintType(
                        ClassicEnumMapper.getCommodityString(
                                CommodityType.forNumber(
                                        commodityBought.getCommodityType().getType())));
                placementOptionApiDTO.setKey(commodityBought.getCommodityType().getKey());
                placementOptionApiDTOs.add(placementOptionApiDTO);
                BaseApiDTO baseApiDTO = new BaseApiDTO();
                baseApiDTO.setUuid(commodityBought.getCommodityType().getKey());
                baseApiDTO.setDisplayName(commodityBought.getCommodityType().getKey());
                placementOptionApiDTO.setScope(baseApiDTO);
            }
        }
        return placementOptionApiDTOs;
    }
}
