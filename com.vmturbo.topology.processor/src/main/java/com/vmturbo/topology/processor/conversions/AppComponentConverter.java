package com.vmturbo.topology.processor.conversions;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.SLA_COMMODITY;

import java.util.stream.Collectors;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery;

/**
 * Converts old-format response (with APPLICATION and APPLICATION_SERVER entities)
 * to new (with APPLICATION components).
 */
public class AppComponentConverter {
    /**
     * Converts all APPLICATION/APPLICATION_SERVER entities from response to APPLICATION_COMPONENT.
     * @param oldFormatResponse response with the old format
     * @return response with the new format
     */
    public Discovery.DiscoveryResponse convertResponse(Discovery.DiscoveryResponse oldFormatResponse) {
        // If no apps or app servers, return original response
        if (oldFormatResponse.getEntityDTOList().stream().noneMatch(this::isAppOrAppServer)) {
            return oldFormatResponse;
        }
        Discovery.DiscoveryResponse.Builder newResponseBuilder = oldFormatResponse.toBuilder().clearEntityDTO();
        oldFormatResponse.getEntityDTOList().forEach(entityDTO -> {
            if (isAppOrAppServer(entityDTO)) {
                EntityDTO appComponent = EntityDTO.newBuilder()
                        .setEntityType(EntityType.APPLICATION_COMPONENT)
                        .setDisplayName(entityDTO.getDisplayName())
                        .setId(entityDTO.getId())
                        .setApplicationData(entityDTO.getApplicationData())
                        .setActionEligibility(entityDTO.getActionEligibility())
                        .addAllCommoditiesBought(entityDTO.getCommoditiesBoughtList())
                        // Ignoring SLA commodities
                        .addAllCommoditiesSold(entityDTO.getCommoditiesSoldList().stream()
                                .filter(c -> !SLA_COMMODITY.equals(c.getCommodityType()))
                                .collect(Collectors.toList()))
                        .addAllLayeredOver(entityDTO.getLayeredOverList())
                        .addAllEntityProperties(entityDTO.getEntityPropertiesList())
                        .addAllReplacesEntityId(entityDTO.getReplacesEntityIdList())
                        .addAllConsistsOf(entityDTO.getConsistsOfList())
                        .addAllNotification(entityDTO.getNotificationList())
                        .build();
                newResponseBuilder.addEntityDTO(appComponent);
            } else {
                newResponseBuilder.addEntityDTO(entityDTO);
            }
        });
        return newResponseBuilder.build();
    }

    /**
     * Checks if this old-format application entity.
     * @param entityDTO entity
     * @return true if this is APPLICATION or APPLICATION_SERVER entity
     */
    private boolean isAppOrAppServer(EntityDTO entityDTO) {
        return EntityType.APPLICATION.equals(entityDTO.getEntityType()) ||
                EntityType.APPLICATION_SERVER.equals(entityDTO.getEntityType());
    }
}
