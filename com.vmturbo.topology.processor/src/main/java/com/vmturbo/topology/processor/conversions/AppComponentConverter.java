package com.vmturbo.topology.processor.conversions;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.SLA_COMMODITY;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT;

import java.util.stream.Collectors;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.SupplyChain;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.CommBoughtProviderProp;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

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
        if (oldFormatResponse.getEntityDTOList().stream().noneMatch(e -> isAppOrAppServer(e.getEntityType()))) {
            return oldFormatResponse;
        }
        Discovery.DiscoveryResponse.Builder newResponseBuilder = oldFormatResponse.toBuilder().clearEntityDTO();
        oldFormatResponse.getEntityDTOList().forEach(entityDTO -> {
            if (isAppOrAppServer(entityDTO.getEntityType())) {
                EntityDTO appComponent = EntityDTO.newBuilder(entityDTO)
                        .setEntityType(APPLICATION_COMPONENT)
                        // Ignoring SLA commodities
                        .clearCommoditiesSold()
                        .addAllCommoditiesSold(entityDTO.getCommoditiesSoldList().stream()
                                .filter(c -> !SLA_COMMODITY.equals(c.getCommodityType()))
                                .collect(Collectors.toList()))
                        .build();
                newResponseBuilder.addEntityDTO(appComponent);
            } else {
                newResponseBuilder.addEntityDTO(entityDTO);
            }
        });
        return newResponseBuilder.build();
    }

    /**
     * Converts probe info.
     *
     * @param oldProbeInfo old probe info format
     * @return new probe info format with APPLICATION_COMPONENT
     */
    public ProbeInfo convertProbeInfo(ProbeInfo oldProbeInfo) {
        ProbeInfo.Builder builder = ProbeInfo.newBuilder(oldProbeInfo)
                .clearEntityMetadata()
                .clearSupplyChainDefinitionSet();

        // Changing Identity Metadata
        EntityIdentityMetadata appCompData = null;
        for (EntityIdentityMetadata data : oldProbeInfo.getEntityMetadataList()) {
            if (data.hasEntityType() && isAppOrAppServer(data.getEntityType())) {
                if (appCompData == null) {
                    appCompData = data.toBuilder().setEntityType(APPLICATION_COMPONENT).build();
                    builder.addEntityMetadata(appCompData);
                }
            } else {
                builder.addEntityMetadata(data);
            }
        }

        TemplateDTO appCompTemplate = null;
        // Changing Supply Chain
        for (TemplateDTO template : oldProbeInfo.getSupplyChainDefinitionSetList()) {
            TemplateDTO.Builder newTemplate = TemplateDTO.newBuilder(template);
            // Changing template classes and their external links
            if (isAppOrAppServer(template.getTemplateClass())) {
                if (appCompTemplate == null) {
                    newTemplate.setTemplateClass(APPLICATION_COMPONENT).clearExternalLink()
                            .addAllExternalLink(template.getExternalLinkList().stream()
                                    .map(e -> e.toBuilder().setValue(e.getValue().toBuilder().setBuyerRef(APPLICATION_COMPONENT)).build())
                                    .collect(Collectors.toList()));
                    appCompTemplate = newTemplate.build();
                    builder.addSupplyChainDefinitionSet(appCompTemplate);
                }
                continue;
            // Changing commodities bought from apps or app servers
            } else if (template.getCommodityBoughtList()
                    .stream()
                    .anyMatch(c -> c.hasKey() && isAppOrAppServer(c.getKey().getTemplateClass()))) {
                newTemplate.clearCommodityBought();
                for (CommBoughtProviderProp comm : template.getCommodityBoughtList()) {
                    if (comm.hasKey() && isAppOrAppServer(comm.getKey().getTemplateClass())) {
                        SupplyChain.Provider key = comm.getKey().toBuilder().setTemplateClass(APPLICATION_COMPONENT).build();
                        newTemplate.addCommodityBought(comm.toBuilder().setKey(key).build());
                    } else {
                        newTemplate.addCommodityBought(comm);
                    }
                }
            }
            builder.addSupplyChainDefinitionSet(newTemplate.build());
        }

        return builder.build();
    }

    /**
     * Checks if this old-format application entity.
     * @param entityType entity type
     * @return true if this is APPLICATION or APPLICATION_SERVER entity
     */
    private boolean isAppOrAppServer(EntityType entityType) {
        return EntityType.APPLICATION.equals(entityType) ||
                EntityType.APPLICATION_SERVER.equals(entityType);
    }
}
