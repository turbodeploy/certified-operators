package com.vmturbo.topology.processor.conversions;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.SLA_COMMODITY;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT;

import java.util.stream.Collectors;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery;
import com.vmturbo.platform.common.dto.SupplyChain;
import com.vmturbo.platform.common.dto.SupplyChain.ExternalEntityLink;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;

/**
 * Converts application entities used prior to the applications model change
 * into application entities that introduced in the new applications model.
 * e.g. APPLICATION and APPLICATION_SERVER are converted into APPLICATION_COMPONENT
 */
public class ApplicationEntitiesConverter {
    /**
     * Converts all APPLICATION/APPLICATION_SERVER entities from response to APPLICATION_COMPONENT.
     * @param oldFormatResponse response with the old format
     * @return response with the new format
     */
    public Discovery.DiscoveryResponse convertResponse(Discovery.DiscoveryResponse oldFormatResponse) {
        // If no apps or app servers, return original response
        if (oldFormatResponse.getEntityDTOList().stream().noneMatch(e -> isOldApplicationEntity(e.getEntityType()))) {
            return oldFormatResponse;
        }
        Discovery.DiscoveryResponse.Builder newResponseBuilder = oldFormatResponse.toBuilder().clearEntityDTO();
        oldFormatResponse.getEntityDTOList().forEach(entityDTO -> {
            if (isOldApplicationEntity(entityDTO.getEntityType())) {
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
        // If we already have APPLICATION_COMPONENT in metadata,
        // return probe info as is
        if (oldProbeInfo.getEntityMetadataList()
                .stream()
                .anyMatch(e -> APPLICATION_COMPONENT.equals(e.getEntityType()))) {
            return oldProbeInfo;
        }
        ProbeInfo.Builder probeInfoBuilder = ProbeInfo.newBuilder(oldProbeInfo)
                .clearEntityMetadata()
                .clearSupplyChainDefinitionSet();

        // Changing Identity Metadata
        EntityIdentityMetadata appComponentMetadata = null;
        for (EntityIdentityMetadata data : oldProbeInfo.getEntityMetadataList()) {
            if (data.hasEntityType() && isOldApplicationEntity(data.getEntityType())) {
                if (appComponentMetadata == null) {
                    appComponentMetadata = data.toBuilder().setEntityType(APPLICATION_COMPONENT).build();
                    probeInfoBuilder.addEntityMetadata(appComponentMetadata);
                }
            } else {
                probeInfoBuilder.addEntityMetadata(data);
            }
        }

        boolean isAppComponentTemplateAdded = false;
        // Changing Supply Chain
        for (TemplateDTO template : oldProbeInfo.getSupplyChainDefinitionSetList()) {
            TemplateDTO.Builder newTemplateBuilder = TemplateDTO.newBuilder(template);

            // Changing commodities bought from apps or app servers
            if (doesBuyApps(template)) {
                updateAppBoughtCommodities(newTemplateBuilder, template);
            }

            // Add only first app-related template, change its class and external links
            if (isOldApplicationEntity(template.getTemplateClass())) {
                if (!isAppComponentTemplateAdded) {
                    newTemplateBuilder.setTemplateClass(APPLICATION_COMPONENT);
                    updateAppComponentExternalLinks(newTemplateBuilder, template);
                    probeInfoBuilder.addSupplyChainDefinitionSet(newTemplateBuilder.build());
                    isAppComponentTemplateAdded = true;
                }
                continue;
            }

            // Add regular templates without change
            probeInfoBuilder.addSupplyChainDefinitionSet(newTemplateBuilder.build());
        }

        return probeInfoBuilder.build();
    }

    /**
     * Update external links of APPLICATION_COMPONENT template.
     * @param newTemplateBuilder builder for new template
     * @param oldTemplate old template
     */
    private void updateAppComponentExternalLinks(TemplateDTO.Builder newTemplateBuilder,
                                                 final TemplateDTO oldTemplate) {
        newTemplateBuilder.clearExternalLink()
                .addAllExternalLink(oldTemplate.getExternalLinkList().stream()
                        .map(e -> {
                            ExternalEntityLink.Builder link =
                                    e.getValue().toBuilder().setBuyerRef(APPLICATION_COMPONENT);
                            return e.toBuilder().setValue(link).build();
                        }).collect(Collectors.toList()));
    }

    /**
     * Update commodities bought from apps.
     * @param newTemplateBuilder builder for new template
     * @param oldTemplate old template
     */
    private void updateAppBoughtCommodities(TemplateDTO.Builder newTemplateBuilder,
                                            final TemplateDTO oldTemplate) {
        newTemplateBuilder.clearCommodityBought();
        boolean isAppComponentCommodityAdded = false;
        for (TemplateDTO.CommBoughtProviderProp commodity : oldTemplate.getCommodityBoughtList()) {

            // Add only first app-related commodity and change its class to APPLICATION_COMPONENT
            if (commodity.hasKey() && isOldApplicationEntity(commodity.getKey().getTemplateClass())) {
                if (!isAppComponentCommodityAdded) {
                    SupplyChain.Provider key = commodity.getKey().toBuilder()
                            .setTemplateClass(APPLICATION_COMPONENT).build();
                    newTemplateBuilder.addCommodityBought(commodity.toBuilder().setKey(key).build());
                    isAppComponentCommodityAdded = true;
                }
                continue;
            }

            // Add regular commodity without change
            newTemplateBuilder.addCommodityBought(commodity);
        }
    }

    /**
     * Checks if this old-format application entity.
     * @param entityType entity type
     * @return true if this is APPLICATION or APPLICATION_SERVER entity
     */
    private boolean isOldApplicationEntity(EntityType entityType) {
        return EntityType.APPLICATION.equals(entityType) ||
                EntityType.APPLICATION_SERVER.equals(entityType);
    }

    /**
     * Checks if entity buys apps or app servers.
     * @param template old-format entity template
     * @return whether entity buys apps or app servers
     */
    private boolean doesBuyApps(final TemplateDTO template) {
        return template.getCommodityBoughtList()
                .stream()
                .anyMatch(c -> c.hasKey() && isOldApplicationEntity(c.getKey().getTemplateClass()));
    }
}
