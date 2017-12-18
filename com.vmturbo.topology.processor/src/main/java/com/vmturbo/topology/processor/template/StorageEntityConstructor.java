package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Storage;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.addCommodityConstraints;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommodityBoughtDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommoditySoldDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.getActiveCommoditiesWithKeysGroups;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.getCommoditySoldConstraint;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Create a TopologyEntityDTO from Storage Template. The new Topology Entity contains such as OID, displayName,
 * commodity sold, commodity bought, entity state, provider policy and consumer policy.
 * And also it will try to keep all commodity constrains from the original topology entity.
 */
public class StorageEntityConstructor implements TopologyEntityConstructor {
    private static final String ZERO = "0";

    /**
     * Create a TopologyEntityDTO from Storage Template.
     *
     * @param template storage template.
     * @param topologyEntityBuilder builder of TopologyEntityDTO which could contains some setting already.
     * @param originalTopologyEntity the original topology entity which this template want to keep its
     *                               commodity constraints. It could be null, if it is new adding template.
     * @return {@link TopologyEntityDTO}.
     */
    @Override
    public TopologyEntityDTO.Builder createTopologyEntityFromTemplate (
            @Nonnull final Template template,
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nullable final TopologyEntityDTO originalTopologyEntity) {
        final List<CommoditiesBoughtFromProvider> commodityBoughtConstraints = getActiveCommoditiesWithKeysGroups(
            originalTopologyEntity);
        final Set<CommoditySoldDTO> commoditySoldConstraints = getCommoditySoldConstraint(
            originalTopologyEntity);
        final List<TemplateResource> storageTemplateResources =
            TemplatesConverterUtils.getTemplateResources(template, Storage);
        addStorageCommodities(topologyEntityBuilder, storageTemplateResources);
        addCommodityConstraints(topologyEntityBuilder, commoditySoldConstraints, commodityBoughtConstraints);
        return topologyEntityBuilder;
    }

    /**
     * Generate commodities for storage template resources.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param storageTemplateResources a list of storage template resources.
     */
    private static void addStorageCommodities(
            @Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
            @Nonnull List<TemplateResource> storageTemplateResources) {
        final Map<String, String> fieldNameValueMap =
            TemplatesConverterUtils.createFieldNameValueMap(storageTemplateResources);
        addStorageCommoditiesBought(topologyEntityBuilder);
        addStorageCommoditiesSold(topologyEntityBuilder, fieldNameValueMap);
    }

    /**
     * Generate storage commodity bought and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     */
    private static void addStorageCommoditiesBought(
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder) {
        CommoditiesBoughtFromProvider.Builder commoditiesBoughtGroup =
            CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.DISK_ARRAY_VALUE);
        CommodityBoughtDTO extentCommodityBought =
                createCommodityBoughtDTO(CommodityDTO.CommodityType.EXTENT_VALUE, 1);
        commoditiesBoughtGroup.addCommodityBought(extentCommodityBought);
        commoditiesBoughtGroup.setMovable(true);
        topologyEntityBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtGroup.build());
    }

    /**
     * Generate storage commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntity.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addStorageCommoditiesSold(@Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
                                                  @Nonnull Map<String, String> fieldNameValueMap) {
        final double disSize =
            Double.valueOf(fieldNameValueMap.getOrDefault(TemplatesConverterUtils.DISK_SIZE, ZERO));
        final double disIops =
            Double.valueOf(fieldNameValueMap.getOrDefault(TemplatesConverterUtils.DISK_IOPS, ZERO));

        CommoditySoldDTO storageAmoutCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, disSize);
        CommoditySoldDTO storageAccessCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, disIops);

        topologyEntityBuilder
            .addCommoditySoldList(storageAccessCommodity)
            .addCommoditySoldList(storageAmoutCommodity);
    }
}
