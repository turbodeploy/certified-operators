package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Storage;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommodityBoughtDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommoditySoldDTO;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Create a TopologyEntityDTO from Storage Template. The new Topology Entity contains such as OID, displayName,
 * commodity sold, commodity bought, entity state, provider policy and consumer policy.
 */
public class StorageEntityConstructor implements TopologyEntityConstructor {

    private static final String ZERO = "0";

    /**
     * Create a TopologyEntityDTO from Storage Template.
     *
     * @param template storage template.
     * @return {@link TopologyEntityDTO}.
     */
    @Override
    public TopologyEntityDTO createTopologyEntityFromTemplate (
        @Nonnull final Template template,
        @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder) {
        final List<TemplateResource> storageTemplateResources =
            TemplatesConverterUtils.getTemplateResources(template, Storage);
        addStorageCommodities(topologyEntityBuilder, storageTemplateResources);
        return topologyEntityBuilder.build();
    }

    /**
     * Generate commodities for storage template resources.
     *
     * @param topologyEntityBuilder builder of TopologyEntityDTO.
     * @param storageTemplateResources a list of storage template resources.
     */
    private static void addStorageCommodities(@Nonnull TopologyEntityDTO.Builder topologyEntityBuilder,
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
    private static void addStorageCommoditiesBought(@Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder) {
        CommodityBoughtDTO extentCommodityBought =
            createCommodityBoughtDTO(CommodityDTO.CommodityType.EXTENT_VALUE, 1);
        topologyEntityBuilder.addCommoditiesBoughtFromProviders(
            CommoditiesBoughtFromProvider.newBuilder()
                .addCommodityBought(extentCommodityBought)
                .setMovable(true)
                .build());
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
