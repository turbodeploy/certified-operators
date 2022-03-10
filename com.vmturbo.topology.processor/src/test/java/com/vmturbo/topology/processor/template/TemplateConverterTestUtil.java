package com.vmturbo.topology.processor.template;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory;
import com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateField;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Utility functions for testing templates.
 */
public class TemplateConverterTestUtil {

    private TemplateConverterTestUtil() {}

    /**
     * A sample template for a VM.
     */
    public static final TemplateInfo VM_TEMPLATE_INFO = TemplateInfo.newBuilder()
            .setName("test-VM-template")
            .setTemplateSpecId(123)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .addResources(TemplateResource.newBuilder()
                    .setCategory(ResourcesCategory.newBuilder()
                            .setName(ResourcesCategoryName.Compute))
                    .addFields(TemplateField.newBuilder()
                            .setName("numOfCpu")
                            .setValue("2"))
                    .addFields(TemplateField.newBuilder()
                            .setName("cpuSpeed")
                            .setValue("200"))
                    .addFields(TemplateField.newBuilder()
                            .setName("cpuConsumedFactor")
                            .setValue("0.1"))
                    .addFields(TemplateField.newBuilder()
                            .setName("memorySize")
                            .setValue("100"))
                    .addFields(TemplateField.newBuilder()
                            .setName("memoryConsumedFactor")
                            .setValue("0.1"))
                    .addFields(TemplateField.newBuilder()
                            .setName("ioThroughput")
                            .setValue("300"))
                    .addFields(TemplateField.newBuilder()
                            .setName("networkThroughput")
                            .setValue("400")))
            .addResources(TemplateResource.newBuilder()
                    .setCategory(ResourcesCategory.newBuilder()
                            .setName(ResourcesCategoryName.Storage))
                    .addFields(TemplateField.newBuilder()
                            .setName("diskIops")
                            .setValue("300"))
                    .addFields(TemplateField.newBuilder()
                            .setName("diskSize")
                            .setValue("300"))
                    .addFields(TemplateField.newBuilder()
                            .setName("diskConsumedFactor")
                            .setValue("0.1")))
            .build();

    /**
     * A sample sold commodity list from a VM.
     */
    public static final Set<CommoditySoldView> VM_COMMODITY_SOLD = Sets.newHashSet(
            new CommoditySoldImpl()
                    .setCommodityType(new CommodityTypeImpl()
                            .setType(CommodityDTO.CommodityType.APPLICATION_VALUE)
                            .setKey("123-application"))
                    .setUsed(1));

    private static final List<CommodityBoughtView> VM_COMMODITY_BOUGHT_FROM_HOST = Lists.newArrayList(
           new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl()
                            .setType(CommodityDTO.CommodityType.NETWORK_VALUE)
                            .setKey("123-network"))
                    .setUsed(1),
            new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl()
                            .setType(CommodityDTO.CommodityType.DATASTORE_VALUE)
                            .setKey("123-datastore"))
                    .setUsed(1),
            new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl()
                            .setType(CommodityDTO.CommodityType.DATACENTER_VALUE)
                            .setKey("123-data-center"))
                    .setUsed(1),
            new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl()
                            .setType(CommodityDTO.CommodityType.CLUSTER_VALUE)
                            .setKey("123-cluster"))
                    .setUsed(1)
    );

    private static final List<CommodityBoughtView> VM_COMMODITY_BOUGHT_FROM_STORAGE = Lists.newArrayList(
            new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl()
                            .setType(CommodityDTO.CommodityType.STORAGE_CLUSTER_VALUE)
                            .setKey("123-storage-cluster"))
                    .setUsed(1),
            new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl()
                            .setType(CommodityDTO.CommodityType.DSPM_ACCESS_VALUE)
                            .setKey("123-dspm-access"))
                    .setUsed(1),
            new CommodityBoughtImpl()
                    .setCommodityType(new CommodityTypeImpl()
                            .setType(CommodityDTO.CommodityType.EXTENT_VALUE)
                            .setKey("123-extent"))
                    .setUsed(1)
    );

    private static final CommoditiesBoughtFromProviderView commodityBoughtFromProviderHost =
            new CommoditiesBoughtFromProviderImpl()
                    .addAllCommodityBought(VM_COMMODITY_BOUGHT_FROM_HOST)
                    .setProviderId(123)
                    .setProviderEntityType(EntityType.PHYSICAL_MACHINE_VALUE);

    private static final CommoditiesBoughtFromProviderView commodityBoughtFromProviderStorage =
            new CommoditiesBoughtFromProviderImpl()
                    .addAllCommodityBought(VM_COMMODITY_BOUGHT_FROM_STORAGE)
                    .setProviderId(456)
                    .setProviderEntityType(EntityType.STORAGE_VALUE);

    /**
     * A sample bought commodity list for a VM.
     */
    public static final List<CommoditiesBoughtFromProviderView> VM_COMMODITY_BOUGHT_FROM_PROVIDER =
            Stream.of(commodityBoughtFromProviderHost, commodityBoughtFromProviderStorage).collect(Collectors.toList());

    /**
     * Gets the first commodity sold that matches the provided type.
     *
     * @param commoditySoldDTOList the list of commodities to search for the commodity
     *                                       with the provided commodityType.
     * @param commodityType the commodityType of the commodity to search for.
     * @return the first commodity sold that matches the provided type.
     */
    public static Optional<CommoditySoldView> getCommoditySold(@Nonnull final List<CommoditySoldView> commoditySoldDTOList,
                                                              final int commodityType) {
        return commoditySoldDTOList.stream()
            .filter(commodity -> commodity.getCommodityType().getType() == commodityType)
            .findFirst();
    }

    /**
     * Gets the value from the first commodity sold that matches the provided type.
     *
     * @param commoditySoldDTOList the list of commodities to search for the commodity
     *                                       with the provided commodityType.
     * @param commodityType the commodityType of the commodity to search for.
     * @return the value from the first commodity sold that matches the provided type.
     */
    public static double getCommoditySoldValue(@Nonnull final List<CommoditySoldView> commoditySoldDTOList,
                                               final int commodityType) {
        return getCommoditySold(commoditySoldDTOList, commodityType)
            .map(CommoditySoldView::getCapacity)
            .get();
    }

    /**
     * Gets the first commodity bought that matches the provided type.
     *
     * @param commoditiesBoughtFromProviders the list of commodities to search for the commodity
     *                                       with the provided commodityType.
     * @param commodityType the commodityType of the commodity to search for.
     * @return the first commodity bought that matches the provided type.
     */
    public static Optional<CommodityBoughtView> getCommodityBought(
            @Nonnull final List<CommoditiesBoughtFromProviderView> commoditiesBoughtFromProviders,
            final int commodityType) {
        return commoditiesBoughtFromProviders.stream()
            .map(CommoditiesBoughtFromProviderView::getCommodityBoughtList)
            .flatMap(List::stream)
            .filter(commodity -> commodity.getCommodityType().getType() == commodityType)
            .findFirst();
    }

    /**
     * Gets the value from the first commodity bought that matches the provided type.
     *
     * @param commoditiesBoughtFromProviders the list of commodities to search for the commodity
     *                                       with the provided commodityType.
     * @param commodityType the commodityType of the commodity to search for.
     * @return the value from the first commodity bought that matches the provided type.
     */
    public static double getCommodityBoughtValue(List<CommoditiesBoughtFromProviderView> commoditiesBoughtFromProviders,
                                                 int commodityType) {
        return getCommodityBought(commoditiesBoughtFromProviders, commodityType)
            .map(CommodityBoughtView::getUsed)
            .get();
    }

    /**
     * Gets the key from the first commodity bought that matches the provided type.
     *
     * @param commoditiesBoughtFromProviders the list of commodities to search for the commodity
     *                                       with the provided commodityType.
     * @param commodityType the commodityType of the commodity to search for.
     * @return the key from the first commodity bought that matches the provided type.
     */
    public static String getCommodityBoughtKey(List<CommoditiesBoughtFromProviderView> commoditiesBoughtFromProviders,
                                               int commodityType) {
        return getCommodityBought(commoditiesBoughtFromProviders, commodityType)
            .map(CommodityBoughtView::getCommodityType)
            .map(CommodityTypeView::getKey)
            .get();
    }
}