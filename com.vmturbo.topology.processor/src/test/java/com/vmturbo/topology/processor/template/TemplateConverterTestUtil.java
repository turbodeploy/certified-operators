package com.vmturbo.topology.processor.template;

import java.util.List;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;

public class TemplateConverterTestUtil {

    public static double getCommoditySoldValue(List<CommoditySoldDTO> commoditySoldDTOList, int commodityType) {
        return commoditySoldDTOList.stream()
            .filter(commodity -> commodity.getCommodityType().getType() == commodityType)
            .findFirst()
            .map(CommoditySoldDTO::getCapacity)
            .get();
    }

    public static double getCommodityBoughtValue(List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders,
                                                 int commodityType) {
        return commoditiesBoughtFromProviders.stream()
            .map(CommoditiesBoughtFromProvider::getCommodityBoughtList)
            .flatMap(List::stream)
            .filter(commodity -> commodity.getCommodityType().getType() == commodityType)
            .findFirst()
            .map(CommodityBoughtDTO::getUsed)
            .get();
    }
}