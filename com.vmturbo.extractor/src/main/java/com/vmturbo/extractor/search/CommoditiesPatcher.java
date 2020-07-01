package com.vmturbo.extractor.search;

import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.searchquery.FieldApiDTO.FieldType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add commodities info, like used, utilization, etc.
 */
public class CommoditiesPatcher implements EntityRecordPatcher<TopologyEntityDTO> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void patch(PartialRecordInfo recordInfo, TopologyEntityDTO entity) {
        // find all commodities and commodity attributes defined in metadata and set on jsonb
        final List<SearchMetadataMapping> commodityMetadata =
                SearchMetadataUtils.getMetadata(recordInfo.entityType, FieldType.COMMODITY);
        if (commodityMetadata.isEmpty()) {
            // nothing to add
            return;
        }

        final Set<Integer> commodityTypes = commodityMetadata.stream()
                .map(m -> EnumUtils.commodityTypeFromApiToProtoInt(m.getCommodityType()))
                .collect(Collectors.toSet());
        // prepare sold commodity map first
        Map<Integer, List<CommoditySoldDTO>> csByType = entity.getCommoditySoldListList().stream()
                .filter(cs -> commodityTypes.contains(cs.getCommodityType().getType()))
                .collect(Collectors.groupingBy(cs -> cs.getCommodityType().getType()));

        final Map<String, Object> attrs = recordInfo.attrs;
        commodityMetadata.forEach(metadata -> {
            int commodityType = EnumUtils.commodityTypeFromApiToProtoInt(metadata.getCommodityType());
            List<CommoditySoldDTO> commoditySoldDTOs = csByType.get(commodityType);
            if (commoditySoldDTOs != null) {
                final String jsonKey = metadata.getJsonKeyName();
                switch (metadata.getCommodityAttribute()) {
                    case USED:
                        attrs.put(jsonKey, getUsed(commoditySoldDTOs));
                        break;
                    case UTILIZATION:
                        attrs.put(jsonKey, getUtilization(commoditySoldDTOs));
                        break;
                    case CAPACITY:
                        attrs.put(jsonKey, getCapacity(commoditySoldDTOs));
                        break;
                    case PEAK:
                        attrs.put(jsonKey, getPeak(commoditySoldDTOs));
                        break;
                    case PERCENTILE:
                        // we are using the used percentile here (there is also peak percentile)
                        // not all commodities have percentile
                        getPercentile(commoditySoldDTOs).ifPresent(percentile ->
                                attrs.put(jsonKey, percentile));
                        break;
                    default:
                        logger.error("Unsupported commodity attribute: {}",
                                metadata.getCommodityAttribute());
                }
            }
        });
    }

    private double getUsed(List<CommoditySoldDTO> commoditySoldDTOs) {
        return commoditySoldDTOs.stream().mapToDouble(CommoditySoldDTO::getUsed).sum();
    }

    private double getCapacity(List<CommoditySoldDTO> commoditySoldDTOs) {
        return commoditySoldDTOs.stream().mapToDouble(CommoditySoldDTO::getCapacity).sum();
    }

    private double getPeak(List<CommoditySoldDTO> commoditySoldDTOs) {
        return commoditySoldDTOs.stream().mapToDouble(CommoditySoldDTO::getPeak).sum();
    }

    private double getUtilization(List<CommoditySoldDTO> commoditySoldDTOs) {
        final double capacity = getCapacity(commoditySoldDTOs);
        return capacity == 0 ? 0 : getUsed(commoditySoldDTOs) / capacity;
    }

    /**
     * For most cases, entity sells one commodity of a type. If there are multiple, we do an
     * average on all of them, since percentile is the utilization.
     *
     * @param commoditySoldDTOs list of {@link CommoditySoldDTO}
     * @return optional percentile utilization
     */
    private OptionalDouble getPercentile(List<CommoditySoldDTO> commoditySoldDTOs) {
        return commoditySoldDTOs.stream()
                .filter(commoditySoldDTO -> commoditySoldDTO.hasHistoricalUsed())
                .filter(commoditySoldDTO -> commoditySoldDTO.getHistoricalUsed().hasPercentile())
                .mapToDouble(commoditySoldDTO -> commoditySoldDTO.getHistoricalUsed().getPercentile())
                .average();
    }
}
