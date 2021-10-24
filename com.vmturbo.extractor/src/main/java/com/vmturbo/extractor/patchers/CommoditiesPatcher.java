package com.vmturbo.extractor.patchers;

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
import com.vmturbo.extractor.search.EnumUtils.CommodityTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.EntityRecordPatcher;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialEntityInfo;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.search.metadata.SearchMetadataMapping;

/**
 * Add commodities info, like used, utilization, etc.
 */
public class CommoditiesPatcher implements EntityRecordPatcher<TopologyEntityDTO> {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void fetch(PartialEntityInfo recordInfo, TopologyEntityDTO entity) {
        // find all commodities and commodity attributes defined in metadata and set on jsonb
        final List<SearchMetadataMapping> commodityMetadata =
                SearchMetadataUtils.getMetadata(recordInfo.getEntityType(), FieldType.COMMODITY);
        if (commodityMetadata.isEmpty()) {
            // nothing to add
            return;
        }

        final Set<Integer> commodityTypes = commodityMetadata.stream()
                .map(m -> CommodityTypeUtils.apiToProto(m.getCommodityType()).getNumber())
                .collect(Collectors.toSet());
        // prepare sold commodity map first
        Map<Integer, List<CommoditySoldDTO>> csByType = entity.getCommoditySoldListList().stream()
                .filter(cs -> commodityTypes.contains(cs.getCommodityType().getType()))
                .collect(Collectors.groupingBy(cs -> cs.getCommodityType().getType()));

        commodityMetadata.forEach(metadata -> {
            int commodityType = CommodityTypeUtils.apiToProto(metadata.getCommodityType()).getNumber();
            List<CommoditySoldDTO> commoditySoldDTOs = csByType.get(commodityType);
            if (commoditySoldDTOs != null) {
                switch (metadata.getCommodityAttribute()) {
                    case USED:
                        recordInfo.putAttr(metadata, getUsed(commoditySoldDTOs));
                        break;
                    case CAPACITY:
                        recordInfo.putAttr(metadata, getCapacity(commoditySoldDTOs));
                        break;
                    case PEAK:
                        recordInfo.putAttr(metadata, getPeak(commoditySoldDTOs));
                        break;
                    case CURRENT_UTILIZATION:
                        recordInfo.putAttr(metadata, getCurrentUtilization(commoditySoldDTOs));
                        break;
                    case WEIGHTED_HISTORICAL_UTILIZATION:
                        // not all commodities have weighted historical utilization
                        getWeightedAverageHistoricalUtilization(commoditySoldDTOs).ifPresent(percentile ->
                        recordInfo.putAttr(metadata, percentile));
                        break;
                    case PERCENTILE_HISTORICAL_UTILIZATION:
                        // not all commodities have percentile historical utilization
                        getPercentileHistoricalUtilization(commoditySoldDTOs).ifPresent(percentile ->
                            recordInfo.putAttr(metadata, percentile));
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

    /**
     * Get the current utilization.
     *
     * <p>This is a simple calculation of the current 'used' value divided by the current
     * 'capacity' value.</p>
     *
     * @param commoditySoldDTOs commodities from which to derive the current utilization
     * @return the current utilization, expressed as a percentage
     */
    private double getCurrentUtilization(List<CommoditySoldDTO> commoditySoldDTOs) {
        final double capacity = getCapacity(commoditySoldDTOs);
        return capacity == 0 ? 0 : getUsed(commoditySoldDTOs) / capacity;
    }

    /**
     * Get the percentile-based historical utilization if available, expressed as a percentage.
     *
     * <p>The details of this calculation can be found in:
     * com.vmturbo.topology.processor.history.percentile.PercentileEditor</p>
     *
     * @param commoditySoldDTOs commodities from which to derive the percentile utilization
     * @return the percentile utilization, if available, expressed as a percentage
     */
    private OptionalDouble getPercentileHistoricalUtilization(List<CommoditySoldDTO> commoditySoldDTOs) {
        return commoditySoldDTOs.stream()
            .filter(commoditySoldDTO -> commoditySoldDTO.getHistoricalUsed().hasPercentile())
            .mapToDouble(commoditySoldDTO -> commoditySoldDTO.getHistoricalUsed().getPercentile())
            // For most cases, entity sells one commodity of a type. If there are multiple, we do an
            // average on all of them, since percentile (as a measure of utilization) is a percentage.
            .average();
    }

    /**
     * Get the weighted average-based historical utilization if available, expressed as a percentage.
     *
     * <p>The details of this calculation can be found in:
     * com.vmturbo.topology.processor.topology.HistoricalEditor::calculateSmoothedValue </p>
     *
     * <p>This is an older method for calculating the historical percentage of a commodity.
     * Percentile is preferred when available, but this can be used as a fall back when it is not.</p>
     *
     * @param commoditySoldDTOs commodities from which to derive the historical utilization
     * @return the historical utilization, if available, expressed as a percentage
     */
    private OptionalDouble getWeightedAverageHistoricalUtilization(List<CommoditySoldDTO> commoditySoldDTOs) {
        return commoditySoldDTOs.stream()
            .filter(commoditySoldDTO -> commoditySoldDTO.getHistoricalUsed().hasHistUtilization()
                    && commoditySoldDTO.hasCapacity()
                    && commoditySoldDTO.getCapacity() > 0)
            .mapToDouble(commoditySoldDTO -> commoditySoldDTO.getHistoricalUsed().getHistUtilization() / commoditySoldDTO.getCapacity())
            // For most cases, entity sells one commodity of a type. If there are multiple, we do an
            // average on all of them, since historical utilization is a percentage.
            .average();
    }
}
