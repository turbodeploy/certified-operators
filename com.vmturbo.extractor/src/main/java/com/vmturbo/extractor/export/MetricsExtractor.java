package com.vmturbo.extractor.export;

import static com.vmturbo.common.protobuf.topology.TopologyDTOUtil.QX_VCPU_BASE_COEFFICIENT;
import static com.vmturbo.extractor.topology.EntityMetricWriter.QX_VCPU_PATTERN;
import static com.vmturbo.extractor.topology.EntityMetricWriter.VM_QX_VCPU_NAME;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.extractor.export.schema.Commodity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Class responsible for extracting metrics (sold & bought commodities) from an entity.
 **/
public class MetricsExtractor {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Extract the required metrics from given entity.
     *
     * @param e entity containing the sold and bought commodities
     * @param reportingCommodityWhitelist whitelist of commodities to extract
     * @return map from commodity type to commodity value
     */
    @Nullable
    public Map<String, Commodity> extractMetrics(@Nonnull TopologyEntityDTO e,
            @Nonnull Set<Integer> reportingCommodityWhitelist) {
        final Map<String, Commodity> commodityByType = new HashMap<>();
        // sold commodity
        Map<Integer, List<CommoditySoldDTO>> csByType = e.getCommoditySoldListList().stream()
                .filter(cs -> reportingCommodityWhitelist.contains(cs.getCommodityType().getType()))
                .collect(Collectors.groupingBy(cs -> cs.getCommodityType().getType()));
        csByType.forEach((typeNo, css) -> {
            final String commodityTypeJsonKey = ExportUtils.getCommodityTypeJsonKey(typeNo);
            if (commodityTypeJsonKey == null) {
                logger.error("Invalid commodity type {} for entity {}", typeNo, e.getOid());
                return;
            }
            // sum across commodity keys in case same commodity type appears with multiple keys
            final double sumUsed = css.stream().mapToDouble(CommoditySoldDTO::getUsed).sum();
            final double sumCap = css.stream().mapToDouble(CommoditySoldDTO::getCapacity).sum();
            final double utilization = sumCap == 0 ? 0 : sumUsed / sumCap;

            Commodity soldComm = new Commodity();
            soldComm.setCurrent(sumUsed);
            soldComm.setCapacity(sumCap);
            soldComm.setUtilization(utilization);
            commodityByType.put(commodityTypeJsonKey, soldComm);
        });

        // bought commodities, group by commodity type regardless of provider or key
        Map<Integer, List<CommodityBoughtDTO>> cbByType = e.getCommoditiesBoughtFromProvidersList()
                .stream()
                .flatMap(cbfp -> cbfp.getCommodityBoughtList().stream())
                .filter(cb -> reportingCommodityWhitelist.contains(cb.getCommodityType().getType()))
                .collect(Collectors.groupingBy(cb -> cb.getCommodityType().getType()));
        cbByType.forEach((typeNo, cbs) -> {
            // sum across same commodity type with different keys and same provider
            final double sumUsed = cbs.stream().mapToDouble(CommodityBoughtDTO::getUsed).sum();
            // special handling for Qx_VCPU bought commodity like we do for reporting
            if (QX_VCPU_PATTERN.matcher(CommodityType.forNumber(typeNo).name()).matches()) {
                // use existing one if there is a sold commodity of same type
                final Commodity commodity = commodityByType.computeIfAbsent(
                        VM_QX_VCPU_NAME.getLiteral(), k -> new Commodity());
                commodity.setCurrent(sumUsed);
                commodity.setCapacity(QX_VCPU_BASE_COEFFICIENT);
                commodity.setUtilization(sumUsed / QX_VCPU_BASE_COEFFICIENT);
            } else {
                final String commodityTypeJsonKey = ExportUtils.getCommodityTypeJsonKey(typeNo);
                if (commodityTypeJsonKey == null) {
                    logger.error("Invalid commodity type {} for entity {}", typeNo, e.getOid());
                    return;
                }
                // use existing one if there is a sold commodity of same type
                final Commodity commodity = commodityByType.computeIfAbsent(
                        commodityTypeJsonKey, k -> new Commodity());
                commodity.setConsumed(sumUsed);
            }
        });

        return commodityByType.isEmpty() ? null : commodityByType;
    }
}

