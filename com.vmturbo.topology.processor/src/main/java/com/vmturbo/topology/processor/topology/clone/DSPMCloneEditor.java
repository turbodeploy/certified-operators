package com.vmturbo.topology.processor.topology.clone;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * The {@link DSPMCloneEditor} implements the clone function for the container.
 */
public class DSPMCloneEditor extends DefaultEntityCloneEditor {

    @Override
    public TopologyEntity.Builder clone(@Nonnull final TopologyEntityImpl entityImpl,
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
            @Nonnull final CloneContext cloneContext, @Nonnull final CloneInfo cloneInfo) {
        final TopologyEntity.Builder dspmClone = super.clone(entityImpl, topologyGraph,
                cloneContext, cloneInfo);
        // if entityImpl is host or storage. Collect all vm consumers and add the usage.
        List<TopologyEntity> consumers = topologyGraph.getConsumers(entityImpl.getOid()).filter(
                consumer -> consumer.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE).collect(
                Collectors.toList());
        final Map<CommodityTypeView, Double> commodityTypeViewToUsedByConsumersMap =
                computeCommodityTypeViewToUsedByConsumersMap(consumers, entityImpl.getOid());
        for (final CommoditySoldImpl commSold : dspmClone.getTopologyEntityImpl()
                .getCommoditySoldListImplList()) {
            Double usedByConsumers = commodityTypeViewToUsedByConsumersMap.get(
                    commSold.getCommodityType());
            Double overhead = 0d;
            if (usedByConsumers != null) {
                overhead = Math.max(commSold.getUsed() - usedByConsumers, 0d);
            }

            commSold.setUsed(overhead).setPeak(overhead);
        }
        return dspmClone;
    }

    /**
     * Add all the usages of all consumers for each commodity if the provider is providerOid.
     *
     * @param consumers the consumers whose usage we are adding up.
     * @param providerOid the id of the provider from which the consumers are buying.
     * @return map which maps the CommodityType to the sume of used values on consumers for that
     *         commodity.
     */
    private Map<CommodityTypeView, Double> computeCommodityTypeViewToUsedByConsumersMap(
            List<TopologyEntity> consumers, long providerOid) {
        Map<CommodityTypeView, Double> commodityTypeViewToUsedByConsumersMap = new HashMap<>();
        List<CommodityBoughtView> commodityBoughts = consumers.stream().map(
                TopologyEntity::getTopologyEntityImpl)
                // The "shopping lists"
                .map(TopologyEntityImpl::getCommoditiesBoughtFromProvidersList).flatMap(
                        List::stream)
                // Those buying from the seller
                .filter(commsBought -> commsBought.getProviderId() == providerOid).map(
                        CommoditiesBoughtFromProviderView::getCommodityBoughtList).flatMap(
                        List::stream).collect(Collectors.toList());
        for (CommodityBoughtView commodityBoughtView : commodityBoughts) {
            if (commodityBoughtView.hasUsed()) {
                Double currentUsed = commodityTypeViewToUsedByConsumersMap.getOrDefault(
                        commodityBoughtView.getCommodityType(), 0d);
                commodityTypeViewToUsedByConsumersMap.put(commodityBoughtView.getCommodityType(),
                        currentUsed + commodityBoughtView.getUsed());
            }
        }
        return commodityTypeViewToUsedByConsumersMap;
    }
}
