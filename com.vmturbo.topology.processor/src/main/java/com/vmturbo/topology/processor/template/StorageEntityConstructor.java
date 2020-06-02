package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.COMMODITY_KEY_SEPARATOR;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Storage;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DATASTORE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DATASTORE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.EXTENT_VALUE;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateResource;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.identity.IdentityProvider;

/**
 * Create a TopologyEntityDTO from Storage Template. The new Topology Entity contains such as OID, displayName,
 * commodity sold, commodity bought, entity state, provider policy and consumer policy.
 * And also it will try to keep all commodity constrains from the original topology entity.
 */
public class StorageEntityConstructor extends TopologyEntityConstructor
        implements ITopologyEntityConstructor {

    private static final String ZERO = "0";

    static final String COMMODITY_KEY_PREFIX = "AddFromTemplate::";

    @Override
    public TopologyEntityDTO.Builder createTopologyEntityFromTemplate(
            @Nonnull final Template template, @Nonnull Map<Long, TopologyEntity.Builder> topology,
            @Nullable TopologyEntityDTO.Builder originalTopologyEntity, boolean isReplaced,
            @Nonnull IdentityProvider identityProvider) throws TopologyEntityConstructorException {
        TopologyEntityDTO.Builder topologyEntityBuilder = super.generateTopologyEntityBuilder(
                template, originalTopologyEntity, isReplaced, identityProvider,
                EntityType.STORAGE_VALUE);

        final List<CommoditiesBoughtFromProvider> commodityBoughtConstraints;
        final Set<CommoditySoldDTO> commoditySoldConstraints;
        if (originalTopologyEntity == null) {
            // The case where a new storage is added from template.
            addExtentCommodityBought(topology, topologyEntityBuilder);
            commodityBoughtConstraints = Collections.emptyList();
            commoditySoldConstraints = addDSPMAccessCommoditySold(topology, topologyEntityBuilder);
        } else {
            // The case where an existing storage is replaced by a template storage.
            commodityBoughtConstraints = getActiveCommoditiesWithKeysGroups(originalTopologyEntity);
            commoditySoldConstraints = getCommoditySoldConstraint(originalTopologyEntity);
            addStorageCommoditiesBought(topologyEntityBuilder);
        }

        final List<TemplateResource> storageTemplateResources = getTemplateResources(template,
                Storage);
        final Map<String, String> fieldNameValueMap =
                createFieldNameValueMap(storageTemplateResources);
        addStorageCommoditiesSold(topologyEntityBuilder, fieldNameValueMap, false);

        // shopRogether entities are not allowed to sell biclique commodities (why???), and storages need
        // to sell biclique commodities, so set shopTogether to false.
        topologyEntityBuilder.getAnalysisSettingsBuilder().setShopTogether(false);

        addCommodityConstraints(topologyEntityBuilder, commoditySoldConstraints,
                commodityBoughtConstraints);
        if (originalTopologyEntity != null) {
            updateRelatedEntityAccesses(originalTopologyEntity,
                    topologyEntityBuilder.build(), commoditySoldConstraints, topology);

            topologyEntityBuilder.setTypeSpecificInfo(originalTopologyEntity.getTypeSpecificInfo());
        }
        return topologyEntityBuilder;
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
     * Generate Extent commodity bought and add to the added storage.
     * Generate Extent commodity sold with the same key and add to all disk arrays.
     * This is because we want to place the added storage but we don't know which disk arrays are in the plan scope.
     *
     * @param topology The topology map from OID -> TopologyEntity.Builder. When performing a replace,
     *                 entities related to the entity being replaced may be updated to fix up relationships
     *                 to point to the new entity along with the old entity.
     * @param topologyEntityBuilder builder of TopologyEntityDTO which could contains some setting already.
     */
    private void addExtentCommodityBought(
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder) {
        final CommoditiesBoughtFromProvider.Builder commoditiesBoughtGroup =
            CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.DISK_ARRAY_VALUE);
        // Unique commodity key.
        final String commodityKey = COMMODITY_KEY_PREFIX + EntityType.DISK_ARRAY.name() +
            COMMODITY_KEY_SEPARATOR + topologyEntityBuilder.getOid();
        final CommodityBoughtDTO extentCommodityBought =
                createCommodityBoughtDTO(EXTENT_VALUE, commodityKey, 1);
        commoditiesBoughtGroup.addCommodityBought(extentCommodityBought);
        commoditiesBoughtGroup.setMovable(true);
        topologyEntityBuilder.addCommoditiesBoughtFromProviders(commoditiesBoughtGroup.build());

        // Add Extent commodity sold to disk arrays.
        topology.values().stream()
            .filter(entity -> entity.getEntityType() == EntityType.DISK_ARRAY_VALUE)
            .map(TopologyEntity.Builder::getEntityBuilder)
            .forEach(entity -> entity.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setCommodityType(CommodityType.newBuilder().setType(EXTENT_VALUE)
                    .setKey(commodityKey))));
    }

    /**
     * Generate DSPM_ACCESS and DATASTORE commodity sold to connect the added storage to all hosts.
     *
     * @param topology The topology map from OID -> TopologyEntity.Builder. When performing a replace,
     *                 entities related to the entity being replaced may be updated to fix up relationships
     *                 to point to the new entity along with the old entity.
     * @param topologyEntityBuilder builder of TopologyEntityDTO which could contains some setting already.
     * @return a set of DSPM_ACCESS commodity sold
     */
    private Set<CommoditySoldDTO> addDSPMAccessCommoditySold(
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            @Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder) {
        // Find all hosts.
        final Set<Long> unvisitedHosts = topology.values().stream()
            .filter(entity -> entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .map(TopologyEntity.Builder::getOid).collect(Collectors.toSet());
        final int numOfHosts = unvisitedHosts.size();


        // A map from host oid to DSPM_ACCESS commodity key.
        final Map<Long, String> hostOidToCommodityKey = new HashMap<>(unvisitedHosts.size());

        topologyForLoop: for (Entry<Long, TopologyEntity.Builder> entry : topology.entrySet()) {
            if (entry.getValue().getEntityType() != EntityType.STORAGE_VALUE) {
                continue;
            }

            for (CommoditySoldDTO.Builder commSold : entry.getValue().getEntityBuilder()
                    .getCommoditySoldListBuilderList()) {
                // Find the DSPM_ACCESS commodity.
                if (commSold.getCommodityType().getType() != DSPM_ACCESS_VALUE ||
                    !commSold.getCommodityType().hasKey() ||
                    !topology.containsKey(commSold.getAccesses())) {
                    continue;
                }
                // Break if we find the commodity keys of all hosts.
                if (hostOidToCommodityKey.size() == numOfHosts) {
                    break topologyForLoop;
                }
                hostOidToCommodityKey.put(commSold.getAccesses(), commSold.getCommodityType().getKey());
                unvisitedHosts.remove(commSold.getAccesses());
            }
        }

        // Create a set of DSPM_ACCESS commodity sold that needs to be added to the added storage.
        final Set<CommoditySoldDTO> commoditySoldConstraints = hostOidToCommodityKey.entrySet().stream()
            .map(entry -> CommoditySoldDTO.newBuilder().setAccesses(entry.getKey())
                .setCommodityType(CommodityType.newBuilder().setType(DSPM_ACCESS_VALUE)
                    .setKey(entry.getValue())).build())
            .collect(Collectors.toSet());
        // Consider the case where a host is not connected to any existing storage.
        // Let the added storage connect to it.
        commoditySoldConstraints.addAll(unvisitedHosts.stream().map(oid ->
            CommoditySoldDTO.newBuilder().setAccesses(oid)
                .setCommodityType(CommodityType.newBuilder().setType(DSPM_ACCESS_VALUE)
                    .setKey(COMMODITY_KEY_PREFIX + DSPM_ACCESS.name() + COMMODITY_KEY_SEPARATOR + oid)).build())
            .collect(Collectors.toList()));

        // Add a DATASTORE commodity sold to each host in order to connect to the added storage.
        Stream.concat(unvisitedHosts.stream(), hostOidToCommodityKey.keySet().stream())
            .filter(topology::containsKey).map(topology::get)
            .map(TopologyEntity.Builder::getEntityBuilder)
            .forEach(entity -> entity.addCommoditySoldList(CommoditySoldDTO.newBuilder()
                .setAccesses(topologyEntityBuilder.getOid())
                .setCommodityType(CommodityType.newBuilder().setType(DATASTORE_VALUE)
                    .setKey(COMMODITY_KEY_PREFIX + DATASTORE.name() + COMMODITY_KEY_SEPARATOR +
                        topologyEntityBuilder.getOid()))));

        return commoditySoldConstraints;
    }
}
