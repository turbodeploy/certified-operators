package com.vmturbo.topology.processor.template;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.COMMODITY_KEY_SEPARATOR;
import static com.vmturbo.common.protobuf.plan.TemplateDTO.ResourcesCategory.ResourcesCategoryName.Storage;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DATASTORE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DATASTORE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DSPM_ACCESS_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.EXTENT_VALUE;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.addCommodityConstraints;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommodityBoughtDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.createCommoditySoldDTO;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.getActiveCommoditiesWithKeysGroups;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.getCommoditySoldConstraint;
import static com.vmturbo.topology.processor.template.TemplatesConverterUtils.updateRelatedEntityAccesses;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.TemplateProtoUtil;
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
public class StorageEntityConstructor extends TopologyEntityConstructor {

    private static final String ZERO = "0";

    static final String COMMODITY_KEY_PREFIX = "AddFromTemplate::";

    @Override
    public Collection<TopologyEntityDTO.Builder> createTopologyEntityFromTemplate(
            @Nonnull final Template template,
            @Nonnull final Map<Long, TopologyEntity.Builder> topology,
            @Nonnull Optional<TopologyEntity.Builder> originalTopologyEntity, boolean isReplaced,
            @Nonnull IdentityProvider identityProvider) throws TopologyEntityConstructorException {
        TopologyEntityDTO.Builder topologyEntityBuilder = super.createTopologyEntityFromTemplate(
                template, topology, originalTopologyEntity, isReplaced, identityProvider).iterator()
                        .next();

        final List<CommoditiesBoughtFromProvider> commodityBoughtConstraints;
        final Set<CommoditySoldDTO> commoditySoldConstraints;
        if (!originalTopologyEntity.isPresent()) {
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

        final List<TemplateResource> storageTemplateResources =
            TemplatesConverterUtils.getTemplateResources(template, Storage);
        final Map<String, String> fieldNameValueMap =
            TemplatesConverterUtils.createFieldNameValueMap(storageTemplateResources);
        addStorageCommoditiesSold(topologyEntityBuilder, fieldNameValueMap);

        // shopRogether entities are not allowed to sell biclique commodities (why???), and storages need
        // to sell biclique commodities, so set shopTogether to false.
        topologyEntityBuilder.getAnalysisSettingsBuilder().setShopTogether(false);

        addCommodityConstraints(topologyEntityBuilder, commoditySoldConstraints, commodityBoughtConstraints);
        if (originalTopologyEntity.isPresent()) {
            updateRelatedEntityAccesses(originalTopologyEntity.get().getOid(),
                    topologyEntityBuilder.getOid(),
                commoditySoldConstraints, topology);
        }
        return Collections.singletonList(topologyEntityBuilder);
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
            createCommodityBoughtDTO(EXTENT_VALUE, Optional.of(commodityKey), 1);
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

    /**
     * Generate storage commodity sold and add to TopologyEntityDTO.
     *
     * @param topologyEntityBuilder builder of TopologyEntity.
     * @param fieldNameValueMap a Map which key is template field name and value is field value.
     */
    private static void addStorageCommoditiesSold(@Nonnull final TopologyEntityDTO.Builder topologyEntityBuilder,
                                                  @Nonnull Map<String, String> fieldNameValueMap) {
        final double diskSize =
            Double.valueOf(fieldNameValueMap.getOrDefault(TemplateProtoUtil.STORAGE_DISK_SIZE, ZERO));
        final double diskIops =
            Double.valueOf(fieldNameValueMap.getOrDefault(TemplateProtoUtil.STORAGE_DISK_IOPS, ZERO));

        CommoditySoldDTO storageAmoutCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE, Optional.ofNullable(diskSize));
        CommoditySoldDTO storageAccessCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE, Optional.ofNullable(diskIops));
        // Since storage templates don't have a latency value, but VMs do buy a latency commodity,
        // a sold commodity is added. Capacity is left unset - StorageLatencyPostStitchingOperation
        // will set it to the default value from EntitySettingSpecs.LatencyCapacity.
        CommoditySoldDTO storageLatencyCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.STORAGE_LATENCY_VALUE);
        // Because we don't have access to settings at this time, we can't calculate capacities for
        // provisioned commodities. By leaving capacities unset, they will be set later in the
        // topology pipeline when settings are avaialble by the
        // OverprovisionCapacityPostStitchingOperation.
        CommoditySoldDTO storageProvisionedCommodity =
            createCommoditySoldDTO(CommodityDTO.CommodityType.STORAGE_PROVISIONED_VALUE);

        topologyEntityBuilder
            .addCommoditySoldList(storageAccessCommodity)
            .addCommoditySoldList(storageAmoutCommodity)
            .addCommoditySoldList(storageLatencyCommodity)
            .addCommoditySoldList(storageProvisionedCommodity);
    }
}
