package com.vmturbo.topology.processor.conversions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;

/**
 * Convert a list of entity DTOs produced by SDK probes to topology
 * processor's entity DTOs.
 */
public class Converter {

    private static final Logger logger = LogManager.getLogger();

    // This is a temporary hard-coded value. TODO: Obtain from probe DTOs or settings.
    private static final double TEMP_DEFAULT_EFFECTIVE_CAPACITY = 100;

    private static Set<CommodityDTO.CommodityType> DSPM_OR_DATASTORE =
                    Sets.newHashSet(CommodityDTO.CommodityType.DSPM_ACCESS, CommodityDTO.CommodityType.DATASTORE);

    private Converter() {}

    private static int type(CommonDTO.EntityDTO dto) {
        return dto.getEntityType().getNumber();
    }

    private static int type(CommonDTO.CommodityDTO dto) {
        return dto.getCommodityType().getNumber();
    }

    /**
     * Convert probe entity DTOs to topology entity DTOs.
     * @param entityDTOs Map of probe entity DTOs keyed by oid (already obtained from the identity service}).
     * @return a list of topology entity DTOs
     */
    public static List<TopologyDTO.TopologyEntityDTO.Builder> convert(Map<Long, CommonDTO.EntityDTO> entityDTOs) {
        // Map from provider ID to OID, to handle forward references in the list of DTOs
        Map<String, Long> providerOIDs = Maps.newHashMap();
        // Cache the oids. Using entrySet().stream() to void parallelism.
        entityDTOs.entrySet().stream().forEach(entry -> providerOIDs.put(entry.getValue().getId(), entry.getKey()));
        ImmutableList.Builder<TopologyDTO.TopologyEntityDTO.Builder> builder = ImmutableList.builder();
        entityDTOs.forEach((oid, dto) -> builder.add(newTopologyEntityDTO(dto, oid, providerOIDs)));
        return builder.build();
    }

    public static TopologyDTO.TopologyEntityDTO.Builder newTopologyEntityDTO(CommonDTO.EntityDTO dto,
                                                                             long oid, Map<String, Long> providerOIDs) {
        final int entityType = type(dto);
        final String displayName = dto.getDisplayName();
        final TopologyDTO.EntityState entityState = entityState(dto.getPowerState());
        final boolean availableAsProvider = dto.getProviderPolicy().getAvailableForPlacement();

        List<TopologyDTO.CommoditySoldDTO> soldList = Lists.newArrayList();
        dto.getCommoditiesSoldList()
            .stream()
            .map(commDTO -> newCommoditySoldDTO(commDTO, providerOIDs))
            .forEach(soldList::add);

        // Map from provider oid to list of commodities bought
        Map<Long, List<TopologyDTO.CommodityBoughtDTO>> boughtMap = Maps.newHashMap();
        for (CommodityBought commodityBought : dto.getCommoditiesBoughtList()) {
            Long providerOid = providerOIDs.get(commodityBought.getProviderId());
            if (providerOid == null) {
                // Skip this commodity if the topology processor doesn't have information
                // about the provider. Logging at error level because this indicates
                // a bug in the information sent by the probe.
                logger.error("Entity {} (name: {}) is buying commodities {} from provider that doesn't exist: {}.",
                        oid, displayName,
                        commodityBought.getBoughtList().stream()
                            .map(CommodityDTO::getCommodityType)
                            .collect(Collectors.toList()),
                        commodityBought.getProviderId());
                continue;
            }
            commodityBought.getBoughtList().stream()
                .map(Converter::newCommodityBoughtDTO)
                .forEach(topologyCommodityDTO -> boughtMap
                    .computeIfAbsent(providerOid, id -> Lists.newArrayList())
                    .add(topologyCommodityDTO));
        }

        // Copy properties map from probe DTO to topology DTO
        // TODO: Support for namespaces and proper handling of duplicate properties (see
        // OM-20545 for description of probe expectations related to duplicate properties).
        Map<String, String> entityPropertyMap = dto.getEntityPropertiesList().stream()
            .collect(Collectors.toMap(EntityProperty::getName, EntityProperty::getValue,
                (valueA, valueB) -> {
                    logger.warn("Duplicate entity property with values \"{}\", \"{}\" detected on entity {} (name: {}).",
                        valueA, valueB, oid, displayName);
                    return valueA;
                }));

        // Add properties of related data to the entity property map - using reflection
        Lists.newArrayList(
                dto.getApplicationData(),
                dto.getDiskArrayData(),
                dto.getPhysicalMachineData(),
                dto.getPhysicalMachineRelatedData(),
                dto.getStorageControllerRelatedData(),
                dto.getReplacementEntityData(),
                dto.getStorageData(),
                dto.getVirtualDatacenterData(),
                dto.getVirtualMachineData(),
                dto.getVirtualMachineRelatedData()
                )
        .stream().forEach(
                data -> data.getAllFields().forEach(
                        // TODO: Lists, such as VirtualDatacenterData.VmUuidList are also converted to String
                        (f, v) -> entityPropertyMap.put(f.getFullName(), v.toString())
                )
        );

        if (dto.hasOrigin()) {
            entityPropertyMap.put("origin", dto.getOrigin().toString()); // TODO: DISCOVERED/PROXY use number?
        }
        entityPropertyMap.put("shopTogether", String.valueOf(dto.getConsumerPolicy().getShopsTogether()));
        return newTopologyEntityDTO(
                entityType,
                oid,
                displayName,
                soldList,
                boughtMap,
                entityState,
                entityPropertyMap,
                availableAsProvider
        );
    }

    private static TopologyDTO.TopologyEntityDTO.Builder newTopologyEntityDTO(
            int entityType,
            long oid,
            String displayName,
            List<TopologyDTO.CommoditySoldDTO> soldList,
            Map<Long, List<TopologyDTO.CommodityBoughtDTO>> boughtMap,
            TopologyDTO.EntityState entityState,
            Map<String, String> entityPropertyMap,
            boolean availableAsProvider
        ) {
        Map<Long, TopologyDTO.TopologyEntityDTO.CommodityBoughtList> map = Maps.newHashMap();
        boughtMap.forEach((k, v) -> map
                .put(k, TopologyDTO.TopologyEntityDTO.CommodityBoughtList.newBuilder()
                        .addAllCommodityBought(v)
                        .build()));
        TopologyDTO.TopologyEntityDTO.ProviderPolicy policy =
                TopologyDTO.TopologyEntityDTO.ProviderPolicy.newBuilder()
                    .setIsAvailableAsProvider(availableAsProvider)
                    .build();
        return TopologyDTO.TopologyEntityDTO.newBuilder()
            .setEntityType(entityType)
            .setOid(oid)
            .setDisplayName(displayName)
            .setEntityState(entityState)
            .setProviderPolicy(policy)
            .putAllEntityPropertyMap(entityPropertyMap)
            .addAllCommoditySoldList(soldList)
            .putAllCommodityBoughtMap(map);
    }

    private static TopologyDTO.EntityState entityState(CommonDTO.EntityDTO.PowerState powerState) {
        switch (powerState) {
        case POWERED_OFF:
            return TopologyDTO.EntityState.POWERED_OFF;
        case POWERED_ON:
            return TopologyDTO.EntityState.POWERED_ON;
        case SUSPENDED:
            return TopologyDTO.EntityState.SUSPENDED;
        default:
            return TopologyDTO.EntityState.UNKNOWN;
        }
    }

    private static TopologyDTO.CommodityBoughtDTO newCommodityBoughtDTO(CommonDTO.CommodityDTO commDTO) {
        return TopologyDTO.CommodityBoughtDTO.newBuilder()
            .setCommodityType(commodityType(commDTO))
            .setUsed(commDTO.getUsed())
            .setPeak(commDTO.getPeak())
            .setActive(commDTO.getActive())
            .build();
    }

    private static TopologyDTO.CommoditySoldDTO newCommoditySoldDTO(
                    CommonDTO.CommodityDTO commDTO,
                    Map<String, Long> providerOIDs) {
        CommoditySoldDTO.Builder builder = TopologyDTO.CommoditySoldDTO.newBuilder()
            .setCommodityType(commodityType(commDTO))
            .setUsed(commDTO.getUsed())
            .setPeak(commDTO.getPeak())
            .setCapacity(commDTO.getCapacity())
            .setEffectiveCapacityPercentage(TEMP_DEFAULT_EFFECTIVE_CAPACITY)
            .setReservedCapacity(commDTO.getReservation())
            .setIsThin(commDTO.getThin())
            .setActive(commDTO.getActive());
        if (DSPM_OR_DATASTORE.contains(commDTO.getCommodityType())) {
            final Long oid = providerOIDs.get(keyToUuid(commDTO.getKey()));
            if (oid == null) {
                // Note that this mechanism for lookup does not work for cloud-related
                // hosts and datastores.
                logger.error("No provider oid for uuid {} (original key={})",
                    keyToUuid(commDTO.getKey()), commDTO.getKey());
            } else {
                builder.setAccesses(providerOIDs.get(keyToUuid(commDTO.getKey())));
            }
        }
        return builder.build();
    }

    private static CommodityType commodityType(CommonDTO.CommodityDTO commDTO) {
        return CommodityType.newBuilder()
                .setType(type(commDTO))
                .setKey(commDTO.getKey())
                .build();
    }

    /**
     * Split the key of DSPMAccess commodity and DatastoreCommodity.
     * The keys look like "PhysicalMachine::7cd62bff-d6c8-e011-0000-00000000000f"
     * for DSPMAcess and "Storage::5787bc1e-357c82ea-47c4-0025b500038f" for
     * DatastoreCommodity, where the part after the colons is the uuid.
     *
     * For cloud targets the key looks like PhysicalMachine::aws::us-west-2::PM::us-west-2b
     *
     * @param key original key
     * @return the uuid part of the key
     */
    private static String keyToUuid(String key) {
        return key.split("::")[1];
    }
}
