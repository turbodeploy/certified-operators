package com.vmturbo.topology.processor.conversions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.PropertiesList;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.entity.Entity;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.entity.EntityNotFoundException;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Convert topology processor's entity DTOs to entity DTOs used by SDK probes
 *
 * This conversion is primarily needed during action execution, when data about entities is being
 * sent back to the probes as part of the action execution request.
 *
 * Since the conversion from SDK EntityDTO to TopologyEntityDTO is lossy, the TopologyEntityDTO
 * is not sufficient input to perform the conversion back to an SDK EntityDTO.
 * Raw entity data from the entity store will be used to supplement the data contained in an
 * TopologyEntityDTO being converted. Additionally, the target store will be used to appropriately
 * set the namespace and target type within the entity properties.
 *
 */
public class TopologyToSdkEntityConverter {

    private static final Logger logger = LogManager.getLogger();

    // We allow 3rd-party probes, so treat any unrecognized probes as probe type "OTHER"
    // TODO: Consider adding this to SDKProbeType enum
    private static final String PROBE_TYPE_OTHER = "OTHER";

    /**
     * For retrieving the raw entityDTO(s) related to a TopologyEntityDTO
     */
    private final EntityStore entityStore;

    /**
     * For looking up the name/address and type of a target
     */
    private final TargetStore targetStore;

    public TopologyToSdkEntityConverter(@Nonnull final EntityStore entityStore,
                                        @Nonnull final TargetStore targetStore) {
        this.entityStore = Objects.requireNonNull(entityStore);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * Convert a topology entity DTO to an SDK entity DTO (used by SDK probes)
     *
     * Since the conversion from SDK EntityDTO to TopologyEntityDTO is lossy, the TopologyEntityDTO
     * is not sufficient input to perform the conversion back to an SDK EntityDTO.
     * Raw entity data from the entity store will be used to supplement the data contained in an
     * TopologyEntityDTO being converted. Additionally, the target store will be used to
     * appropriately set the namespace and target type within the entity properties.
     *
     * @param topologyEntityDTO a topology entity DTO, representing the stitched data for an entity
     * @return an SDK entity DTO (used by SDK probes)
     */
    @Nonnull
    public EntityDTO convertToEntityDTO(@Nonnull final TopologyEntityDTO topologyEntityDTO) {
        // Lookup the raw entity data that was originally discovered for this entity. The conversion
        // from a (discovered) EntityDTO to a TopologyEntityDTO is lossy, so the raw data
        // will be required in order to reconstruct an EntityDTO from a TopologyEntityDTO.
        // The original EntityDTO will be used as a starting point, and then additional data will
        // be added from the (stitched) TopologyEntityDTO. This additional data includes commodities
        // and entity properties.
        final long entityOid = topologyEntityDTO.getOid();
        final Optional<Entity> optRawEntity = entityStore.getEntity(entityOid);
        if (!optRawEntity.isPresent()) {
            throw new EntityConversionException("Conversion of entity " + entityOid +
                "failed since no matching entity could be found in the store of raw " +
                "discovered entity data.");
        }
        final Entity rawEntity = optRawEntity.get();
        final EntityDTO prototype;
        try {
            // Choose an EntityDTO from among those available in the entity to use as a starting point
            prototype = entityStore.chooseEntityDTO(rawEntity);
        } catch (EntityNotFoundException e) {
            throw new EntityConversionException(e.getMessage(), e);
        }

        final Builder builder = EntityDTO.newBuilder(prototype)
                // no translation needed
                .setDisplayName(topologyEntityDTO.getDisplayName());

        // Replace the raw commodities bought and sold with the stitched data
        builder.clearCommoditiesSold();
        builder.clearCommoditiesBought();
        builder.addAllCommoditiesSold(getCommoditiesSold(topologyEntityDTO));
        builder.addAllCommoditiesBought(getCommoditiesBought(topologyEntityDTO));

        // Add additional entity properties based on the stitched data
        builder.addAllEntityProperties(getAllTargetSpecificEntityProperties(rawEntity));

        return builder.build();
    }

    /**
     * Create a list of SDK CommodityDTOs from a TopologyEntityDTO (and its CommoditySoldDTOs)
     *
     * @param topologyEntityDTO the topology entity whose commodities sold will be converted
     * @return a list of SDK CommodityDTOs from a TopologyEntityDTO (and its CommoditySoldDTOs)
     */
    private static List<CommodityDTO> getCommoditiesSold(final TopologyEntityDTO topologyEntityDTO) {
        return topologyEntityDTO.getCommoditySoldListList().stream()
                .map(TopologyToSdkEntityConverter::newCommodityDTO)
                .collect(Collectors.toList());
    }

    /**
     * Create a list of SDK CommitityBoughts from a TopologyEntityDTO (and its CommodityBoughtDTOs)
     *
     * @param topologyEntityDTO the topology entity whose commodities bought will be converted
     * @return a list of SDK CommitityBoughts from a TopologyEntityDTO (and its CommodityBoughtDTOs)
     */
    private List<CommodityBought> getCommoditiesBought(final TopologyEntityDTO topologyEntityDTO) {
        return topologyEntityDTO.getCommoditiesBoughtFromProvidersList().stream()
                .map(this::newCommodityBought)
                .collect(Collectors.toList());
    }

    /**
     * Convert a single XL CommoditySoldDTO into an SDK CommodityDTO
     *
     * @param commoditySoldDTO the XL-domain CommoditySoldDTO to be converted
     * @return an SDK CommodityDTO
     */
    private static CommodityDTO newCommodityDTO(final CommoditySoldDTO commoditySoldDTO) {
        CommodityDTO.Builder builder = CommodityDTO.newBuilder()
                // this appears to be a 1-1 mapping, based on existing conversion code going the
                // other direction (CommodityDTO -> CommoditySoldDTO)
                .setCommodityType(CommodityDTO.CommodityType.forNumber(
                        commoditySoldDTO.getCommodityType().getType()));

        // Copy the used setting, if present
        if (commoditySoldDTO.hasUsed()) {
            builder.setUsed(commoditySoldDTO.getUsed());
        }

        // Copy the peak setting, if present
        if (commoditySoldDTO.hasPeak()) {
            if (commoditySoldDTO.getPeak() < 0) {
                logger.error("Peak quantity = {} for commodity type {}", commoditySoldDTO.getPeak(), commoditySoldDTO.getCommodityType());
            }

            builder.setPeak(commoditySoldDTO.getPeak());
        }

        // Copy the capacity setting, if present
        if (commoditySoldDTO.hasCapacity()) {
            builder.setCapacity(commoditySoldDTO.getCapacity());
        }

        // Copy the reserved capacity setting, if present
        if (commoditySoldDTO.hasReservedCapacity()) {
            builder.setReservation(commoditySoldDTO.getReservedCapacity());
        }

        // Copy the isThin setting, if present
        if (commoditySoldDTO.hasIsThin()) {
            builder.setThin(commoditySoldDTO.getIsThin());
        }

        // Copy the active setting, if present
        if (commoditySoldDTO.hasActive()) {
            builder.setActive(commoditySoldDTO.getActive());
        }

        // Copy the isResizable setting, if present
        if (commoditySoldDTO.hasIsResizeable()) {
            builder.setResizable(commoditySoldDTO.getIsResizeable());
        }

        // Copy the type key over, if present. The setter will not tolerate a null parameter!
        if (commoditySoldDTO.getCommodityType().hasKey()) {
            builder.setKey(commoditySoldDTO.getCommodityType().getKey());
        }

        // Copy the capacity increment, if present
        if (commoditySoldDTO.hasCapacityIncrement()) {
            builder.setUsedIncrement(commoditySoldDTO.getCapacityIncrement());
        }

        // Copy the display name, if present
        if (commoditySoldDTO.hasDisplayName()) {
            builder.setDisplayName(commoditySoldDTO.getDisplayName());
        }

        // Copy the aggregate commodity keys, if present
        if (!commoditySoldDTO.getAggregatesList().isEmpty()) {
            builder.addPropMap(PropertiesList.newBuilder()
                .setName(SDKConstants.AGGREGATES)
                .addAllValues(commoditySoldDTO.getAggregatesList())
                .build());
        }

        // EffectiveCapacityPercentage is not mapped, because we don't know whether to map it back
        // to a Limit or a UtilizationThresholdPct. It may have been derived from either of these
        // fields when originally converted from a CommodityDTO.

        // Scaling factor and max quantity are also not mapped, due to no corresponding setting

        return builder.build();
    }

    /**
     * Convert an XL CommoditiesBoughtFromProvider into an SDK CommodityBought
     *
     * @param commoditiesBoughtFromProvider the XL-domain CommoditiesBoughtFromProvider to convert
     * @return an SDK CommodityBought
     */
    private CommodityBought newCommodityBought(
            final CommoditiesBoughtFromProvider commoditiesBoughtFromProvider) {
        CommodityBought.Builder builder = CommodityBought.newBuilder();

        // Retrieve the provider OID and convert it to a UUID that is meaningful to the probes
        final long providerOID = commoditiesBoughtFromProvider.getProviderId();

        final String providerUUID = entityStore.chooseEntityDTO(providerOID).getId();
        builder.setProviderId(providerUUID);

        // Convert the list of CommodityBoughtDTOs into a list of CommodityDTOs
        commoditiesBoughtFromProvider.getCommodityBoughtList().stream()
                .map(TopologyToSdkEntityConverter::newCommodityDTO)
                .forEach(commodityDTO -> {
                    builder.addBought(commodityDTO);});

        // Convert the provider type, if present
        if (commoditiesBoughtFromProvider.hasProviderEntityType()) {
            builder.setProviderType(EntityType.forNumber(
                    commoditiesBoughtFromProvider.getProviderEntityType()));
        }

        return builder.build();
    }

    /**
     * Helper method for {@link #newCommodityBought(CommoditiesBoughtFromProvider) newCommodityBought},
     * converting the inner
     * {@link com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO CommodityBoughtDTOs}
     * within a {@link CommoditiesBoughtFromProvider}.
     *
     * @param commodityBoughtDTO the XL-domain commodityBoughtDTO to convert
     * @return the SDK-domain CommodityDTO resulting from the conversion
     */
    private static CommodityDTO newCommodityDTO(
            TopologyDTO.CommodityBoughtDTO commodityBoughtDTO) {
        CommodityDTO.Builder builder = CommodityDTO.newBuilder()
                // CommodityType appears to be a 1-1 mapping, based on existing conversion code
                // going the other direction (CommodityDTO -> CommoditySoldDTO)
                .setCommodityType(CommodityType.forNumber(
                        commodityBoughtDTO.getCommodityType().getType()));

        // Copy the used setting, if present
        if (commodityBoughtDTO.hasUsed()) {
            builder.setUsed(commodityBoughtDTO.getUsed());
        }

        // Copy the peak setting, if present
        if (commodityBoughtDTO.hasPeak()) {
            builder.setPeak(commodityBoughtDTO.getPeak());
        }

        // Copy the active setting, if present
        if (commodityBoughtDTO.hasActive()) {
            builder.setActive(commodityBoughtDTO.getActive());
        }

        // Copy the display name, if present
        if (commodityBoughtDTO.hasDisplayName()) {
            builder.setDisplayName(commodityBoughtDTO.getDisplayName());
        }

        // Copy the aggregate commodity keys, if present
        if (!commodityBoughtDTO.getAggregatesList().isEmpty()) {
            builder.addPropMap(PropertiesList.newBuilder()
                .setName(SDKConstants.AGGREGATES)
                .addAllValues(commodityBoughtDTO.getAggregatesList())
                .build());
        }

        // Scaling factor is not mapped, due to no corresponding setting in the CommodityBoughtDTO

        return builder.build();
    }

    /**
     * Get a list of all target-specific {@link EntityProperty}s related to the provided entity.
     * Iterates through the list of targets that discovered this entity and extracts entity
     * properties from the raw entity data, compiling them into a list and differentiating them
     * using a namespace that is based on the target that discovered them.
     *
     * @param entity the entity to use to find entity properties
     * @return a list of all target-specific {@link EntityProperty}s related to the provided entity
     */
    private List<EntityProperty> getAllTargetSpecificEntityProperties(Entity entity) {
        return entity.getPerTargetInfo().stream()
                // Extract the list of entity properties for each target that discovered this entity
                .map(longPerTargetInfoEntry ->
                        getTargetSpecificEntityProperties(
                                longPerTargetInfoEntry.getKey(),
                                longPerTargetInfoEntry.getValue()))
                // Combine all the resulting lists into a single flattened list
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /**
     * Get a list of all {@link EntityProperty}s related to the provided target.
     * The entity properties will be given a namespace relating to the target: the name/address of
     * the target (if available) or else the id of the target.
     *
     * Entity properties provide a mechanism to store arbitrary data that is meaningful to probes,
     * but is not meaningful (e.g.) to the Market. A prime example of this is a UUID, which is needed
     * for the probe to identify an entity, but not useful within XL. We don't have fields for this
     * data in our internal model, so we store this data as namespace-key-value triplets that are
     * sent back to the probes when executing actions.
     *
     * For each target that generated entity properties for an entity, we add a special property
     * called with a key "TARGET_TYPE". The value will be the ProbeType that generated the properties,
     * and the namespace will match the namespace of the rest of the properties generated by that
     * target. A probe simply has to search for a property with key of "TARGET_TYPE" and value
     * matching its own ProbeType, and then it will know in which namespace its properties are stored.
     *
     * @param targetId the ID of the target to fetch entity properties for
     * @param perTargetInfo the raw entity info originally discovered by this target
     * @return a list of {@link EntityProperty}s related to the provided target
     */
    private List<EntityProperty> getTargetSpecificEntityProperties(final Long targetId,
                                                                   final PerTargetInfo perTargetInfo) {
        // The namespace to use for all the entity properties gathered for this target.
        // Use the name/address for this target if it is available, else use the target OID.
        // The target OID won't mean anything to the probes, so it is better to have the name/address.
        final Optional<String> targetAddress = targetStore.getTargetAddress(targetId);
        if ( ! targetAddress.isPresent()) {
            logger.warn("Target name/address could not be determined for target " + targetId
            + ". Using target ID to populate the namespace of entity properties instead.");
        }
        String namespace = targetAddress.orElse(String.valueOf(targetId));

        // Create a new list of entitiy properties, based on the entity properties extracted from
        // the provided PerTargetInfo. The new list of entity properties will have the namespace set.
        List<EntityProperty> entityProperties =
                perTargetInfo.getEntityInfo().getEntityPropertiesList().stream()
                        .map(entityProperty -> newEntityProperty(namespace, entityProperty))
                        .collect(Collectors.toCollection(ArrayList::new));

        // Add the special LocalName entity property (if it wasn't already converted from the raw
        // data), which describes the name of the entity as discovered by the target that populated
        // this namespace
        final boolean propertiesContainLocalName = entityProperties.stream()
                .anyMatch(entityProperty ->
                        SupplyChainConstants.LOCAL_NAME.equals(entityProperty.getName()));
        if ( ! propertiesContainLocalName) {
            entityProperties.add(newEntityProperty(SupplyChainConstants.LOCAL_NAME,
                    // ID is used in cases where a local name wasn't provided in the entity props
                    perTargetInfo.getEntityInfo().getId(),
                    namespace));
        }

        // Add the special TargetType entity property, which describes the name of the target that
        // populated this namespace
        String targetType = getTargetType(targetId);
        entityProperties.add(newEntityProperty(SupplyChainConstants.TARGET_TYPE,
                targetType,
                namespace));
        return  entityProperties;
    }

    private String getTargetType(final Long targetId) {
        Optional<SDKProbeType> probeType = targetStore.getProbeTypeForTarget(targetId);
        if ( ! probeType.isPresent()) {
            // We allow 3rd-party probes, so treat any unrecognized probes as probe type "OTHER"
            return PROBE_TYPE_OTHER;
        }
        // Target type is the same thing as probe type.
        return probeType.get().getProbeType();
    }

    private static EntityProperty newEntityProperty(final String namespace,
                                                    final EntityProperty entityProperty) {
        // Create a new entity property, based on the name and value from the input entity property,
        // while setting the namespace as provided.
        return newEntityProperty(
                entityProperty.getName(),
                entityProperty.getValue(),
                namespace
                );
    }

    private static EntityProperty newEntityProperty(final String name,
                                                    final String value,
                                                    final String namespace) {
        return EntityProperty.newBuilder()
                .setName(name)
                .setValue(value)
                .setNamespace(namespace)
                .build();
    }
}
