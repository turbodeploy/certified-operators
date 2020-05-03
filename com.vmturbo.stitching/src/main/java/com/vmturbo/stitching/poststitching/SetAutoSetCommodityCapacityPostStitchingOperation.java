package com.vmturbo.stitching.poststitching;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.CaseFormat;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.stats.Stats.EntityUuidAndType;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesCapacityValuesRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.Builder;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Set commodity capacity (auto scaled range).
 * The main algorithm for setting the capacity is as following:
 * if (userPolicyExits) {
 *      if (autoset from user policy == true) {
 * 	         then auto set.. take value from db last 7 days from the stats daily table.
 *      } else {
 * 	         take the value from settings -> this will search for value in user settings
 * 	         and if not found then from default settings
 *      }
 * } else {
 * 	    if (autoset == true) {
 * 		    then auto set.. take value from db last 7 days from the stats daily table.
 *      } else {
 * 		    take value from default settings
 *      }
 * }
 */
public class SetAutoSetCommodityCapacityPostStitchingOperation implements PostStitchingOperation {

    private final EntityType entityType;
    private final ProbeCategory probeCategory;
    private final String capacitySettingName;
    private final String autoSetSettingName;
    private final CommodityType commodityType;
    private final com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig commodityStitchingOperationConfig;
    private static final Logger logger = LogManager.getLogger();

    /**
     * Creates an instance of this class.
     * @param entityType entity type
     * @param probeCategory probe category
     * @param commodityType commodity type
     * @param capacitySettingName capacity setting name
     * @param autoSetSettingName auto scale setting name
     */
    public SetAutoSetCommodityCapacityPostStitchingOperation(
            @Nonnull final EntityType entityType,
            @Nonnull final ProbeCategory probeCategory,
            @Nonnull final CommodityType commodityType,
            @Nonnull final String capacitySettingName,
            @Nonnull final String autoSetSettingName,
            @Nonnull com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig commodityStitchingOperationConfig) {
        this.entityType = Objects.requireNonNull(entityType);
        this.probeCategory = Objects.requireNonNull(probeCategory);
        this.commodityType = Objects.requireNonNull(commodityType);
        this.capacitySettingName = Objects.requireNonNull(capacitySettingName);
        this.autoSetSettingName = Objects.requireNonNull(autoSetSettingName);
        this.commodityStitchingOperationConfig = commodityStitchingOperationConfig;
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeCategoryEntityTypeScope(probeCategory, entityType);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {
        Map<TopologyEntity, Set<Builder>> entitiesToUpdateCapacity = new HashMap<>();
        // iterate over entities and if the named setting exists for that entity, find all
        // sold commodities of the correct type and set their capacities according to the
        // value in the setting.
        entities.forEach(entity -> {
            final Optional<Setting> capacitySetting = settingsCollection
                    .getEntitySetting(entity.getOid(), capacitySettingName);
            if (!capacitySetting.isPresent()) {
                logger.error("Capacity Setting {} does not exist for entity {}."
                                + " Not setting capacity for it.", capacitySettingName,
                        entity.getDisplayName());
            } else {
                // Checking if the capacity value should be updated from the db
                 if (shouldUpdateCapacityFromDb(entity, settingsCollection)) {
                    entitiesToUpdateCapacity.put(entity, entity.getTopologyEntityDtoBuilder()
                        .getCommoditySoldListBuilderList().stream()
                        .filter(this::commodityTypeMatches)
                        .collect(Collectors.toSet()));
                } else {
                    // Queueing and updating entities which we can determine locally their capacity value
                    resultBuilder.queueUpdateEntityAlone(entity,
                            entityToUpdate -> entityToUpdate.getTopologyEntityDtoBuilder()
                                    .getCommoditySoldListBuilderList().stream()
                                    .filter(this::commodityTypeMatches)
                                    .forEach(commSold ->
                                            commSold.setCapacity(getCapacityValueToSet(settingsCollection,
                                                entity))));
                }
            }
        });
        // filtering ut the entities without commodities
        final Map<TopologyEntity, Set<Builder>> entitySetMap = entitiesToUpdateCapacity.entrySet().stream()
            .filter(entityEntry -> !entityEntry.getValue().isEmpty())
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        // will be used to match the oid from the stats response to the entity
        final Map<Long, TopologyEntity> oidToEntity = entitySetMap.keySet().stream()
            .collect(Collectors.toMap(e -> e.getOid(), e -> e));
        if (!entitySetMap.isEmpty()) {
            updateEntitiesFromDb(entitySetMap, resultBuilder, oidToEntity, settingsCollection);
        }
        return resultBuilder.build();
    }

    private void updateEntitiesFromDb(final Map<TopologyEntity, Set<Builder>> entitySetMap,
                                      final EntityChangesBuilder<TopologyEntity> resultBuilder,
                                      final Map<Long, TopologyEntity> oidToEntities,
                                      final EntitySettingsCollection settingsCollection) {
        final GetEntityCommoditiesCapacityValuesRequest.Builder requestBuilder = buildRequest(entitySetMap);
        // Getting the response from history component and queueing entity capacity update
        if (commodityStitchingOperationConfig.getStatsClient() != null) {
            commodityStitchingOperationConfig.getStatsClient().getEntityCommoditiesCapacityValues(requestBuilder.build())
                .forEachRemaining(response -> {
                    response.getEntitiesToCommodityTypeCapacityList().forEach(entityToCommodity -> {
                        resultBuilder.queueUpdateEntityAlone(oidToEntities.get(entityToCommodity.getEntityUuid()),
                            entityToUpdate -> entityToUpdate.getTopologyEntityDtoBuilder()
                                .getCommoditySoldListBuilderList().stream()
                                .filter(this::commodityTypeMatches)
                                .forEach(commSold ->    // updating the capacity from the response
                                    commSold.setCapacity(entityToCommodity.getCapacity())
                                ));
                        oidToEntities.remove(entityToCommodity.getEntityUuid());
                        }
                    );
                });
        }
        // updating with the entities that did not return any data from the history component
        // with max between the current capacity and the default policy values
        oidToEntities.entrySet().forEach(oidToEntity -> {
            final Optional<Setting> capacitySetting = settingsCollection
                .getEntitySetting(oidToEntity.getKey(), capacitySettingName);
            float policyCapacity = Float.MIN_VALUE;
            if (capacitySetting.isPresent()) {
                policyCapacity = settingsCollection.getEntitySetting(oidToEntity.getKey(), capacitySettingName)
                    .get().getNumericSettingValue().getValue();
            }
            final double currentCapacity = oidToEntity.getValue().getTopologyEntityDtoBuilder()
                .getCommoditySoldListBuilderList().get(0).getCapacity();
            final float capacityValue = Math.max((float)currentCapacity, policyCapacity);
            resultBuilder.queueUpdateEntityAlone(oidToEntity.getValue(), entityToUpdate ->
                entityToUpdate.getTopologyEntityDtoBuilder()
                .getCommoditySoldListBuilderList().stream()
                .filter(this::commodityTypeMatches)
                .forEach(commSold ->
                    commSold.setCapacity(capacityValue)
                ));
        });
    }

    /**
     * Converting the input map to a GetEntityCommoditiesCapacityValuesRequest.
     * The request contains the commodity type from the operation used by the history component to get
     * the matching table, then from the input map of Entity to a Set of commoditiesSold we build a set of
     * EntityToCommodity entries for each entity uuid, entity type, commodity key
     *
     * @param entitySetMap entity to sold commodities
     * @return request for sending history component
     */
    private GetEntityCommoditiesCapacityValuesRequest.Builder buildRequest(final Map<TopologyEntity, Set<Builder>> entitySetMap) {
        final GetEntityCommoditiesCapacityValuesRequest.Builder requestBuilder =
            GetEntityCommoditiesCapacityValuesRequest.newBuilder()
                .setCommodityTypeName(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL,commodityType.name()));

        final Set<Set<EntityUuidAndType.Builder>> entityToCommoditySet = entitySetMap.entrySet().stream().map(
            entry -> entry.getValue().stream()
                .map(commodity -> EntityUuidAndType.newBuilder()
                .setEntityUuid(entry.getKey().getOid())
                .setEntityType(entry.getKey().getEntityType()))
                .collect(Collectors.toSet())).collect(Collectors.toSet());

        requestBuilder.addAllEntityUuidAndTypeSet(
            entityToCommoditySet.stream()
                .flatMap(entry -> entry.stream()).map(entityToCommodity -> entityToCommodity.build())
                .collect(Collectors.toSet()));

        return requestBuilder;
    }

    /**
     * Indicate if we need to go the the db for updating the capacity.
     * The condition is : if (user policy exists and it says autoset from the user policy is true or
     * user policy does not exist and autoset from default policy is true.
     *
     * @param entity for which we need to set capacity
     * @param settingsCollection contains the settings we check in
     * @return true if an update from db is needed, false otherwise.
     */
    private boolean shouldUpdateCapacityFromDb(final TopologyEntity entity,
                                                final EntitySettingsCollection settingsCollection) {
        if (settingsCollection.hasUserPolicySettings(entity.getOid())) {
            final Optional<Setting> entityUserSetting = settingsCollection.getEntityUserSetting(entity,
                EntitySettingSpecs.getSettingByName(autoSetSettingName).get());
            if (entityUserSetting.isPresent() && entityUserSetting.get().getBooleanSettingValue().getValue()) {
                return true;
            }
        } else {
            if (settingsCollection.getEntitySetting(entity.getOid(), autoSetSettingName)
                .get().getBooleanSettingValue().getValue()) {
                return true;
            }
        }
        return false;
    }

    @Nonnull
    @Override
    public String getOperationName() {
        return String.join("_", getClass().getSimpleName(),
                probeCategory.getCategory(), entityType.toString(), commodityType.name(),
                capacitySettingName, autoSetSettingName);
    }

    private boolean commodityTypeMatches(TopologyDTO.CommoditySoldDTO.Builder commodity) {
        return commodity.getCommodityType().getType() == commodityType.getNumber();
    }

    private double getCapacityValueToSet(EntitySettingsCollection settingsCollection,
                                         TopologyEntity entity) {
        // updating the capacity according to the policies, first user policies and then default
        if (settingsCollection.hasUserPolicySettings(entity.getOid())) {
            final Optional<Setting> autoSetEntityUserSetting = settingsCollection
                .getEntityUserSetting(entity, EntitySettingSpecs.getSettingByName(autoSetSettingName).get());
            if (autoSetEntityUserSetting.isPresent() && autoSetEntityUserSetting.get().getBooleanSettingValue().getValue()) {
                logger.warn("Capacity value for {} was incorrectly set", entity.getDisplayName());
                return 0;
            } else {
                return settingsCollection.getEntitySetting(entity.getOid(), capacitySettingName)
                    .get().getNumericSettingValue().getValue();
            }
        } else {
            final Optional<Setting> autoSetSetting = settingsCollection.getEntitySetting(entity.getOid(),
                autoSetSettingName);
            if (autoSetSetting.isPresent() &&
                autoSetSetting.get().getBooleanSettingValue().getValue()) {
                logger.warn("Capacity value for {} was incorrectly set", entity.getDisplayName());
                return 0;
            } else {
                return settingsCollection.getEntitySetting(entity.getOid(), capacitySettingName)
                    .get().getNumericSettingValue().getValue();
            }
        }
    }
}
