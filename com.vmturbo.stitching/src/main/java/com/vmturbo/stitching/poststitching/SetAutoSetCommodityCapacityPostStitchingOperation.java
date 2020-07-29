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
import com.vmturbo.common.protobuf.stats.Stats.EntityToCommodityTypeCapacity;
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
 * if (user policy exits) {
 *     if ("Enable SLO" setting from user policy is enabled) {
 *         Take the SLO value from the user settings and use it as capacity.
 *         If not found, take the SLO value from the default settings.
 *     } else {
 *         Auto-set the capacity.
 *     }
 * } else {
 *     if ("Enable SLO" setting from default policy is enabled) {
 *         Take the SLO value from the default settings and use it as capacity.
 *     } else {
 *         Auto-set the capacity.
 *     }
 * }
 *
 *<p>** Auto setting is done as the following:
 * Take highest capacity value from db last 7 days (or hours on initialization) from the stats daily/hourly table,
 * Then use the weighted avg of:
 * (HISTORICAL_CAPACITY_WEIGHT * db value) + (CURRENT_CAPACITY_WEIGHT * capacity returned from probe)
 *
 * If no data returned from the db we will set the 'commodity used' value as the capacity
 */
public class SetAutoSetCommodityCapacityPostStitchingOperation implements PostStitchingOperation {

    private final EntityType entityType;
    private final ProbeCategory probeCategory;
    private final String sloValueSettingName;
    private final String enableSLOSettingName;
    private final CommodityType commodityType;
    private final com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig commodityStitchingOperationConfig;
    private static final Logger logger = LogManager.getLogger();
    private static final double HISTORICAL_CAPACITY_WEIGHT = 0.8;
    private static final double CURRENT_CAPACITY_WEIGHT = 0.2;

    /**
     * Creates an instance of this class.
     * @param entityType entity type
     * @param probeCategory probe category
     * @param commodityType commodity type
     * @param sloValueSettingName the SLO value setting name
     * @param enableSLOSettingName the Enable SLO setting name
     */
    public SetAutoSetCommodityCapacityPostStitchingOperation(
            @Nonnull final EntityType entityType,
            @Nonnull final ProbeCategory probeCategory,
            @Nonnull final CommodityType commodityType,
            @Nonnull final String sloValueSettingName,
            @Nonnull final String enableSLOSettingName,
            @Nonnull com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig commodityStitchingOperationConfig) {
        this.entityType = Objects.requireNonNull(entityType);
        this.probeCategory = Objects.requireNonNull(probeCategory);
        this.commodityType = Objects.requireNonNull(commodityType);
        this.sloValueSettingName = Objects.requireNonNull(sloValueSettingName);
        this.enableSLOSettingName = Objects.requireNonNull(enableSLOSettingName);
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
            final Optional<Setting> sloSetting = settingsCollection
                    .getEntitySetting(entity.getOid(), sloValueSettingName);
            if (!sloSetting.isPresent()) {
                logger.error("SLO Setting {} does not exist for entity {}."
                                + " Not setting capacity for it.", sloValueSettingName,
                        entity.getDisplayName());
            } else {
                // Checking if the capacity value should be updated from the db
                 if (shouldUpdateCapacityFromDb(entity, settingsCollection)) {
                    entitiesToUpdateCapacity.put(entity, entity.getTopologyEntityDtoBuilder()
                        .getCommoditySoldListBuilderList().stream()
                        .filter(this::commodityTypeMatches)
                        .collect(Collectors.toSet()));
                } else {
                     // Queueing and updating entities which we can determine their capacity value
                     // by using the SLO value from the settings as capacity.
                    resultBuilder.queueUpdateEntityAlone(entity,
                            entityToUpdate -> entityToUpdate.getTopologyEntityDtoBuilder()
                                    .getCommoditySoldListBuilderList().stream()
                                    .filter(this::commodityTypeMatches)
                                    .forEach(commSold -> commSold.setCapacity(
                                            getSLOValueFromSetting(settingsCollection, entity))));
                }
            }
        });
        // filtering out the entities without commodities
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
                                      final Map<Long, TopologyEntity> oidToEntities, final EntitySettingsCollection settingsCollection) {
        final GetEntityCommoditiesCapacityValuesRequest.Builder requestBuilder = buildRequest(entitySetMap);
        // Getting the response from history component and queueing entity capacity update
        if (commodityStitchingOperationConfig.getStatsClient() != null) {
            commodityStitchingOperationConfig.getStatsClient().getEntityCommoditiesCapacityValues(requestBuilder.build())
                .forEachRemaining(response -> {
                    if (response.getEntitiesToCommodityTypeCapacityList() != null) {
                        response.getEntitiesToCommodityTypeCapacityList().forEach(entityToCommodity -> {
                            final TopologyEntity topologyEntity = oidToEntities.get(entityToCommodity.getEntityUuid());
                            if (topologyEntity != null) {
                                    resultBuilder.queueUpdateEntityAlone(topologyEntity,
                                        entityToUpdate -> entityToUpdate.getTopologyEntityDtoBuilder()
                                            .getCommoditySoldListBuilderList().stream()
                                            .filter(this::commodityTypeMatches)
                                            .forEach(commSold -> // updating the capacity from the response
                                                setValidCapacity(commSold, entityToCommodity, entityToUpdate)
                                            ));
                                    oidToEntities.remove(entityToCommodity.getEntityUuid());
                                } else {
                                    logger.error("Was not able to find matching entity for {}",
                                        entityToCommodity.getEntityUuid());
                                }
                            }
                        );
                    }
                });
        }
        // updating with the entities that did not return any data from the history component
        // with the current used capacity or policy
        oidToEntities.entrySet().forEach(oidToEntity -> {
            resultBuilder.queueUpdateEntityAlone(oidToEntity.getValue(), entityToUpdate ->
                entityToUpdate.getTopologyEntityDtoBuilder()
                .getCommoditySoldListBuilderList().stream()
                .filter(this::commodityTypeMatches)
                .forEach(commSold ->
                    // set the used value if its not 0, otherwise set the policy value
                    commSold.setCapacity(commSold.getUsed() != 0 ? commSold.getUsed() :
                        settingsCollection
                            .getEntitySetting(entityToUpdate.getOid(), sloValueSettingName)
                            // we know its not empty because that is the first validation in performOperation
                            .get().getNumericSettingValue().getValue())
                ));
        });
    }

    /**
     * Setting the commodity capacity after null check and debug logging.
     *
     * @param commSold to which we want to set the value
     * @param entityToCommodity contains the historical value returned from the db
     * @param entityToUpdate contains the current 'used' value returned from the probe
     */
    private void setValidCapacity(final Builder commSold, final EntityToCommodityTypeCapacity entityToCommodity,
                                  final TopologyEntity entityToUpdate) {
        logger.debug("Calculating sold {} commodity capacity for {} with current used capacity: {} "
                + "and historical capacity: {}", commodityType.name(), entityToUpdate.getDisplayName(),
            commSold.getUsed(), entityToCommodity.getCapacity());
        commSold.setCapacity(getWeightedAvgCapacity(
            entityToCommodity.getCapacity(),
            commSold.getUsed()));
    }

    private double getWeightedAvgCapacity(double historicalCapacity, double currentCapacity) {
        return (historicalCapacity * HISTORICAL_CAPACITY_WEIGHT + currentCapacity * CURRENT_CAPACITY_WEIGHT);
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
     * Get a specific setting for a specific entity.
     * If a user setting exists, it is returned. If not, the default setting is returned.
     *
     * @param entity the entity for which to get the setting.
     * @param settingsCollection the settings collection.
     * @param settingName the name of the setting.
     * @return the user / default setting for the entity.
     */
    private Optional<Setting> getSetting(@Nonnull final TopologyEntity entity,
                                         @Nonnull final EntitySettingsCollection settingsCollection,
                                         @Nonnull final String settingName) {
        if (settingsCollection.hasUserPolicySettings(entity.getOid())) {
            return settingsCollection.getEntityUserSetting(entity,
                    EntitySettingSpecs.getSettingByName(settingName).get());

        }
        return settingsCollection.getEntitySetting(entity.getOid(), settingName);
    }

    /**
     * Indicate if we need to go the the db for updating the capacity (auto-set).
     * If a user policy exists and its "Enable SLO" setting is disabled, or a user policy does not
     * exist and the default policy's "Enable SLO" setting is disabled, then return true (and
     * auto-set algorithm will be used).
     *
     * @param entity for which we need to set capacity
     * @param settingsCollection contains the settings we check in
     * @return true if auto-set is needed, false otherwise.
     */
    private boolean shouldUpdateCapacityFromDb(final TopologyEntity entity,
                                               final EntitySettingsCollection settingsCollection) {
        return getSetting(entity, settingsCollection, enableSLOSettingName)
                .map(setting -> !setting.getBooleanSettingValue().getValue())
                .orElse(true);
    }

    @Nonnull
    @Override
    public String getOperationName() {
        return String.join("_", getClass().getSimpleName(),
                probeCategory.getCategory(), entityType.toString(), commodityType.name(),
                sloValueSettingName, enableSLOSettingName);
    }

    private boolean commodityTypeMatches(TopologyDTO.CommoditySoldDTO.Builder commodity) {
        return commodity.getCommodityType().getType() == commodityType.getNumber();
    }

    /**
     * Get the SLO from the relevant user / default setting.
     * If the "Enable SLO" setting is true and SLO numeric setting exists, then return
     * the SLO value from the setting. Otherwise return 0.
     *
     * @param settingsCollection the settings collection.
     * @param entity the entity for which to get the SLO value.
     * @return the SLO value if exists, 0 otherwise.
     */
    private double getSLOValueFromSetting(EntitySettingsCollection settingsCollection,
                                         TopologyEntity entity) {
        return getSetting(entity, settingsCollection, enableSLOSettingName)
                // make sure that the "Enable SLO" setting is true
                .filter(setting -> setting.getBooleanSettingValue().getValue())
                .map(setting -> getSetting(entity, settingsCollection, sloValueSettingName)
                        .map(valueSetting -> valueSetting.getNumericSettingValue().getValue())
                        .orElse(0f))
                .orElse(0f);
    }
}
