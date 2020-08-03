package com.vmturbo.history.db;

import java.util.Collection;
import java.util.Optional;

import org.jooq.Table;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.stats.StatsUtils;
import com.vmturbo.history.schema.abstraction.tables.Entities;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;

/**
 * Instances of this type provide information that's important to history component behavior,
 * regarding the entity types it may encounter in topologies or in API queries.
 */
public interface EntityType {

    /**
     * Obtain an entity type instance for the given name.
     *
     * <p>Where applicable, names should be consistent with names used by the API, e.g. names
     * appearing in {@link ApiEntityType} values.</p>
     *
     * @param name name of entity type
     * @return corresponding entity type, if it exists
     */
    static Optional<EntityType> named(String name) {
        return Optional.ofNullable(EntityTypeDefinitions.NAME_TO_ENTITY_TYPE_MAP.get(name));
    }

    /**
     * Obtain an entity type instance for the given name.
     *
     * @param name name of entity type
     * @return corresponding entity type, if it exists
     * @throws IllegalArgumentException if no such entity exists
     */
    static EntityType get(String name) throws IllegalArgumentException {
        return EntityType.named(name).orElseThrow(() ->
            new IllegalArgumentException("EntityType " + name + " not found"));
    }

    /**
     * Get the name of this entity type.
     *
     * @return entity type name
     */
    String getName();

    /**
     * Resolve this entity type to the type to which it is aliased.
     *
     * <p>Multiple aliasing steps are followed, if present, so the result will be an un-aliased
     * entity type.</p>
     *
     * @return this entity type if it is not aliased, else the type at the end of its aliasing chain
     */
    EntityType resolve();

    /**
     * Return the stats table that stores latest records for this entity type.
     *
     * @return the latest table, if this entity type has one
     */
    Optional<Table<?>> getLatestTable();

    /**
     * Return the stats table that stores hourly rollup records for this entity type.
     *
     * @return the hourly table, if this entity type has one
     */
    Optional<Table<?>> getHourTable();

    /**
     * Return the stats table that stores daily rollup records for this entity type.
     *
     * @return the daily table, if this entity type has one
     */
    Optional<Table<?>> getDayTable();

    /**
     * Return the stats table that stores monthly rollup records for this entity type.
     *
     * @return the monthly table, if this entity type has one
     */
    Optional<Table<?>> getMonthTable();

    /**
     * Get the table prefix for stats tables for this entity type.
     *
     * <p>This value is joined with the table suffixes for the various time frames, with an
     * intervening underscore, to arrive at the table name.</p>
     *
     * <p>N.B.: For entity stats table, this includes the word "stats" - e.g. the previs for VM
     * tables is "vm_stats", not "vm".</p>
     *
     * @return the table prefix for this entity, if this entity has stats tables
     */
    Optional<String> getTablePrefix();

    /**
     * Get the entity stats table where this table stores data for the given time frame.
     *
     * @param timeFrame desired time frame - "latest" or a rollup time frame
     * @return corresponding stats table, if this entity has one
     * @throws IllegalArgumentException if an invalid timeframe is provided
     */
    Optional<Table<?>> getTimeFrameTable(TimeFrame timeFrame) throws IllegalArgumentException;

    /**
     * Obtain the entity type that uses the given table for history stats (latest or rollups).
     *
     * @param table table
     * @return associated entity type, if any
     */
    static Optional<EntityType> fromTable(Table<?> table) {
        return Optional.ofNullable(EntityTypeDefinitions.TABLE_TO_ENTITY_TYPE_MAP.get(table));
    }

    /**
     * Get the SDK entity type associated with this entity type, if any.
     *
     * @return associated SDK entity type, if any
     */
    Optional<EntityDTO.EntityType> getSdkEntityType();

    /**
     * Get the entity type that's associated with the given SDK entity type, if any.
     *
     * @param sdkEntityType SDK entity type
     * @return corresponding entity type, if any
     */
    static Optional<EntityType> fromSdkEntityType(EntityDTO.EntityType sdkEntityType) {
        return fromSdkEntityType(sdkEntityType.getNumber());
    }

    /**
     * Get the entity type that's associated with the given SDK entity type number, if any.
     *
     * @param sdkEntityTypeNo SDK entity type number
     * @return corresponding entity type, if any
     */
    static Optional<EntityType> fromSdkEntityType(int sdkEntityTypeNo) {
        return Optional.ofNullable(EntityTypeDefinitions.SDK_TO_ENTITY_TYPE_MAP.get(sdkEntityTypeNo));
    }


    /**
     * Check whether the given entity type has the given use case.
     *
     * @param useCase use case to check
     * @return true if the entity type has the use case
     */
    boolean hasUseCase(UseCase useCase);

    /**
     * Check whether entities of this type should be persisted in the {@link Entities} table.
     *
     * @return true if entities should be persisted
     */
    default boolean persistsEntity() {
        return hasUseCase(UseCase.PersistEntity);
    }

    /**
     * Check whether entities of this type should have commodity/attribute stats persisted to
     * stats tables.
     *
     * @return true if entity stats should be persisted
     */
    default boolean persistsStats() {
        return hasUseCase(UseCase.PersistStats);
    }

    /**
     * Check whether entities of this type participate in entity stats rollup processing.
     *
     * @return true if entity stats data should be rolled up
     */
    default boolean rollsUp() {
        return hasUseCase(UseCase.RollUp);
    }

    /**
     * Check whether entities of this type should persist price index data to stats tables.
     *
     * @return true if price index data should be persisted
     */
    default boolean persistsPriceIndex() {
        return getSdkEntityType()
            .map(sdkType -> !StatsUtils.SDK_ENTITY_TYPES_WITHOUT_SAVED_PRICES.contains(
                sdkType.getNumber()))
            .orElse(false);
    }

    /**
     * Check whether this is a spend entity type.
     *
     * @return true if this is a spend entity type
     */
    default boolean isSpend() {
        return hasUseCase(UseCase.Spend);
    }

    /**
     * Get a collection of all the defined entity types.
     *
     * @return all defined entity types
     */
    static Collection<EntityType> allEntityTypes() {
        return EntityTypeDefinitions.ENTITY_TYPE_DEFINITIONS;
    }

    /**
     * Use-cases that may apply to individual entity types.
     */
    enum UseCase {
        /** Entities are persisted to the entities table. */
        PersistEntity,
        /** Entity attributes and bought/sold commodities are persisted to stats tables. */
        PersistStats,
        /** Price index data is persisted to stats tables. */
        RollUp,
        /** This is a spend entity. */
        Spend;

        static final UseCase[] STANDARD_STATS = new UseCase[]{
            PersistEntity, PersistStats, RollUp
        };

        static final UseCase[] NON_ROLLUP_STATS = new UseCase[]{
            PersistEntity, PersistStats
        };

        static final UseCase[] NON_PRICE_STATS = new UseCase[]{
            PersistEntity, PersistEntity, RollUp
        };
    }
}
