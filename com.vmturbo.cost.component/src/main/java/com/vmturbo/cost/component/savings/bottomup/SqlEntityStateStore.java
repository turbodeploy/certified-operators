package com.vmturbo.cost.component.savings.bottomup;

import static com.vmturbo.cost.component.db.Tables.ENTITY_CLOUD_SCOPE;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_STATE;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.cost.component.db.tables.records.EntitySavingsStateRecord;
import com.vmturbo.cost.component.entity.scope.SQLEntityCloudScopedStore;
import com.vmturbo.cost.component.savings.EntitySavingsException;
import com.vmturbo.cost.component.savings.EntityState;
import com.vmturbo.cost.component.savings.StateStore;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Implementation of EntityStateStore that persists data in entity_savings_state table.
 */
public class SqlEntityStateStore extends SQLEntityCloudScopedStore implements EntityStateStore<DSLContext>,
        StateStore {

    private final Logger logger = LogManager.getLogger();

    /**
     * Used for batch operations.
     */
    private final int chunkSize;

    /**
     * JOOQ access.
     */
    private final DSLContext dsl;

    /**
     * Constructor.
     *
     * @param dsl Dsl context for jooq
     * @param chunkSize chunk size for batch operations
     */
    public SqlEntityStateStore(@Nonnull final DSLContext dsl, final int chunkSize) {
        super(dsl, chunkSize);
        this.dsl = dsl;
        this.chunkSize = chunkSize;
    }

    @Override
    public EntityState getEntityState(long entityId) throws EntitySavingsException {
        return getEntityStates(ImmutableSet.of(entityId)).get(entityId);
    }

    @Override
    public void getEntityStates(Consumer<EntityState> consumer) {
        getAllEntityStates(dsl, consumer);
    }

    @Nonnull
    @Override
    public Map<Long, EntityState> getEntityStates(@Nonnull final Set<Long> entityIds) throws EntitySavingsException {
        final Result<Record2<Long, String>> records;
        try {
            records = dsl.select(ENTITY_SAVINGS_STATE.ENTITY_OID, ENTITY_SAVINGS_STATE.ENTITY_STATE)
                    .from(ENTITY_SAVINGS_STATE)
                    .where(ENTITY_SAVINGS_STATE.ENTITY_OID.in(entityIds))
                    .fetch();
            return records.stream().collect(Collectors.toMap(Record2::value1,
                    record -> EntityState.fromJson(record.value2())));
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when getting entity states from database.", e);
        }
    }

    /**
     * Get a map of entity states that need to be processed even without driving events.  This
     * includes all entity states that were updated in the previous calculation pass and entity
     * states that contain an expired action.
     *
     * @param timestamp timestamp of the end of the period being processed
     * @param uuids if non-empty, return only the forced entity states that are in the uuid list
     * @return a map of entity_oid -> EntityState that must be processed.
     * @throws EntitySavingsException when an error occurs
     */
    @Override
    public Map<Long, EntityState> getForcedUpdateEntityStates(LocalDateTime timestamp,
            Set<Long> uuids)
            throws EntitySavingsException {
        final Result<Record2<Long, String>> records;
        try {
            Condition where = ENTITY_SAVINGS_STATE.UPDATED.eq((byte)1)
                    .or(ENTITY_SAVINGS_STATE.NEXT_EXPIRATION_TIME.le(timestamp));
            if (!uuids.isEmpty()) {
                where = where.and(ENTITY_SAVINGS_STATE.ENTITY_OID.in(uuids));
            }
            records = dsl.select(ENTITY_SAVINGS_STATE.ENTITY_OID, ENTITY_SAVINGS_STATE.ENTITY_STATE)
                    .from(ENTITY_SAVINGS_STATE)
                    .where(where)
                    .fetch();
            return records.stream().collect(Collectors.toMap(Record2::value1,
                    record -> EntityState.fromJson(record.value2())));
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when getting entity states from database.", e);
        }
    }

    @Override
    public void clearUpdatedFlags(@Nonnull DSLContext dsl, Set<Long> uuids)
            throws EntitySavingsException {
        try {
            Condition where = ENTITY_SAVINGS_STATE.UPDATED.eq((byte)1);
            if (!uuids.isEmpty()) {
                where = where.and(ENTITY_SAVINGS_STATE.ENTITY_OID.in(uuids));
            }
            dsl.update(ENTITY_SAVINGS_STATE)
                    .set(ENTITY_SAVINGS_STATE.UPDATED, (byte)0)
                    .where(where)
                    .execute();
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when getting entity states from database.", e);
        }
    }

    @Override
    public void deleteEntityStates(@Nonnull final Set<Long> entityIds, @Nonnull DSLContext dsl) throws EntitySavingsException {
        try {
            dsl.deleteFrom(ENTITY_SAVINGS_STATE)
                    .where(ENTITY_SAVINGS_STATE.ENTITY_OID.in(entityIds))
                    .execute();
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when deleting entity states.", e);
        }
    }

    /**
     * Update a single existing entity states. This is used by the TEM to updated existing state,
     * and does not affect the cloud topology table.
     *
     * @param entityState entity state to write
     * @throws EntitySavingsException error during operation
     */
    @Override
    public void updateEntityState(@Nonnull final EntityState entityState)
            throws EntitySavingsException {
        EntitySavingsStateRecord record = ENTITY_SAVINGS_STATE.newRecord();
        record.setEntityOid(entityState.getEntityId());
        record.setUpdated(entityState.isUpdated() ? (byte)1 : (byte)0);
        record.setNextExpirationTime(Timestamp.from(Instant.ofEpochMilli(entityState
                .getNextExpirationTime())).toLocalDateTime());
        record.setEntityState(entityState.toJson());
        List<EntitySavingsStateRecord> records = new ArrayList<>();
        records.add(record);

        try {
            dsl.loadInto(ENTITY_SAVINGS_STATE)
                    .batchAfter(chunkSize)
                    .onDuplicateKeyUpdate()
                    .loadRecords(records)
                    .fields(ENTITY_SAVINGS_STATE.ENTITY_OID,
                            ENTITY_SAVINGS_STATE.UPDATED,
                            ENTITY_SAVINGS_STATE.ENTITY_STATE,
                            ENTITY_SAVINGS_STATE.NEXT_EXPIRATION_TIME)
                    .execute();
        } catch (IOException e) {
            throw new EntitySavingsException("Error occurred when updating entity states.", e);
        }
    }

    @Override
    public void updateEntityStates(@Nonnull final Map<Long, EntityState> entityStateMap,
            @Nonnull final TopologyEntityCloudTopology cloudTopology,
            @Nonnull final Set<Long> testEntityOidSet)
            throws EntitySavingsException {
        updateEntityStates(entityStateMap, cloudTopology, dsl, testEntityOidSet);
    }

    /**
     * Update entity states. If the state of the entity is not already in the store, create it.
     *
     * @param entityStateMap entity ID mapped to entity state
     * @param cloudTopology cloud topology
     * @param dsl DSL context for jooq
     * @param uuids if non-empty, the list of UUIDs to be updated, else all UUIDs will be updated
     * @throws EntitySavingsException error during operation
     */
    @Override
    public void updateEntityStates(@Nonnull final Map<Long, EntityState> entityStateMap,
            @Nonnull final TopologyEntityCloudTopology cloudTopology,
            @Nonnull DSLContext dsl, @Nonnull final Set<Long> uuids)
            throws EntitySavingsException {
        // If limiting the number of states to be updated, remove the entities that aren't selected.
        if (!uuids.isEmpty()) {
            entityStateMap.keySet().removeAll(entityStateMap.keySet().stream()
                    .filter(uuid -> !uuids.contains(uuid))
                    .collect(Collectors.toSet()));
        }

        // State records requires the corresponding scope record is already present in the scope table.
        // Only create state records that have a scope record.
        List<EntityCloudScopeRecord> scopeRecords = entityStateMap.keySet().stream()
                .map(entityOid -> createCloudScopeRecord(entityOid, cloudTopology,
                        uuids.contains(entityOid)))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        try {
            insertCloudScopeRecords(scopeRecords);
        } catch (IOException e) {
            throw new EntitySavingsException("Error occurred when writing to entity_cloud_scope table.", e);
        }

        scopeRecords.addAll(getScopeRecordsForMissingEntities(scopeRecords, entityStateMap));

        Set<Long> scopeRecordEntityIds = scopeRecords.stream()
                .map(EntityCloudScopeRecord::getEntityOid)
                .collect(Collectors.toSet());

        List<EntitySavingsStateRecord> records = new ArrayList<>();
        entityStateMap.values().forEach(entityState -> {
            if (scopeRecordEntityIds.contains(entityState.getEntityId())) {
                EntitySavingsStateRecord record = ENTITY_SAVINGS_STATE.newRecord();
                record.setEntityOid(entityState.getEntityId());
                record.setUpdated(entityState.isUpdated() ? (byte)1 : (byte)0);
                record.setNextExpirationTime(Timestamp.from(Instant.ofEpochMilli(entityState
                        .getNextExpirationTime())).toLocalDateTime());
                record.setEntityState(entityState.toJson());
                records.add(record);
            } else {
                logger.warn("Entity state cannot be created for entity {} because its corresponding "
                        + "record does not exist in the entity_cloud_scope table. ",
                        entityState.getEntityId());
            }
        });

        try {
            dsl.loadInto(ENTITY_SAVINGS_STATE)
                    .batchAfter(chunkSize)
                    .onDuplicateKeyUpdate()
                    .loadRecords(records)
                    .fields(ENTITY_SAVINGS_STATE.ENTITY_OID,
                            ENTITY_SAVINGS_STATE.UPDATED,
                            ENTITY_SAVINGS_STATE.ENTITY_STATE,
                            ENTITY_SAVINGS_STATE.NEXT_EXPIRATION_TIME)
                    .execute();
        } catch (IOException e) {
            throw new EntitySavingsException("Error occurred when updating entity states.", e);
        }
    }

    /**
     * If an entity is deleted, it won't be present in the cloud topology. The method
     * createCloudScopeRecord will not return an EntityCloudScopeRecord object for that entity.
     * This method will find entity OIDs that has a state but is not present in the scopeRecords
     * list returned from createCloudScopeRecord. It then makes a query from the scope table for the
     * scope record directly using the entity OIDs.
     *
     * @param scopeRecords list of scope records returned from createCloudScopeRecord
     * @param entityStateMap entity state map
     * @return list of EntityCloudScopeRecord for deleted entities
     */
    private List<EntityCloudScopeRecord> getScopeRecordsForMissingEntities(
            @Nonnull List<EntityCloudScopeRecord> scopeRecords,
            @Nonnull final Map<Long, EntityState> entityStateMap) {
        Set<Long> scopeRecordEntityIds = scopeRecords.stream()
                .map(EntityCloudScopeRecord::getEntityOid)
                .collect(Collectors.toSet());
        // Find entity OIDs that have a state but is not in the scopeRecords list.
        Set<Long> stateEntityOids = new HashSet<>(entityStateMap.keySet());
        stateEntityOids.removeAll(scopeRecordEntityIds);

        if (!stateEntityOids.isEmpty()) {
            return dsl.selectFrom(ENTITY_CLOUD_SCOPE)
                    .where(ENTITY_CLOUD_SCOPE.ENTITY_OID.in(stateEntityOids))
                    .fetch();
        }
        return new ArrayList<>();
    }

    @Override
    public void getAllEntityStates(@Nonnull Consumer<EntityState> consumer) {
        getAllEntityStates(dsl, consumer);
    }

    @Override
    public void getAllEntityStates(@Nonnull DSLContext transactionContext,
            @Nonnull Consumer<EntityState> consumer) {
        // Use jooq lazy fetch to avoid load the whole table into memory.
        // https://www.jooq.org/doc/3.2/manual/sql-execution/fetching/lazy-fetching/
        // https://stackoverflow.com/questions/32209248/java-util-stream-with-resultset
        transactionContext.connection(conn -> {
            conn.setAutoCommit(false);
            try (Stream<EntityState> stream = DSL.using(conn, transactionContext.settings())
                    .selectFrom(ENTITY_SAVINGS_STATE)
                    .fetchSize(chunkSize)
                    .fetchStream()
                    .map(record -> EntityState.fromJson(record.getEntityState()))) {
                stream.forEach(consumer);
            }
        });
    }

    @Nullable
    private EntityCloudScopeRecord createCloudScopeRecord(Long entityOid,
            TopologyEntityCloudTopology cloudTopology, boolean isTestEntity) {
        Integer entityType;
        Long serviceProviderOid;
        Long regionOid;
        Optional<Long> availabilityZoneOid = Optional.empty();
        Long accountOid;
        Optional<Long> resourceGroupOid;
        if (isTestEntity) {
            /*
             * To implement scope for the test entities, create scope based upon the entity's UUID,
             * divide the UUID by the following:
             *  - service provider  100,000
             *  - region            10,000
             *  - account           1,000
             *  - resource group    100
             */
            entityType = EntityType.VIRTUAL_MACHINE_VALUE;
            serviceProviderOid = entityOid / 100000L;
            regionOid = entityOid / 10000L;
            accountOid = entityOid / 1000L;
            resourceGroupOid = Optional.of(entityOid / 100L);
        } else {
            if (!cloudTopology.getEntity(entityOid).isPresent()) {
                logger.debug(
                        "Cannot create entity cloud scope record because some of the required information is missing ; EntityOid={}",
                        entityOid);
                return null;
            }

            entityType = cloudTopology.getEntity(entityOid).map(TopologyEntityDTO::getEntityType).orElse(null);

            // Get the service provider OID.
            Optional<TopologyEntityDTO> serviceProvider = cloudTopology.getServiceProvider(entityOid);
            serviceProviderOid = serviceProvider.map(TopologyEntityDTO::getOid).orElse(null);

            // Get the region OID.
            Optional<TopologyEntityDTO> region = cloudTopology.getConnectedRegion(entityOid);
            regionOid = region.map(TopologyEntityDTO::getOid).orElse(null);

            // Get the availability zone OID.
            Optional<TopologyEntityDTO> availabilityZone = cloudTopology.getConnectedAvailabilityZone(entityOid);
            availabilityZoneOid = availabilityZone.map(TopologyEntityDTO::getOid);

            // Get the account OID.
            Optional<TopologyEntityDTO> businessAccount = cloudTopology.getOwner(entityOid);
            accountOid = businessAccount.map(TopologyEntityDTO::getOid).orElse(null);

            // Get the resource group OID.
            Optional<GroupAndMembers> resourceGroup = cloudTopology.getResourceGroup(entityOid);
            resourceGroupOid = resourceGroup.map(groupAndMembers -> groupAndMembers.group().getId());
        }

        if (entityType != null && serviceProviderOid != null && regionOid != null && accountOid != null) {
            return createCloudScopeRecord(entityOid, entityType, accountOid, regionOid,
                    availabilityZoneOid, serviceProviderOid, resourceGroupOid, LocalDateTime.now());
        }
        logger.warn("Cannot create entity cloud scope record because required information is missing."
                        + " EntityOid={}, EntityType={}, serviceProviderOid={}, regionOid={}, accountOid={}",
                entityOid, entityType, serviceProviderOid, regionOid, accountOid);

        return null;
    }
}
