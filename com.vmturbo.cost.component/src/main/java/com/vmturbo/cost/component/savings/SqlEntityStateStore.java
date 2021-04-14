package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_STATE;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.cost.component.db.tables.records.EntitySavingsStateRecord;
import com.vmturbo.cost.component.entity.scope.SQLCloudScopedStore;
import com.vmturbo.group.api.GroupAndMembers;

/**
 * Implementation of EntityStateStore that persists data in entity_savings_state table.
 */
public class SqlEntityStateStore extends SQLCloudScopedStore implements EntityStateStore {

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
     * @return a map of entity_oid -> EntityState that must be processed.
     * @throws EntitySavingsException when an error occurs
     */
    @Override
    public Map<Long, EntityState> getForcedUpdateEntityStates(LocalDateTime timestamp)
            throws EntitySavingsException {
        final Result<Record2<Long, String>> records;
        try {
            records = dsl.select(ENTITY_SAVINGS_STATE.ENTITY_OID, ENTITY_SAVINGS_STATE.ENTITY_STATE)
                    .from(ENTITY_SAVINGS_STATE)
                    .where(ENTITY_SAVINGS_STATE.UPDATED.eq((byte)1)
                            .or(ENTITY_SAVINGS_STATE.NEXT_EXPIRATION_TIME.le(timestamp)))
                    .fetch();
            return records.stream().collect(Collectors.toMap(Record2::value1,
                    record -> EntityState.fromJson(record.value2())));
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when getting entity states from database.", e);
        }
    }

    @Override
    public void clearUpdatedFlags() throws EntitySavingsException {
        try {
            dsl.update(ENTITY_SAVINGS_STATE)
                    .set(ENTITY_SAVINGS_STATE.UPDATED, (byte)0)
                    .where(ENTITY_SAVINGS_STATE.UPDATED.eq((byte)1))
                    .execute();
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when getting entity states from database.", e);
        }
    }

    @Override
    public void deleteEntityStates(@Nonnull final Set<Long> entityIds) throws EntitySavingsException {
        try {
            dsl.deleteFrom(ENTITY_SAVINGS_STATE)
                    .where(ENTITY_SAVINGS_STATE.ENTITY_OID.in(entityIds))
                    .execute();
        } catch (DataAccessException e) {
            throw new EntitySavingsException("Error occurred when deleting entity states.", e);
        }
    }

    @Override
    public void updateEntityStates(@Nonnull final Map<Long, EntityState> entityStateMap,
                                   @Nonnull final TopologyEntityCloudTopology cloudTopology)
            throws EntitySavingsException {
        List<EntitySavingsStateRecord> records = new ArrayList<>();
        entityStateMap.values().forEach(entityState -> {
            EntitySavingsStateRecord record = ENTITY_SAVINGS_STATE.newRecord();
            record.setEntityOid(entityState.getEntityId());
            record.setUpdated(entityState.isUpdated() ? (byte)1 : (byte)0);
            record.setNextExpirationTime(Timestamp.from(Instant.ofEpochMilli(entityState
                    .getNextExpirationTime())).toLocalDateTime());
            record.setEntityState(entityState.toJson());
            records.add(record);
        });

        List<EntityCloudScopeRecord> scopeRecords = entityStateMap.keySet().stream()
                .map(entityOid -> createCloudScopeRecord(entityOid, cloudTopology))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        try {
            insertCloudScopeRecords(scopeRecords);
        } catch (IOException e) {
            throw new EntitySavingsException("Error occurred when writing to entity_cloud_scope table.", e);
        }

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
    public Stream<EntityState> getAllEntityStates() {
        // Use jooq lazy fetch to avoid load the whole table into memory.
        // https://www.jooq.org/doc/3.2/manual/sql-execution/fetching/lazy-fetching/
        // https://stackoverflow.com/questions/32209248/java-util-stream-with-resultset
        return dsl.selectFrom(ENTITY_SAVINGS_STATE)
                .fetchSize(chunkSize)
                .fetchStream()
                .map(record -> EntityState.fromJson(record.getEntityState()));
    }

    private EntityCloudScopeRecord createCloudScopeRecord(Long entityOid, TopologyEntityCloudTopology cloudTopology) {

        final Integer entityType = cloudTopology.getEntity(entityOid).map(TopologyEntityDTO::getEntityType).orElse(null);

        // Get the service provider OID.
        Optional<TopologyEntityDTO> serviceProvider = cloudTopology.getServiceProvider(entityOid);
        final Long serviceProviderOid = serviceProvider.map(TopologyEntityDTO::getOid).orElse(null);

        // Get the region OID.
        Optional<TopologyEntityDTO> region = cloudTopology.getConnectedRegion(entityOid);
        final Long regionOid = region.map(TopologyEntityDTO::getOid).orElse(null);

        // Get the availability zone OID.
        Optional<TopologyEntityDTO> availabilityZone = cloudTopology.getConnectedAvailabilityZone(entityOid);
        final Optional<Long> availabilityZoneOid = availabilityZone.map(TopologyEntityDTO::getOid);

        // Get the account OID.
        Optional<TopologyEntityDTO> businessAccount = cloudTopology.getOwner(entityOid);
        final Long accountOid = businessAccount.map(TopologyEntityDTO::getOid).orElse(null);

        // Get the resource group OID.
        Optional<GroupAndMembers> resourceGroup = cloudTopology.getResourceGroup(entityOid);
        final Optional<Long> resourceGroupOid = resourceGroup.map(groupAndMembers -> groupAndMembers.group().getId());

        if (entityType != null && serviceProviderOid != null && regionOid != null && accountOid != null) {
            return createCloudScopeRecord(entityOid, entityType, accountOid, regionOid,
                    availabilityZoneOid, serviceProviderOid, resourceGroupOid, LocalDateTime.now());
        }

        logger.error("Cannot create entity cloud scope record because required information is missing."
                        + " EntityOid={}, EntityType={}, serviceProviderOid={}, regionOid={}, accountOid={}",
                entityOid, entityType, serviceProviderOid, regionOid, accountOid);
        return null;
    }
}
