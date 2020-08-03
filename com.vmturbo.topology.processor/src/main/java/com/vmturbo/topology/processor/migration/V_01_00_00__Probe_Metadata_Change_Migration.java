package com.vmturbo.topology.processor.migration;

import static com.vmturbo.topology.processor.db.tables.AssignedIdentity.ASSIGNED_IDENTITY;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;


import com.google.common.collect.Iterables;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityIdToEntityTypeMappingRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.common.RequiresDataInitialization;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.services.IdentityServiceUnderlyingStore;
import com.vmturbo.topology.processor.identity.storage.EntityInMemoryProxyDescriptor;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 *  Migrate the probeInfo for VCenter Probes due to addition of new fields to the
 *  Identity Metadata properties. Also migrate the entity records in TP for all entities
 *  discovered by the vCenter probes.
 *  https://vmturbo.atlassian.net/browse/OM-36485
 *
 * Before:
 *  VIRTUAL_MACHINE:
 *   nonVolatileProperties:
 *     - id
 *   volatileProperties:
 *     - entityPropertiesList(name,LocalName)/value*
 *   heuristicProperties:
 *     - entityPropertiesList(name,instanceUuid)/value*
 *     - entityPropertiesList(name,vmHardwareCompatibilityVersion)/value*
 *     - virtualMachineData/guestName
 *
 *  After:
 *  VIRTUAL_MACHINE:
 *   nonVolatileProperties:
 *     - id
 *   volatileProperties:
 *     - entityPropertiesList(name,LocalName)/value*
 *   heuristicProperties:
 *     - entityPropertiesList(name,instanceUuid)/value*
 *     - entityPropertiesList(name,vmHardwareCompatibilityVersion)/value*
 *     - virtualMachineData/guestName
 *     - entityPropertiesList(name,targetAddress)/value*
 *   heuristicThreshold: 100
 *
 */
public class V_01_00_00__Probe_Metadata_Change_Migration extends AbstractMigration {

    private final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;

    private final DSLContext dslContext;

    private final StatsHistoryServiceBlockingStub statsClient;

    private final IdentityServiceUnderlyingStore identityInMemoryStore;

    private final IdentityProvider identityProvider;

    private static final String NEW_HEURISTIC_PROPERTY =
            "entityPropertiesList(name,targetAddress)/value*";

    private static final int NEW_HEURISTIC_THRESHOLD = 100;

    private static final String NEW_HEURISTIC_PROPERTY_DUMMY_VALUE = "DUMMY_VALUE";

    private static final int DB_RECORD_BATCH_INSERT_SIZE = 5000;

    public V_01_00_00__Probe_Metadata_Change_Migration(@Nonnull ProbeStore probeStore,
                                                       @Nonnull DSLContext dslContext,
                                                       @Nonnull StatsHistoryServiceBlockingStub statsClient,
                                                       @Nonnull IdentityServiceUnderlyingStore identityInMemoryStore,
                                                       @Nonnull IdentityProvider identityProvider) {

        this.probeStore = Objects.requireNonNull(probeStore);
        this.dslContext = Objects.requireNonNull(dslContext);
        this.statsClient = Objects.requireNonNull(statsClient);
        this.identityInMemoryStore = Objects.requireNonNull(identityInMemoryStore);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    @Override
    public MigrationProgressInfo doStartMigration() {
        // 1. Update the probe metadata with the new heuristic properties.
        // 2. As we currently don't store entityType, we can't decide which
        //    entity to update. So we add a new entity_type column to the
        //    the assigned_identity table and then populate it by fetching
        //    the entityTypes from history component.
        // 3. For those entities in the assigned_identity table for which we
        //    couldn't find the entityType in history component, we delete them.
        // 4. Update the assigned_identity table with the entityType for each entity.
        // 5. Now filter all the entities which match this probeID and have
        //    entityType==VM.
        // 6. For each of this VM entity, add the new heuristicProperty fields to its
        //    properties record.

        logger.info("Starting migration of heuristic properties.");

        // Force initialization of the probe store, so the non-migrated
        // data is loaded from Consul into its local state.
        if (probeStore instanceof RequiresDataInitialization) {
            try {
                ((RequiresDataInitialization)probeStore).initialize();
            } catch (InitializationException e) {
                String msg = "Failed to initialize probe store with error: " + e.getMessage();
                logger.error("{}", msg, e);
                return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
            }
        }

        Optional<Long> probeId =
                probeStore.getProbeIdForType(SDKProbeType.VCENTER.getProbeType());
        if (!probeId.isPresent()) {
            String msg ="No vCenter probe to upgrade. Upgrade finished.";
            logger.info(msg);
            return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, msg);
        }

        Optional<ProbeInfo> oldProbeInfo =
                probeStore.getProbe(probeId.get());
        OptionalInt vmIdentityMetadataIndex =
                IntStream.range(0, oldProbeInfo.get().getEntityMetadataCount())
                        .filter(i ->
                                oldProbeInfo.get().getEntityMetadata(i).getEntityType() ==
                                        EntityType.VIRTUAL_MACHINE)
                        .findFirst();

        // If there is no VM EntityMetadata, fail.
        if (!vmIdentityMetadataIndex.isPresent()) {
            String msg = "Missing VM EntityMetadata for vCenter probe";
            logger.error(msg);
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        }

        EntityIdentityMetadata.Builder newVmIdentityMetadata =
                oldProbeInfo.get().getEntityMetadata(
                        vmIdentityMetadataIndex.getAsInt()).toBuilder();

        List<PropertyMetadata> heuristicProperties =
                new ArrayList<>(newVmIdentityMetadata.getHeuristicPropertiesList());

        boolean heuristicAlreadyExists =
                heuristicProperties.stream()
                        .anyMatch(p -> p.getName().equals(NEW_HEURISTIC_PROPERTY));

        ProbeInfo.Builder newProbeInfo = oldProbeInfo.get().toBuilder();
        if (!heuristicAlreadyExists) {
            // The order of the heuristic properties in the identity-metadata.yml is
            // different from what is sent by the probe. The new heuristic property is
            // the first in the heuristic properties list in the ProbeInfo sent by the
            // probe. So it's added at the start of the list here.
            newVmIdentityMetadata.addHeuristicProperties(0,
                    PropertyMetadata.newBuilder().
                            setName(NEW_HEURISTIC_PROPERTY)
                            .build());

            newVmIdentityMetadata.setHeuristicThreshold(NEW_HEURISTIC_THRESHOLD);
            newProbeInfo.setEntityMetadata(vmIdentityMetadataIndex.getAsInt(),
                    newVmIdentityMetadata);

            logger.info("Updating to the new probeInfo {} for probeId: {}",
                    newProbeInfo, probeId);
            try {
                probeStore.updateProbeInfo(newProbeInfo.build());
            } catch (ProbeException pe) {
                String msg = "Error while persisting new probeInfo {}";
                logger.error(msg, newProbeInfo, pe);
                return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
            }

        }
        updateMigrationProgress(MigrationStatus.RUNNING, 50,
            "Successfully updated the probeInfo. Starting migration of entities");

        // 4. Now update the VM entities which are discovered by this vCenter probe.
        // The EntityId->EntityType mapping is fetched from history and is used to set the EntityType for the entities
        // in the assigned_identity table. Any entity for which there is no EntityType mapping, will be dropped from the
        // assigned_identity table. Since topology flows from TP -> History, if there is a delay, some entities may show
        // up in History component which are no longer in the TP database. But these stale id's should be ok as their
        // stats would be eventually purged after the stats retention period.
        // The stale entries could be prevented by purging the kafka topology queue. But the additional complexity of
        // this solution is deemed unnecessary.
        Set<Long> tpEntityIds;
        try {
            tpEntityIds = getEntityIdsWithNoEntityType();
        } catch (DataAccessException ex) {
            String msg = "Error getting entities with no entityType from DB";
            logger.error(msg, ex);
            return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
        }

        Map<Long, EntityType> entityIdToEntityTypeMap = new HashMap<>();
        if (tpEntityIds.size() > 0) {
            // Only query from history component if there are entities for which the
            // entityType is not set.
            // As we are not storing the entityType in the identityMetadata, we have to first
            // fetch the entityId -> entityType mapping from history component.
            // Fetching everything in one shot is ok. If there are 100,000 entities, the size
            // of this map will be about 9MB(100000.0*(64+32))/pow(2,20))
            try {
                entityIdToEntityTypeMap = statsClient.getEntityIdToEntityTypeMapping(
                        GetEntityIdToEntityTypeMappingRequest.getDefaultInstance())
                        .getEntityIdToEntityTypeMapMap();
            } catch (DataAccessException ex) {
                String msg = "Error getting entities with no entityType from DB";
                logger.error(msg, ex);
                return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
            }
        }

        // Only update the entities for which we know the entityType.
        tpEntityIds.retainAll(entityIdToEntityTypeMap.keySet());
        logger.info("Setting the correct entityTypes for the entities in the DB. Total records: {} ",
                tpEntityIds.size());
        try {
            updateEntityTypeInDB(tpEntityIds, entityIdToEntityTypeMap);
        } catch (DataAccessException ex) {
            String msg = "Failed to update the entity types in DB.";
            logger.error(msg, ex);
            return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
        }

        // TODO: before deleting the entities, query and log these entities.
        logger.info("Deleting entities for whom no entityTypes could be resolved.");
        try {
            deleteEntitiesWithInvalidEntityType();
        } catch (DataAccessException ex) {
            String msg = "Error deleting entries with no associated entityTypes.";
            logger.error(msg, ex);
            return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
        }

        //6. Now start the migration of the heuristicProperties.
        Map<Long, EntityInMemoryProxyDescriptor> entitiesToMigrate;
        try {
            entitiesToMigrate = fetchEntitiesToMigrate(probeId.get());
        } catch (DataAccessException ex) {
            String msg = "Error fetching entities to migrate";
            logger.error(msg, ex);
            return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
        }

        logger.info("Migrating vCenter VM entities with the new heuristic properties.");
        try {
            updateEntityHeuristicProperties(entitiesToMigrate);
        } catch (DataAccessException ex) {
            String msg = "Error inserting entityType for entityId";
            logger.error(msg, ex);
            return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
        }

        // Reload the IdentityMetadataInMemory store so that it picks up the updated entries
        // with the dummy-values.
        identityProvider.updateProbeInfo(newProbeInfo.build());
        identityInMemoryStore.reloadEntityDescriptors();
        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100,
                "Successfully migrated the probeInfo and the" +
                        " corresponding entities to use the new heuristic properties.");
    }

    /**
     * Return the EntityIds of the entities in the assigned_identity table
     * which don't have a valid entity type.
     */
    private Set<Long> getEntityIdsWithNoEntityType() throws DataAccessException {
        return dslContext
                .select(ASSIGNED_IDENTITY.ID)
                .from(ASSIGNED_IDENTITY)
                .where(ASSIGNED_IDENTITY.ENTITY_TYPE.eq(Integer.MAX_VALUE))
                .fetchStream()
                .map(rec -> rec.value1())
                .collect(Collectors.toSet());
    }

    /**
     * Remove all the entities for which there is no valid entityType.
     */
    private void deleteEntitiesWithInvalidEntityType() throws DataAccessException {
        dslContext
                .deleteFrom(ASSIGNED_IDENTITY)
                .where(ASSIGNED_IDENTITY.ENTITY_TYPE.eq(Integer.MAX_VALUE))
                .execute();
    }

    /**
     * Update the entities with the correct entityIds.
     */
    private void updateEntityTypeInDB(@Nonnull Set<Long> entityIds,
                                      @Nonnull Map<Long, EntityType> entityIdToEntityTypeMap)
            throws DataAccessException {

        for (List<Long> entityIdBatch :
                Iterables.partition(entityIds, DB_RECORD_BATCH_INSERT_SIZE)) {

            dslContext.transaction(configuration -> {
                try {
                    final DSLContext transactionContext = DSL.using(configuration);
                    // Initialize the batch.
                    final BatchBindStep batch =
                            transactionContext.batch(
                                    transactionContext.update(ASSIGNED_IDENTITY)
                                            .set(ASSIGNED_IDENTITY.ENTITY_TYPE, 0)
                                            .where(ASSIGNED_IDENTITY.ID.eq(0L)));

                    entityIdBatch.forEach(entityId -> {
                        batch.bind(entityIdToEntityTypeMap.get(entityId).getNumber(), entityId);
                    });

                    batch.execute();
                } catch (DataAccessException ex) {
                    throw ex;
                }
            });
        }
    }

    /**
     * Fetch the records whose heuristic identity properties has to be updated.
     *
     * @param probeId
     * @return Map of the entityId -> EntityInMemoryProxyDescriptor
     * @throws DataAccessException
     */
    private Map<Long, EntityInMemoryProxyDescriptor> fetchEntitiesToMigrate(long probeId)
            throws DataAccessException {

        return dslContext
                .select(ASSIGNED_IDENTITY.ID, ASSIGNED_IDENTITY.PROPERTIES)
                .from(ASSIGNED_IDENTITY)
                .where(ASSIGNED_IDENTITY.PROBE_ID.eq(probeId)).and(
                ASSIGNED_IDENTITY.ENTITY_TYPE.eq(EntityType.VIRTUAL_MACHINE_VALUE))
                .fetch()
                .intoMap(ASSIGNED_IDENTITY.ID, ASSIGNED_IDENTITY.PROPERTIES);
    }

    /**
     *  Update the entities in the assigned_identity table with the newly added
     *  heuristic properties.
     *
     * @param entitiesToMigrate Map of EntityId -> IdentifyingProperties
     * @throws DataAccessException
     */
    private void updateEntityHeuristicProperties(
            @Nonnull Map<Long, EntityInMemoryProxyDescriptor> entitiesToMigrate)
            throws DataAccessException {

        for (List<Entry<Long, EntityInMemoryProxyDescriptor>> entityRecordsBatch :
                Iterables.partition(entitiesToMigrate.entrySet(), DB_RECORD_BATCH_INSERT_SIZE)) {

            dslContext.transaction(configuration -> {
                try {
                    final DSLContext transactionContext = DSL.using(configuration);
                    // Initialize the batch.
                    final BatchBindStep batch =
                            transactionContext.batch(
                                    transactionContext.update(ASSIGNED_IDENTITY)
                                            // use the 1st value as dummy value as jooq requires non-null value.
                                            .set(ASSIGNED_IDENTITY.PROPERTIES, entityRecordsBatch.get(0).getValue())
                                            .where(ASSIGNED_IDENTITY.ID.eq(0L)));

                    entityRecordsBatch.forEach(entry -> {
                        long entityId = entry.getKey();
                        EntityInMemoryProxyDescriptor oldProxyDescriptor = entry.getValue();
                        List<PropertyDescriptor> oldHeuristicProps =
                                new ArrayList<>(oldProxyDescriptor.getHeuristicProperties());
                        boolean alreadyUpdated = (oldHeuristicProps.size() == 4);
                        if (alreadyUpdated) {
                            logger.info("Already migrated heuristic properties for entity {}", entityId);
                            return;
                        }

                        // The heuristic properties for an entity are persisted in sorted order. So we need to
                        // re-arrange the existing records so that the dummy value for the new property is
                        // injected in the right sort position and the ranks have to be re-assigned.
                        // Existing sort order:
                        //      1) entityPropertiesList(name,instanceUuid)/value*
                        //      2) entityPropertiesList(name,vmHardwareCompatibilityVersion)/value*
                        //      3) virtualMachineData/guestName
                        // Order after new property {entityPropertiesList(name,targetAddress)/value*)} is added:
                        //      1) entityPropertiesList(name,instanceUuid)/value*
                        //      2) entityPropertiesList(name,targetAddress)/value*
                        //      3) entityPropertiesList(name,vmHardwareCompatibilityVersion)/value*
                        //      4) virtualMachineData/guestName
                        List<PropertyDescriptor> newHeuristicProps = new ArrayList<>();
                        int propertyRank = oldHeuristicProps.get(0).getPropertyTypeRank();
                        PropertyDescriptor propertyDescriptor =
                                new PropertyDescriptorImpl(oldHeuristicProps.get(0).getValue(), propertyRank);
                        newHeuristicProps.add(propertyDescriptor);
                        propertyRank += 1;
                        propertyDescriptor =
                                new PropertyDescriptorImpl(NEW_HEURISTIC_PROPERTY_DUMMY_VALUE, propertyRank);
                        newHeuristicProps.add(propertyDescriptor);

                        propertyRank += 1;
                        propertyDescriptor =
                                new PropertyDescriptorImpl(oldHeuristicProps.get(1).getValue(), propertyRank);
                        newHeuristicProps.add(propertyDescriptor);
                        propertyRank += 1;
                        propertyDescriptor =
                                new PropertyDescriptorImpl(oldHeuristicProps.get(2).getValue(), propertyRank);
                        newHeuristicProps.add(propertyDescriptor);

                        EntityInMemoryProxyDescriptor newProxyDescriptor =
                                new EntityInMemoryProxyDescriptor(oldProxyDescriptor.getOID(),
                                        (List)oldProxyDescriptor.getIdentifyingProperties(),
                                        newHeuristicProps);


                        batch.bind(newProxyDescriptor, entityId);
                    });

                    batch.execute();
                } catch (DataAccessException ex) {
                    throw ex;
                }
            });

        }
    }

}
