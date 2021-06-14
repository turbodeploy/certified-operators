package com.vmturbo.topology.processor.migration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.common.Migration.MigrationProgressInfo;
import com.vmturbo.common.protobuf.common.Migration.MigrationStatus;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.components.common.migration.AbstractMigration;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.db.tables.AssignedIdentity;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.PropertyDescriptor;
import com.vmturbo.topology.processor.identity.extractor.PropertyDescriptorImpl;
import com.vmturbo.topology.processor.identity.services.PolicyMatcherDefault;
import com.vmturbo.topology.processor.identity.storage.EntityInMemoryProxyDescriptor;
import com.vmturbo.topology.processor.probes.ProbeException;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Migrate the probeInfo for VCenter Probes due to addition of new fields to the
 * Identity Metadata properties. Also migrate the entity records in TP for all entities
 * discovered by the vCenter probes.
 * https://git.turbonomic.com/turbonomic/opsmgr/-/merge_requests/2636
 *
 * <p>Before:
 *   VIRTUAL_VOLUME:
 *     nonVolatileProperties:
 *       - id
 *
 * <p>After:
 *   VIRTUAL_VOLUME:
 *     nonVolatileProperties:
 *       - id
 *     volatileProperties:
 *       - entityPropertiesList(name,vmLocalName)/value*
 *     heuristicProperties:
 *       - entityPropertiesList(name,targetAddress)/value*
 *     heuristicThreshold: 100
 */
public class V_01_01_09__VV_Metadata_Change_Migration extends AbstractMigration {

    private final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;

    private final DSLContext dslContext;

    private final IdentityProvider identityProvider;

    private static final String NEW_VOLATILE_PROPERTY =
        "entityPropertiesList(name,vmLocalName)/value*";

    private static final String NEW_HEURISTIC_PROPERTY =
        "entityPropertiesList(name,targetAddress)/value*";

    private static final int NEW_HEURISTIC_THRESHOLD = 100;

    private static final int DB_RECORD_BATCH_INSERT_SIZE = 5000;

    private static final String NO_VCENTER_PROBE_MSG = "No vCenter probe to upgrade. Upgrade finished.";

    /**
     * Constructor.
     *
     * @param probeStore a probe store
     * @param dslContext a DSLContext with which to interact with an underlying persistent datastore
     * @param identityProvider the Identity Provider responsible for management of OID's
     */
    V_01_01_09__VV_Metadata_Change_Migration(@Nonnull ProbeStore probeStore,
                                             @Nonnull DSLContext dslContext,
                                             @Nonnull IdentityProvider identityProvider) {
        this.probeStore = Objects.requireNonNull(probeStore);
        this.dslContext = Objects.requireNonNull(dslContext);
        this.identityProvider = Objects.requireNonNull(identityProvider);
    }

    @Override
    protected MigrationProgressInfo doStartMigration() {
        logger.info("Starting migration of volatile and heuristic properties of virtual volumes of vCenter.");

        // Force initialization of the probe store, so the non-migrated
        // data is loaded from Consul into its local state.
        try {
            probeStore.initialize();
        } catch (InitializationException e) {
            String msg = "Failed to initialize probe store with error: " + e.getMessage();
            logger.error("{}", msg, e);
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        }

        final Optional<Long> probeId = probeStore.getProbeIdForType(SDKProbeType.VCENTER.getProbeType());
        if (!probeId.isPresent()) {
            logger.info(NO_VCENTER_PROBE_MSG);
            return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, NO_VCENTER_PROBE_MSG);
        }

        final Optional<ProbeInfo> oldProbeInfo = probeStore.getProbe(probeId.get());
        if (!oldProbeInfo.isPresent()) {
            logger.info(NO_VCENTER_PROBE_MSG);
            return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100, NO_VCENTER_PROBE_MSG);
        }

        // Create new ProbeInfo and new EntityIdentityMetadata.
        final ProbeInfo.Builder newProbeInfo = oldProbeInfo.get().toBuilder();
        final Optional<EntityIdentityMetadata.Builder> newVVIdentityMetadataOptional = newProbeInfo.getEntityMetadataBuilderList()
            .stream().filter(metadata -> metadata.getEntityType() == EntityType.VIRTUAL_VOLUME).findFirst();

        // If there is no VV EntityMetadata, fail.
        if (!newVVIdentityMetadataOptional.isPresent()) {
            String msg = "Missing Virtual Volume EntityMetadata for vCenter probe";
            logger.error(msg);
            return updateMigrationProgress(MigrationStatus.FAILED, 0, msg);
        }

        final EntityIdentityMetadata.Builder newVVIdentityMetadata = newVVIdentityMetadataOptional.get();

        final boolean heuristicAlreadyExists =
            newVVIdentityMetadata.getHeuristicPropertiesList().stream()
                .anyMatch(p -> p.getName().equals(NEW_HEURISTIC_PROPERTY));
        if (!heuristicAlreadyExists) {
            // The order of the heuristic properties in the identity-metadata.yml is
            // different from what is sent by the probe. The new heuristic property is
            // the first in the heuristic properties list in the ProbeInfo sent by the
            // probe. So it's added at the start of the list here.
            newVVIdentityMetadata.addHeuristicProperties(0,
                PropertyMetadata.newBuilder().setName(NEW_HEURISTIC_PROPERTY).build());
            newVVIdentityMetadata.setHeuristicThreshold(NEW_HEURISTIC_THRESHOLD);
        }

        final boolean volatileAlreadyExists =
            newVVIdentityMetadata.getVolatilePropertiesList().stream()
                .anyMatch(p -> p.getName().equals(NEW_VOLATILE_PROPERTY));
        if (!volatileAlreadyExists) {
            newVVIdentityMetadata.addVolatileProperties(0,
                PropertyMetadata.newBuilder().setName(NEW_VOLATILE_PROPERTY).build());
        }

        if (!heuristicAlreadyExists || !volatileAlreadyExists) {
            logger.info("Updating to the new probeInfo {} for probeId: {}.", newProbeInfo, probeId);
            try {
                probeStore.updateProbeInfo(newProbeInfo.build());
            } catch (ProbeException pe) {
                String msg = "Error while persisting new probeInfo {}.";
                logger.error(msg, newProbeInfo, pe);
                return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
            }
        }

        updateMigrationProgress(MigrationStatus.RUNNING, 50,
            "Successfully updated the probeInfo. Starting migration of virtual volume entities.");

        // Now start the migration of the heuristicProperties.
        final Map<Long, EntityInMemoryProxyDescriptor> entitiesToMigrate;
        try {
            entitiesToMigrate = fetchEntitiesToMigrate(probeId.get());
        } catch (DataAccessException ex) {
            String msg = "Error fetching virtual volume entities to migrate.";
            logger.error(msg, ex);
            return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
        }

        logger.info("Migrating vCenter {} virtual volume entities with the new volatile and heuristic properties.", entitiesToMigrate.size());
        try {
            updateEntityHeuristicProperties(entitiesToMigrate);
        } catch (DataAccessException ex) {
            String msg = "Error migrating vCenter virtual volume entities with the new volatile and heuristic properties.";
            logger.error(msg, ex);
            return updateMigrationProgress(MigrationStatus.FAILED, 50, msg);
        }

        identityProvider.updateProbeInfo(newProbeInfo.build());
        return updateMigrationProgress(MigrationStatus.SUCCEEDED, 100,
            "Successfully migrated the probeInfo and"
                + " corresponding virtual volumes to use the new volatile and heuristic properties.");
    }

    /**
     * Fetch the records whose identity properties has to be updated.
     *
     * @param probeId probeId
     * @return Map of the entityId -> EntityInMemoryProxyDescriptor
     * @throws DataAccessException an exception may be thrown
     */
    private Map<Long, EntityInMemoryProxyDescriptor> fetchEntitiesToMigrate(long probeId) throws DataAccessException {
        return dslContext
            .select(AssignedIdentity.ASSIGNED_IDENTITY.ID, AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES)
            .from(AssignedIdentity.ASSIGNED_IDENTITY)
            .where(AssignedIdentity.ASSIGNED_IDENTITY.PROBE_ID.eq(probeId))
                .and(AssignedIdentity.ASSIGNED_IDENTITY.ENTITY_TYPE.eq(EntityType.VIRTUAL_VOLUME_VALUE))
                .and(AssignedIdentity.ASSIGNED_IDENTITY.EXPIRED.isFalse())
            .fetch()
            .intoMap(AssignedIdentity.ASSIGNED_IDENTITY.ID, AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES);
    }

    /**
     * Update the entities in the assigned_identity table with the newly added volatile and heuristic properties.
     *
     * @param entitiesToMigrate Map of EntityId -> IdentifyingProperties
     * @throws DataAccessException an exception may be thrown
     */
    private void updateEntityHeuristicProperties(
        @Nonnull Map<Long, EntityInMemoryProxyDescriptor> entitiesToMigrate) throws DataAccessException {
        for (List<Entry<Long, EntityInMemoryProxyDescriptor>> entityRecordsBatch
            : Iterables.partition(entitiesToMigrate.entrySet(), DB_RECORD_BATCH_INSERT_SIZE)) {

            dslContext.transaction(configuration -> {
                final DSLContext transactionContext = DSL.using(configuration);
                // Create a batch statement to execute a set of update queries in batch mode for better performance.
                // Need to first initial batch statement and then add bind values to it.
                final BatchBindStep batch =
                    transactionContext.batch(
                        transactionContext.update(AssignedIdentity.ASSIGNED_IDENTITY)
                            // use the 1st value as dummy value as jooq requires non-null value.
                            // will receive exception java.lang.IllegalArgumentException: argument "content" is null if null is used.
                            .set(AssignedIdentity.ASSIGNED_IDENTITY.PROPERTIES, entityRecordsBatch.get(0).getValue())
                            .where(AssignedIdentity.ASSIGNED_IDENTITY.ID.eq(0L)));

                entityRecordsBatch.forEach(entry -> {
                    final long entityId = entry.getKey();
                    final EntityInMemoryProxyDescriptor oldProxyDescriptor = entry.getValue();

                    // There should be only 2 existing identifying properties
                    // "1:virtual volume uuid"
                    // "2:VIRTUAL_VOLUME"
                    final List<PropertyDescriptor> newIdentifyingProps;
                    final boolean identifyingAlreadyUpdated = oldProxyDescriptor.getIdentifyingProperties().size() != 2;
                    if (identifyingAlreadyUpdated) {
                        newIdentifyingProps = oldProxyDescriptor.getIdentifyingProperties();
                    } else {
                        newIdentifyingProps = new ArrayList<>(oldProxyDescriptor.getIdentifyingProperties());
                        newIdentifyingProps.add(new PropertyDescriptorImpl(PolicyMatcherDefault.DUMMY_VALUE, 3));
                    }

                    // There shouldn't be any existing heuristic properties
                    final List<PropertyDescriptor> newHeuristicProps;
                    final boolean heuristicAlreadyUpdated = !oldProxyDescriptor.getHeuristicProperties().isEmpty();
                    if (heuristicAlreadyUpdated) {
                        newHeuristicProps = oldProxyDescriptor.getHeuristicProperties();
                    } else {
                        newHeuristicProps =
                            Collections.singletonList(new PropertyDescriptorImpl(PolicyMatcherDefault.DUMMY_VALUE, 4));
                    }

                    if (identifyingAlreadyUpdated && heuristicAlreadyUpdated) {
                        return;
                    }

                    final EntityInMemoryProxyDescriptor newProxyDescriptor =
                        new EntityInMemoryProxyDescriptor(oldProxyDescriptor.getOID(),
                            newIdentifyingProps,
                            newHeuristicProps);

                    batch.bind(newProxyDescriptor, entityId);
                });

                batch.execute();
            });
        }
    }
}
