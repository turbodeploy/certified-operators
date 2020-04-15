package com.vmturbo.clustermgr.management;

import static com.vmturbo.common.protobuf.cluster.ComponentStatusProtoUtil.getComponentLogId;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.UpdateSetMoreStep;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.clustermgr.db.Tables;
import com.vmturbo.clustermgr.db.tables.records.RegisteredComponentRecord;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentIdentifier;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentInfo;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStarting;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.UriInfo;

/**
 * Registry for component information. Stores data in MySQL.
 */
public class ComponentRegistry {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Database access context.
     */
    private final DSLContext dsl;

    private final Clock clock;

    private final long unhealthyDeregistrationSec;

    ComponentRegistry(@Nonnull final DSLContext dsl,
                      @Nonnull final Clock clock,
                      final long unhealthyDeregistrationTime,
                      @Nonnull final TimeUnit unhealthyDeregistrationTimeUnit) {
        this.dsl = dsl;
        this.clock = clock;
        this.unhealthyDeregistrationSec = unhealthyDeregistrationTimeUnit.toSeconds(unhealthyDeregistrationTime);
    }

    /**
     * Register a component with the registry.
     *
     * @param componentStartInfo Information about the starting component.
     * @return The registered component's information (should be the same as the input).
     * @throws RegistryUpdateException If there is an error interacting with the database.
     */
    @Nonnull
    public ComponentInfo registerComponent(@Nonnull final ComponentStarting componentStartInfo) throws RegistryUpdateException {
        final RegisteredComponentRecord record = protoToRecord(componentStartInfo);
        try {
            return dsl.transactionResult(transactionContext -> {
                final ComponentInfo componentInfo = componentStartInfo.getComponentInfo();
                final DSLContext transactionDsl = DSL.using(transactionContext);
                // We may have another component with the same type and instance id, but a different
                // JVM id (e.g. if a pod is restarting and retains its instance ID, but the restart
                // interleaves with the shutdown).
                final int numDeleted = transactionDsl.deleteFrom(Tables.REGISTERED_COMPONENT)
                    .where(Tables.REGISTERED_COMPONENT.COMPONENT_TYPE.eq(record.getComponentType()))
                        .and(Tables.REGISTERED_COMPONENT.INSTANCE_ID.eq(record.getInstanceId()))
                    .execute();
                if (numDeleted > 0) {
                    logger.info("Deleted {} existing service registrations with component type {} and instance {}",
                        numDeleted, record.getComponentType(), record.getInstanceId());
                }
                final int numInserted = transactionDsl.insertInto(Tables.REGISTERED_COMPONENT)
                    .set(record)
                    .execute();
                if (numInserted < 1) {
                    logger.warn("Failed to insert component info for component {}",
                        getComponentLogId(componentInfo.getId()));
                } else {
                    logger.info("Registered component {}", getComponentLogId(componentInfo.getId()));
                }
                return componentInfo;
            });
        } catch (DataAccessException e) {
            throw new RegistryUpdateException(e);
        }
    }

    /**
     * De-register a component, removing it from the registry.
     * @param componentId The identifier for the component.
     * @return True if a component with this identifier existed, and is now removed.
     * @throws RegistryUpdateException If there is an error interacting with the database.
     */
    public boolean deregisterComponent(@Nonnull final ComponentIdentifier componentId)
            throws RegistryUpdateException {
        try {
            final int numDeleted = dsl.deleteFrom(Tables.REGISTERED_COMPONENT)
                .where(getComponentIdConditions(componentId))
                .execute();
            if (numDeleted > 0) {
                logger.info("Deleted registered component {}", getComponentLogId(componentId));
            }
            return numDeleted > 0;
        } catch (DataAccessException e) {
            throw new RegistryUpdateException(e);
        }
    }

    private Condition getComponentIdConditions(ComponentIdentifier identifier) {
        return Tables.REGISTERED_COMPONENT.INSTANCE_ID.eq(identifier.getInstanceId())
                .and(Tables.REGISTERED_COMPONENT.JVM_ID.eq(identifier.getJvmId()));
    }

    /**
     * Get the currently registered components.
     *
     * @return The registered components, arranged by component type and instance id.
     */
    @Nonnull
    public Table<String, String, RegisteredComponent> getRegisteredComponents() {
        try {
            final Table<String, String, RegisteredComponent> registeredComponents = HashBasedTable.create();
            dsl.selectFrom(Tables.REGISTERED_COMPONENT)
                .fetch()
                .forEach(record -> {
                    registeredComponents.put(record.getComponentType(), record.getInstanceId(),
                        new RegisteredComponent(recordToProto(record),
                            ComponentHealth.fromNumber(record.getStatus())
                                .orElse(ComponentHealth.UNKNOWN)));
                });
            return registeredComponents;
        } catch (DataAccessException e) {
            logger.warn("Failed to query for registered components.", e);
            return HashBasedTable.create();
        }
    }

    /**
     * Update the status of a registered component. If the target component does not exist,
     * this has no effect.
     *
     * <p/>If a component has been unhealthy for too long (as per the duration provided to the
     * registry's constructor), the component will be deregistered as part of this method.
     *
     * @param componentId The identifier for the component to update.
     * @param newStatus New component status. May be the same as the existing status.
     * @param statusDescription New status description.  May be the same as the existing description.
     * @throws RegistryUpdateException If there is an error interacting with the database.
     */
    public void updateComponentHealthStatus(@Nonnull final ComponentIdentifier componentId,
                                            ComponentHealth newStatus,
                                            String statusDescription) throws RegistryUpdateException {
        try {
            dsl.transaction(transactionContext -> {
                final LocalDateTime curTime = LocalDateTime.now(clock);
                DSLContext transactionDsl = DSL.using(transactionContext);
                RegisteredComponentRecord oldRecord = transactionDsl.selectFrom(Tables.REGISTERED_COMPONENT)
                    .where(getComponentIdConditions(componentId))
                    // There should be at most one.
                    .fetchOne();
                if (oldRecord == null) {
                    // This could happen if the component deregistered during the health check,
                    // but it should almost never happen.
                    logger.warn("Component {} no longer registered. Ignoring health status {} update",
                        getComponentLogId(componentId), newStatus);
                    return;
                }

                final Duration timeAtStatus = Duration.between(oldRecord.getLastStatusChangeTime(), curTime);
                final boolean criticalTooLong = oldRecord.getStatus() == newStatus.getNumber() &&
                    newStatus == ComponentHealth.CRITICAL &&
                    timeAtStatus.getSeconds() >= unhealthyDeregistrationSec;

                if (criticalTooLong) {
                    // Remove the registration if the component has been failing a health check
                    // too long. This probably means it went down without sending a notification.
                    final int deletedRows = transactionDsl.deleteFrom(Tables.REGISTERED_COMPONENT)
                        .where(getComponentIdConditions(componentId))
                        .execute();
                    if (deletedRows > 0) {
                        logger.warn("Deregistered component {} due to failing health check for {}",
                            getComponentLogId(componentId), timeAtStatus);
                    } else {
                        // This shouldn't really happen.
                        logger.warn("Failed to deregister unhealthy component {}",
                            getComponentLogId(componentId));
                    }
                } else if (oldRecord.getStatus() == newStatus.getNumber()) {
                    // The status hasn't changed, but we still update the "update time" and the
                    // description if it changed.
                    final UpdateSetMoreStep<RegisteredComponentRecord> updateStep =
                        transactionDsl.update(Tables.REGISTERED_COMPONENT)
                            .set(Tables.REGISTERED_COMPONENT.LAST_UPDATE_TIME, curTime);
                    if (!oldRecord.getStatusDescription().equals(statusDescription)) {
                        updateStep.set(Tables.REGISTERED_COMPONENT.STATUS_DESCRIPTION, statusDescription);
                    }
                    final int modifiedRows = updateStep.where(getComponentIdConditions(componentId)).execute();
                    if (modifiedRows > 0) {
                        logger.debug("Successfully updated component {} to {}", getComponentLogId(componentId), newStatus);
                    }
                } else {
                    // The status changed. Update the relevant fields.
                    final int modifiedRows = transactionDsl.update(Tables.REGISTERED_COMPONENT)
                        .set(Tables.REGISTERED_COMPONENT.STATUS, newStatus.getNumber())
                        .set(Tables.REGISTERED_COMPONENT.STATUS_DESCRIPTION, statusDescription)
                        .set(Tables.REGISTERED_COMPONENT.LAST_STATUS_CHANGE_TIME, curTime)
                        .set(Tables.REGISTERED_COMPONENT.LAST_UPDATE_TIME, curTime)
                        .where(getComponentIdConditions(componentId))
                        .execute();
                    if (modifiedRows > 0) {
                        logger.debug("Successfully updated component {} to {}", getComponentLogId(componentId), newStatus);
                    }

                    if (newStatus == ComponentHealth.HEALTHY) {
                        // It went from non-healthy to healthy.
                        logger.info("Component {} is now healthy. Old status: {}",
                            getComponentLogId(componentId),
                            ComponentHealth.fromNumber(oldRecord.getStatus()).orElse(ComponentHealth.UNKNOWN));
                    } else {
                        // It went from healthy to non-healthy.
                        logger.warn("Component {} is no longer healthy. New status: {}. Description:\n{}",
                            getComponentLogId(componentId), newStatus, statusDescription);
                    }
                }
            });
        } catch (DataAccessException e) {
            throw new RegistryUpdateException(e);
        }
    }

    @Nonnull
    private ComponentInfo recordToProto(@Nonnull final RegisteredComponentRecord record) {
        return ComponentInfo.newBuilder()
            .setId(ComponentIdentifier.newBuilder()
                .setComponentType(record.getComponentType())
                .setInstanceId(record.getInstanceId())
                .setJvmId(record.getJvmId()))
            .setUriInfo(UriInfo.newBuilder()
                .setIpAddress(record.getIpAddress())
                .setPort(record.getPort())
                .setRoute(record.getRoute()))
            .build();
    }

    @Nonnull
    private RegisteredComponentRecord protoToRecord(@Nonnull final ComponentStarting componentStartInfo) {
        ComponentInfo componentInfo = componentStartInfo.getComponentInfo();

        RegisteredComponentRecord record = new RegisteredComponentRecord();
        record.setComponentType(componentInfo.getId().getComponentType());
        record.setInstanceId(componentInfo.getId().getInstanceId());
        record.setJvmId(componentInfo.getId().getJvmId());

        try {
            record.setAuxiliaryInfo(JsonFormat.printer()
                .print(componentStartInfo.getBuildProperties()));
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }

        record.setIpAddress(componentInfo.getUriInfo().getIpAddress());
        record.setRoute(componentInfo.getUriInfo().getRoute());
        record.setPort(componentInfo.getUriInfo().getPort());

        record.setStatus(ComponentHealth.UNKNOWN.getNumber());
        record.setStatusDescription("No health check performed");
        LocalDateTime curTime = LocalDateTime.now(clock);
        record.setRegistrationTime(curTime);
        record.setLastUpdateTime(curTime);
        record.setLastStatusChangeTime(curTime);
        return record;
    }

    /**
     * Exception thrown when interacting with the database throws exceptions.
     *
     * <p/>Used to encapsulate a {@link DataAccessException}, to ensure that users of the registry
     * handle errors.
     */
    public static class RegistryUpdateException extends Exception {

        /**
         * Create a new instance of the exception.
         *
         * @param cause The cause.
         */
        public RegistryUpdateException(final Throwable cause) {
            super(cause);
        }

        /**
         * Create a new instance of the exception.
         *
         * @param message The message.
         */
        public RegistryUpdateException(final String message) {
            super(message);
        }
    }
}
