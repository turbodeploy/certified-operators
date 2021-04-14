package com.vmturbo.topology.processor.controllable;

import static com.vmturbo.topology.processor.db.tables.EntityMaintenance.ENTITY_MAINTENANCE;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.InsertOnDuplicateSetMoreStep;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.AutomationLevel;
import com.vmturbo.topology.processor.db.tables.records.EntityMaintenanceRecord;
import com.vmturbo.topology.processor.entity.EntitiesWithNewStateListener;

/**
 * Data access object for CRUD ENTITY_MAINTENANCE table.
 * This table has two columns, HOST_ID and EXIT_TIME.
 * 1. When a host enters maintenance mode, we'll insert a record (host_oid, null)
 *    or update EXIT_TIME of existing record to null.
 * 2. When a host exits maintenance mode, we'll update the EXIT_TIME to now only when the existing EXIT_TIME is null.
 *    If EXIT_TIME is not null, it means that host already exists maintenance mode.
 * 3. Every broadcast, we delete expired records and return all hosts that should be marked as controllable false.
 */
public class EntityMaintenanceTimeDao implements EntitiesWithNewStateListener {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final int drsMaintenanceProtectionWindow;

    private final Clock clock;

    private final boolean accountForVendorAutomation;

    /**
     * Constructor.
     *
     * @param dsl Jooq context to use
     * @param drsMaintenanceProtectionWindow number of seconds to keep host controllable false
     *                                     after it exits maintenance mode
     * @param clock clock
     * @param accountForVendorAutomation enable or disable feature
     */
    EntityMaintenanceTimeDao(final DSLContext dsl,
                             final int drsMaintenanceProtectionWindow,
                             final Clock clock,
                             final boolean accountForVendorAutomation) {
        this.dsl = Objects.requireNonNull(dsl);
        this.drsMaintenanceProtectionWindow = drsMaintenanceProtectionWindow;
        this.clock = clock;
        this.accountForVendorAutomation = accountForVendorAutomation;
    }

    /**
     * Listen to {@link com.vmturbo.topology.processor.entity.EntityStore}.
     * Record hosts in maintenance mode and update the time when a host exits maintenance mode.
     *
     * @param entitiesWithNewState entities with new state
     */
    @Override
    public void onEntitiesWithNewState(final EntitiesWithNewState entitiesWithNewState) {
        if (accountForVendorAutomation) {
            updateMaintenanceExitTime(entitiesWithNewState);
            recordMaintenanceHosts(entitiesWithNewState);
        }
    }

    /**
     * Delete expired records and return all hosts that should be marked as controllable false.
     *
     * @return hosts that should be marked as controllable false
     */
    public Set<Long> getControllableFalseHost() {
        Set<Long> oids;
        try {
            deleteExpiredRecords();
            oids = dsl.selectFrom(ENTITY_MAINTENANCE)
                .where(ENTITY_MAINTENANCE.EXIT_TIME.isNotNull())
                .fetchSet(ENTITY_MAINTENANCE.ENTITY_OID);
        } catch (DataAccessException e) {
            logger.error("Failed to read the entities in maintenance state from the db.", e);
            oids = Collections.emptySet();
        }

        return oids;
    }

    /**
     * Delete expired records. This method is called every broadcast.
     *
     * @throws DataAccessException if an error occurs in db operation
     */
    private void deleteExpiredRecords() throws DataAccessException {
        dsl.transaction(configuration -> {
            final DSLContext dslContext = DSL.using(configuration);
            final LocalDateTime now = LocalDateTime.now(clock);
            final LocalDateTime expiredThresholdTime =
                now.minusSeconds(drsMaintenanceProtectionWindow);

            dslContext.deleteFrom(ENTITY_MAINTENANCE)
                .where(ENTITY_MAINTENANCE.EXIT_TIME.lessOrEqual(expiredThresholdTime))
                .execute();
        });
    }

    /**
     * Update the time when a host exits maintenance mode.
     *
     * @param entitiesWithNewState entities with new state
     */
    private void updateMaintenanceExitTime(final EntitiesWithNewState entitiesWithNewState) {
        // We know that the state of a host that exits maintenance mode is POWERED_ON,
        // but we don't know the previous state of a host.
        // If the state of a host is POWERED_ON, it doesn't mean that its previous state must be MAINTENANCE.
        // Its previous state can be POWERED_OFF or UNKNOWN, etc.
        // So in order to update EXIT_TIME correctly (update only when a host exits maintenance mode),
        // we need to introduce a way to figure out its previous state:
        // If the given host oid exists in table and its EXIT_TIME value is null,
        // it means the previous state is MAINTENANCE.
        // This is because record (host_oid, null) will be inserted only when a host enters maintenance mode.
        final Set<Long> automatedHostsNotInMaintenanceMode = entitiesWithNewState.getTopologyEntityList().stream()
            .filter(entity -> entity.getEntityType() == EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE)
            .filter(entity -> entity.getEntityState() != EntityState.MAINTENANCE)
            .filter(entity -> entity.hasTypeSpecificInfo()
                && entity.getTypeSpecificInfo().hasPhysicalMachine()
                && entity.getTypeSpecificInfo().getPhysicalMachine().hasAutomationLevel()
                && entity.getTypeSpecificInfo().getPhysicalMachine().getAutomationLevel() == AutomationLevel.FULLY_AUTOMATED)
            .map(TopologyEntityDTO::getOid)
            .collect(Collectors.toSet());

        logger.trace("Automated hosts not in maintenance mode: {}", automatedHostsNotInMaintenanceMode);
        if (automatedHostsNotInMaintenanceMode.isEmpty()) {
            return;
        }

        try {
            dsl.transaction(configuration -> {
                final DSLContext dslContext = DSL.using(configuration);
                final LocalDateTime now = LocalDateTime.now(clock);

                dslContext.update(ENTITY_MAINTENANCE)
                    .set(ENTITY_MAINTENANCE.EXIT_TIME, now)
                    .where(ENTITY_MAINTENANCE.ENTITY_OID.in(automatedHostsNotInMaintenanceMode))
                    // Previous state of host is MAINTENANCE if and only if exit_time is null
                    .and(ENTITY_MAINTENANCE.EXIT_TIME.isNull())
                    .execute();
            });
        } catch (DataAccessException e) {
            logger.error("Failed to update EXIT_TIME of hosts {} exiting maintenance mode.",
                automatedHostsNotInMaintenanceMode, e);
        }
    }

    /**
     * Record hosts in maintenance mode. EXIT_TIME will be set to null.
     *
     * @param entitiesWithNewState entities with new state
     */
    private void recordMaintenanceHosts(final EntitiesWithNewState entitiesWithNewState) {
        final Set<Long> hostsInMaintenanceMode = entitiesWithNewState.getTopologyEntityList().stream()
            .filter(entity -> entity.getEntityType() == EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE)
            .filter(entity -> entity.getEntityState() == EntityState.MAINTENANCE)
            .map(TopologyEntityDTO::getOid)
            .collect(Collectors.toSet());

        logger.trace("Hosts in maintenance mode: {}", hostsInMaintenanceMode);
        if (hostsInMaintenanceMode.isEmpty()) {
            return;
        }

        try {
            dsl.transaction(configuration -> {
                final DSLContext dslContext = DSL.using(configuration);

                final List<InsertOnDuplicateSetMoreStep<EntityMaintenanceRecord>> inserts =
                    hostsInMaintenanceMode.stream()
                        .map(hostOid -> dslContext.insertInto(ENTITY_MAINTENANCE)
                            .set(ENTITY_MAINTENANCE.ENTITY_OID, hostOid)
                            .setNull(ENTITY_MAINTENANCE.EXIT_TIME)
                            .onDuplicateKeyUpdate()
                            .setNull(ENTITY_MAINTENANCE.EXIT_TIME))
                        .collect(Collectors.toList());

                dslContext.batch(inserts).execute();
            });
        } catch (DataAccessException e) {
            logger.error("Failed to record hosts {} in maintenance mode.", hostsInMaintenanceMode, e);
        }
    }
}
