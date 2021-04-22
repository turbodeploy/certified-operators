package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;

/**
 * Module to track entity realized/missed savings/investments stats.
 */
public class EntitySavingsTracker {
    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();

    private final EntitySavingsStore entitySavingsStore;

    private final EntityEventsJournal entityEventsJournal;

    private final EntityStateStore entityStateStore;

    private final SavingsCalculator savingsCalculator;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final RepositoryClient repositoryClient;

    private final long realtimeTopologyContextId;

    private final int chunkSize;

    private final Clock clock;

    /**
     * Constructor.
     *
     * @param entitySavingsStore entitySavingsStore
     * @param entityEventsJournal entityEventsJournal
     * @param entityStateStore Persistent state store.
     * @param clock clock
     * @param cloudTopologyFactory cloud topology factory
     * @param repositoryClient repository client
     * @param realtimeTopologyContextId realtime topology context ID
     * @param chunkSize chunkSize for database batch operations
     */
    EntitySavingsTracker(@Nonnull EntitySavingsStore entitySavingsStore,
                         @Nonnull EntityEventsJournal entityEventsJournal,
                         @Nonnull EntityStateStore entityStateStore,
                         @Nonnull final Clock clock,
                         @Nonnull TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                         @Nonnull RepositoryClient repositoryClient,
                         long realtimeTopologyContextId,
                         final int chunkSize) {
        this.entitySavingsStore = Objects.requireNonNull(entitySavingsStore);
        this.entityEventsJournal = Objects.requireNonNull(entityEventsJournal);
        this.entityStateStore = Objects.requireNonNull(entityStateStore);
        this.savingsCalculator = new SavingsCalculator();
        this.clock = clock;
        this.cloudTopologyFactory = cloudTopologyFactory;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.repositoryClient = repositoryClient;
        this.chunkSize = chunkSize;
    }

    /**
     * Process events posted to the internal state of each entity whose savings/investments are
     * being tracked.  Processing will stop at the supplied time.
     *
     * @param startTime start time
     * @param endTime end time
     * @return List of times to the hour mark for which we have wrote stats this time, so these
     *      hours are now eligible for daily and monthly rollups, if applicable.
     */
    @Nonnull
    List<Long> processEvents(@Nonnull LocalDateTime startTime, @Nonnull LocalDateTime endTime) {
        logger.debug("Processing savings/investment.");

        final List<Long> hourlyStatsTimes = new ArrayList<>();

        // There should not be any events before period start time left in the journal.
        // If for some reasons old events are left in the journal, remove them.
        List<SavingsEvent> events =
                entityEventsJournal.removeEventsBetween(0, TimeUtil.localDateTimeToMilli(startTime, clock));
        if (events.size() > 0) {
            logger.warn("There are {} in the events journal that have timestamps before period start time of {}.",
                    events.size(), startTime);
        }

        LocalDateTime periodStartTime = startTime;
        LocalDateTime periodEndTime = startTime.plusHours(1);
        try {
            while (periodEndTime.isBefore(endTime) || periodEndTime.equals(endTime)) {
                final long startTimeMillis = TimeUtil.localDateTimeToMilli(periodStartTime, clock);
                final long endTimeMillis = TimeUtil.localDateTimeToMilli(periodEndTime, clock);

                // Read from entity event journal.
                events = entityEventsJournal.removeEventsBetween(startTimeMillis, endTimeMillis);

                // Get all entity IDs from the events
                Set<Long> entityIds = events.stream()
                        .map(SavingsEvent::getEntityId)
                        .collect(Collectors.toSet());

                logger.info("Process {} events for {} entities between {} ({}) and {} ({}).",
                        events.size(), entityIds.size(), startTimeMillis, periodStartTime,
                        endTimeMillis, periodEndTime);

                // Get states for entities that have events or had state changes in the last period
                // and put the states in the state map.
                Map<Long, EntityState> entityStates = entityStateStore.getEntityStates(entityIds);
                Map<Long, EntityState> forcedEntityStates = entityStateStore
                        .getForcedUpdateEntityStates(periodEndTime);
                entityStates.putAll(forcedEntityStates);

                // Clear the updated_by_event flags
                entityStateStore.clearUpdatedFlags();

                // Invoke calculator
                savingsCalculator.calculate(entityStates, forcedEntityStates.values(), events,
                        startTimeMillis, endTimeMillis);

                // Update entity states. Also insert new states to track new entities.
                entityStateStore.updateEntityStates(entityStates, createCloudTopology(entityStates.keySet()));

                try {
                    // create stats records from state map for this period.
                    generateStats(startTimeMillis);
                    hourlyStatsTimes.add(startTimeMillis);
                } catch (EntitySavingsException e) {
                    logger.error("Error occurred when Entity Savings Tracker writes stats to entity savings store. "
                            + "Start time: {} End time: {}", startTime, endTime, e);
                    // Stop processing and don't update the last period end time.
                    break;
                }
                // We delete inactive entity state after the stats for the entity have been flushed
                // a final time.
                Set<Long> statesToRemove = entityStates.values().stream()
                        .filter(EntityState::isDeletePending)
                        .map(EntityState::getEntityId)
                        .collect(Collectors.toSet());
                entityStateStore.deleteEntityStates(statesToRemove);

                // Advance time period by 1 hour.
                periodStartTime = periodStartTime.plusHours(1);
                periodEndTime = periodEndTime.plusHours(1);
            }
        } catch (EntitySavingsException e) {
            logger.error("Operation error in entity state store.", e);
        }

        logger.debug("Savings/investment processing complete for {} hourly times.",
                hourlyStatsTimes.size());
        return hourlyStatsTimes;
    }

    /**
     * Generate savings stats records from the state map.
     *
     * @param statTime Timestamp of the stats records which is the start time of a period.
     * @throws EntitySavingsException Error occurred when inserting the DB records.
     */
    @VisibleForTesting
    void generateStats(long statTime) throws EntitySavingsException {
        Set<EntitySavingsStats> stats = new HashSet<>();
        // Use try with resource here because the stream implementation uses an open cursor that
        // need to be closed.
        try (Stream<EntityState> stateStream = entityStateStore.getAllEntityStates()) {
            stateStream.forEach(state -> {
                stats.addAll(stateToStats(state, statTime));
                if (stats.size() >= chunkSize) {
                    try {
                        entitySavingsStore.addHourlyStats(stats);
                    } catch (EntitySavingsException e) {
                        // Wrap exception in RuntimeException and rethrow because it is within a lambda.
                        throw new RuntimeException(e);
                    }
                    stats.clear();
                }
            });
            if (!stats.isEmpty()) {
                // Flush partial chunk
                entitySavingsStore.addHourlyStats(stats);
            }
        } catch (RuntimeException e) {
            if (e.getCause() instanceof EntitySavingsException) {
                throw new EntitySavingsException("Error occurred when adding stats to database.", e.getCause());
            }
            throw e;
        }
    }

    private Set<EntitySavingsStats> stateToStats(@Nonnull EntityState state, long statTime) {
        Set<EntitySavingsStats> stats = new HashSet<>();
        long entityId = state.getEntityId();
        Double savings = state.getRealizedSavings();
        if (savings != null) {
            stats.add(new EntitySavingsStats(entityId, statTime,
                    EntitySavingsStatsType.REALIZED_SAVINGS, savings));
        }
        Double investments = state.getRealizedInvestments();
        if (investments != null) {
            stats.add(new EntitySavingsStats(entityId, statTime,
                    EntitySavingsStatsType.REALIZED_INVESTMENTS, investments));
        }
        savings = state.getMissedSavings();
        if (savings != null) {
            stats.add(new EntitySavingsStats(entityId, statTime,
                    EntitySavingsStatsType.MISSED_SAVINGS, savings));
        }
        investments = state.getMissedInvestments();
        if (investments != null) {
            stats.add(new EntitySavingsStats(entityId, statTime,
                    EntitySavingsStatsType.MISSED_INVESTMENTS, investments));
        }
        return stats;
    }

    private TopologyEntityCloudTopology createCloudTopology(Set<Long> entityOids) {
        // The cloud topology requires the list of OIDs of entities (VMs, volumes, DBs, DBSs) and
        // their associated accounts, availability zones (if applicable), regions and service providers.

        // Find all availability zones and business accounts associated with the entities.
        Stream<TopologyEntityDTO> workloadEntities =
                repositoryClient.retrieveTopologyEntities(new ArrayList<>(entityOids), realtimeTopologyContextId);
        List<Long> availabilityZoneOids = workloadEntities.flatMap(entity -> entity.getConnectedEntityListList().stream())
                .filter(connEntity -> connEntity.getConnectedEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                .map(ConnectedEntity::getConnectedEntityId)
                .distinct()
                .collect(Collectors.toList());

        // Find all related accounts.
        Set<Long> accountOids = repositoryClient.getAllBusinessAccountOidsInScope(entityOids);

        // Get all regions and service provider entities.
        // Note that we get all all regions and all service providers instead of only those
        // associated with the entities.
        // It is because the number of regions and service providers is finite.
        // The logic to find the connected regions of availability zones requires all regions anyways.
        List<Long> regionAndAServiceProviderOids =
                repositoryClient.getEntitiesByType(Arrays.asList(EntityType.REGION, EntityType.SERVICE_PROVIDER))
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toList());

        List<Long> entityOidList = new ArrayList<>(entityOids);
        entityOidList.addAll(availabilityZoneOids);
        entityOidList.addAll(accountOids);
        entityOidList.addAll(regionAndAServiceProviderOids);

        return cloudTopologyFactory.newCloudTopology(
                repositoryClient.retrieveTopologyEntities(entityOidList, realtimeTopologyContextId));
    }
}
