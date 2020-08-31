package com.vmturbo.cost.component.topology;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.cost.CostDebug.EnableCostRecordingRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CostJournalRecorder {

    private static final Logger logger = LogManager.getLogger();

    /**
     * These are the entities we look for by default. This is because these are the "top-level"
     * entities for cost calculation. The main intent is to avoid picking "VOLUME" because it
     * doesn't have many factors contributing to its cost, and recording a VM would record the
     * underlying volumes anyway.
     */
    private static Set<Integer> DEFAULT_SELECTION_TYPES = ImmutableSet.of(
            EntityType.VIRTUAL_MACHINE_VALUE,
            EntityType.DATABASE_VALUE,
            EntityType.DATABASE_SERVER_VALUE);

    /**
     * The maximum number of broadcasts a non-default {@link EntitySelector} will be used.
     * See {@link CostJournalRecorder#overrideBroadcasts}.
     */
    private static final int MAX_OVERRIDE_BROADCASTS = 10;

    /**
     * The default entity selector, used during normal operations.
     * See cost/CostDebug.proto for externally-available options to override
     * the entity selector
     * (handled in {@link CostJournalRecorder#overrideEntitySelector(EnableCostRecordingRequest)}).
     */
    private static EntitySelector DEFAULT_SELECTOR = (journals) -> {
        // By default, pick the first non-empty cost journal and print log it.
        final Optional<CostJournal<TopologyEntityDTO>> selectedEntity =
            journals.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .filter(entry -> DEFAULT_SELECTION_TYPES.contains(entry.getValue().getEntity().getEntityType()))
                .map(Entry::getValue)
                .findFirst();
        if (selectedEntity.isPresent()) {
            return addDependencies(
                Collections.singleton(selectedEntity.get().getEntity().getOid()),
                journals);
        } else {
            logger.info("All cost journals empty.");
            return Collections.emptySet();
        }
    };

    @GuardedBy("recorderLock")
    private final Map<Long, CostJournal<TopologyEntityDTO>> trackedJournals = new HashMap<>();

    @GuardedBy("recorderLock")
    private EntitySelector entitySelector = DEFAULT_SELECTOR;

    /**
     * It's possible to "override" the entity selection logic the recorder uses via an external
     * RPC (see {@link CostJournalRecorder#overrideEntitySelector(EnableCostRecordingRequest)}.
     *
     * We don't want to allow the override to continue infinitely, because it can have a significant
     * performance impact. This variable controls how many broadcasts to override the default logic
     * for.
     */
    private volatile int overrideBroadcasts = 0;

    private final Object recorderLock = new Object();

    /**
     * Record a subset of the passed-in cost journals, saving them for retrieval via API and
     * printing them to the logs.
     *
     * @param journals The journals, as calculated in {@link TopologyCostCalculator}.
     */
    public void recordCostJournals(@Nonnull final Map<Long, CostJournal<TopologyEntityDTO>> journals) {
        logger.info("Recording cost journals...");
        synchronized (recorderLock) {
            trackedJournals.clear();
            final Set<Long> entitiesToPick = entitySelector.selectEntities(journals);
            entitiesToPick.forEach(id -> trackedJournals.put(id, journals.get(id)));
            if (overrideBroadcasts > 0) {
                overrideBroadcasts = Math.max(overrideBroadcasts - 1, 0);
                if (overrideBroadcasts == 0) {
                    resetEntitySelector();
                }
            }

            // Log all tracked journals.
            trackedJournals.values().forEach(journal -> logger.debug("\n {}", journal.toString()));
        }
    }

    /**
     * Get the descriptions of the recorded cost journals.
     * @param ids The IDs to look for. If empty, get all recorded cost journals.
     * @return A stream of {@link String}s, where each string describes one journal.
     */
    @Nonnull
    public Stream<String> getJournalDescriptions(@Nonnull final Set<Long> ids) {
        synchronized (recorderLock) {
            if (ids.isEmpty()) {
                return trackedJournals.values().stream()
                        .map(CostJournal::toString);
            } else {
                return addDependencies(ids, trackedJournals).stream()
                    .map(trackedJournals::get)
                    .filter(Objects::nonNull)
                    .map(CostJournal::toString);
            }
        }
    }

    /**
     * Override the entity selector used to choose which journals to record.
     *
     * @param overrideRequest The gRPC request specifying the criteria to use for entity selection.
     */
    public void overrideEntitySelector(@Nonnull final EnableCostRecordingRequest overrideRequest) {
        logger.info("Overriding cost recorder configuration with: {}", overrideRequest);
        synchronized (recorderLock) {
            final int reqNumBroadcasts = overrideRequest.getNumBroadcasts();
            if (reqNumBroadcasts > MAX_OVERRIDE_BROADCASTS) {
                logger.warn("Requested number of override broadcasts ({}) greater than " +
                        "maximum allowed ({}).", reqNumBroadcasts, MAX_OVERRIDE_BROADCASTS);
            } else if (reqNumBroadcasts < 1) {
                throw new IllegalArgumentException("Illegal requested number of broadcasts to override.");
            }
            overrideBroadcasts = Math.min(reqNumBroadcasts, MAX_OVERRIDE_BROADCASTS);

            switch (overrideRequest.getFilterCase()) {
                case ENTITY:
                    if (overrideRequest.getEntity().getEntityIdsList().isEmpty()) {
                        throw new IllegalArgumentException("Entity ID list must not be empty.");
                    }
                    entitySelector = journals ->
                        addDependencies(overrideRequest.getEntity().getEntityIdsList(), journals);
                    break;
                case COUNT:
                    // Pick random non-empty cost journals.
                    entitySelector = journals -> {
                        List<Long> entities = journals.entrySet().stream()
                            .filter(entry -> !entry.getValue().isEmpty())
                            .map(Entry::getKey)
                            .collect(Collectors.toList());
                        if (entities.size() < overrideRequest.getCount().getCount()) {
                            return addDependencies(entities, journals);
                        } else {
                            // If the number of non-empty journals are greater than the number
                            // of costs requested, we want to pick randomly. We shuffle to make
                            // sure the entities selected every broadcast are different - don't
                            // want to purely rely on the random order in the input journals.
                            Collections.shuffle(entities);
                            return addDependencies(entities.subList(0, overrideRequest.getCount().getCount()), journals);
                        }
                    };
                    break;
                case RECORD_ALL:
                    entitySelector = Map::keySet;
                    break;
                default:
                    throw new IllegalArgumentException("Must specify a filter case. Bad value: " +
                            overrideRequest.getFilterCase());
            }
        }
    }

    @Nonnull
    private static Set<Long> addDependencies(@Nonnull final Collection<Long> ids,
                         @Nonnull final Map<Long, CostJournal<TopologyEntityDTO>> costJournals) {
        final Set<Long> retSet = new HashSet<>(ids);
        ids.stream()
            .map(costJournals::get)
            .filter(Objects::nonNull)
            .flatMap(CostJournal::getDependentJournals)
            .map(CostJournal::getEntity)
            .map(TopologyEntityDTO::getOid)
            .filter(costJournals::containsKey)
            .forEach(retSet::add);
        return retSet;
    }


    /**
     * Reset the entity selection criteria to the default.
     */
    public void resetEntitySelector() {
        logger.info("Resetting cost recorder configuration.");
        synchronized (recorderLock) {
            entitySelector = DEFAULT_SELECTOR;
        }
    }

    /**
     * Select the entities that should be recorded out of a particular set of cost journals.
     */
    @FunctionalInterface
    private interface EntitySelector {

        @Nonnull
        Set<Long> selectEntities(@Nonnull final Map<Long, CostJournal<TopologyEntityDTO>> journals);
    }
}
