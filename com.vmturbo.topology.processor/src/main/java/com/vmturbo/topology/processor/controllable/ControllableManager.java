package com.vmturbo.topology.processor.controllable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.stitching.TopologyEntity;

/**
 * Controllable means that when there are some actions are executing, we should not let Market generate
 * resize, move, suspend actions for its providers. Because when the action is executing, those hosts
 * utilization maybe above its normal value, especially in automatically controlling environment.
 * <p>
 * Class responsible for modify topology entity DTO controllable flag. It will get all entity ids from
 * entity action table which stores current all not controllable entity ids. For each not controllable
 * entity id, it should change its TopologyEntityDTO controllable flag to false.
 */
public class ControllableManager {

    private final EntityActionDao entityActionDao;

    private static final Logger logger = LogManager.getLogger();

    public ControllableManager(@Nonnull final EntityActionDao entityActionDao) {
        this.entityActionDao = Objects.requireNonNull(entityActionDao);
    }

    /**
     * First get all out of controllable entity ids, and change their TopologyEntityDTO controllable flag
     * to false.
     *
     * @param topology a Map contains all topology entities, and which key is entity Id, and value is
     *                 {@link TopologyEntity.Builder}.
     * @return Number of modified entities.
     */
    public int applyControllable(@Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        final AtomicInteger numModified = new AtomicInteger(0);
        final Set<Long> entityIdsNotControllable =
                entityActionDao.getNonControllableEntityIds();
        entityIdsNotControllable.stream()
                .filter(topology::containsKey)
                .map(topology::get)
                .map(TopologyEntity.Builder::getEntityBuilder)
                .forEach(entityBuilder -> {
                        if (entityBuilder.getAnalysisSettingsBuilder().getControllable()) {
                            // It's currently controllable, and about to be marked
                            // non-controllable.
                            numModified.incrementAndGet();
                        }
                        entityBuilder.getAnalysisSettingsBuilder().setControllable(false);
                        logger.trace("Applying controllable false for entity {}.",
                                entityBuilder.getDisplayName());
                });
        return numModified.get();
    }

    /**
     * If entity has an activate action in the table, it means the entity is about or
     * has been activated. It should not be suspendable in analysis.
     *
     * @param topology a Map contains all topology entities, and which key is entity Id, and value is
     *                 {@link TopologyEntity.Builder}.
     * @return Number of modified entities.
     */
    public int applySuspendable(@Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        final AtomicInteger numModified = new AtomicInteger(0);
        final Set<Long> entityIdsNotSuspendable =
                entityActionDao.getNonSuspendableEntityIds();
        entityIdsNotSuspendable.stream()
            .filter(topology::containsKey)
            .map(topology::get)
            .map(TopologyEntity.Builder::getEntityBuilder)
            .forEach(entityBuilder -> {
                if (entityBuilder.getAnalysisSettingsBuilder().getSuspendable()) {
                    // It's currently suspendable, and about to be marked
                    // non-suspendable.
                    numModified.incrementAndGet();
                }
                entityBuilder.getAnalysisSettingsBuilder().setSuspendable(false);
                logger.trace("Applying suspendable false for entity {}.",
                        entityBuilder.getDisplayName());
            });
        return numModified.get();
    }
}
