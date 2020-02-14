package com.vmturbo.topology.processor.controllable;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
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

    /**
     * If entity has an SCALE (resize actions on cloud) action in the table, it means the entity is about or
     * has been scaled. It should not be resizeable in analysis.
     *
     * @param topology a Map contains all topology entities, and which key is entity Id, and value is
     *                 {@link TopologyEntity.Builder}.
     * @return Number of modified entities.
     */
    public int applyScaleEligibility(@Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        final AtomicInteger numModified = new AtomicInteger(0);
        final Set<Long> ineligibleForResizeEntityIds =
                entityActionDao.ineligibleForScaleEntityIds();
        ineligibleForResizeEntityIds.stream()
                .filter(topology::containsKey)
                .map(topology::get)
                .map(TopologyEntity.Builder::getEntityBuilder)
                // Set flag only for VMs
                .filter(entityBuilder -> entityBuilder.getEntityType() == EntityType.VIRTUAL_MACHINE.getValue())
                .forEach(entityBuilder -> {
                    if (entityBuilder.getAnalysisSettingsBuilder().getIsEligibleForScale()
                            && entityBuilder.getEntityType() == EntityType.VIRTUAL_MACHINE.getValue()) {
                        // It's currently eligible for scale, and about to be marked
                        // ineligible.
                        numModified.incrementAndGet();
                    }
                    entityBuilder.getAnalysisSettingsBuilder().setIsEligibleForScale(false);
                    logger.trace("Applying is eligible for scale false for entity {}.",
                            entityBuilder.getDisplayName());
                });
        return numModified.get();
    }

    /**
     * If entity has an RIGHT_SIZE (resize actions on prem) action in the table, it means the entity is about or
     * has been resized. It should not be to resize down in analysis.
     *
     * @param topology a Map contains all topology entities, and which key is entity Id, and value is
     *                 {@link TopologyEntity.Builder}.
     * @return Number of modified entities.
     */
    public int applyResizeDownEligibility(@Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        final AtomicInteger numModified = new AtomicInteger(0);
        final Set<Long> ineligibleForResizeEntityIds =
                entityActionDao.ineligibleForResizeDownEntityIds();
        ineligibleForResizeEntityIds.stream()
                .filter(topology::containsKey)
                .map(topology::get)
                .map(TopologyEntity.Builder::getEntityBuilder)
                // Set flag only for VMs
                .filter(entityBuilder -> entityBuilder.getEntityType() == EntityType.VIRTUAL_MACHINE.getValue())
                .forEach(entityBuilder -> {
                    if (entityBuilder.getAnalysisSettingsBuilder().getIsEligibleForResizeDown()) {
                        // It's currently eligible for resize down, and about to be marked
                        // ineligible.
                        numModified.incrementAndGet();
                    }
                    entityBuilder.getAnalysisSettingsBuilder().setIsEligibleForResizeDown(false);
                    logger.trace("Applying IsEligibleForResizeDown false for entity {}.",
                            entityBuilder.getDisplayName());
                });
        return numModified.get();
    }
}
