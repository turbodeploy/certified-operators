package com.vmturbo.topology.processor.controllable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;

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
     * First get all out of controllable entity ids, and change their TopologyEntityDTO controllable
     * flag to false.
     *
     * @param topology a Map contains all topology entities, and which key is entity Id, and value
     *                 is {@link TopologyEntity.Builder}.
     * @return Number of modified entities.
     */
    public int applyControllable(@Nonnull final Map<Long, TopologyEntity.Builder> topology) {
        int numModified = 0;
        List<Long> oidsToRemoveFromDB = new ArrayList<>();

        for (long entityOid : entityActionDao.getNonControllableEntityIds()) {
            Builder builder = topology.get(entityOid);
            if (builder != null) {
                TopologyEntityDTO.Builder entityBuilder = builder.getEntityBuilder();

                if (entityBuilder.getEntityState() == EntityState.MAINTENANCE) {
                    // Clear action information regarding this entity from ENTITY_ACTION table so
                    // that when entity exits maintenance mode it will be controllable.
                    // We collect all such entity OIDs in a list so we can make a single call to
                    // the database. That reduces overhead and makes the removal transactional.
                    oidsToRemoveFromDB.add(entityOid);
                    // Entities in maintenance mode are always controllable.
                    continue;
                }

                if (entityBuilder.getCommoditiesBoughtFromProvidersList().stream()
                    .map(shoppinglist -> topology.get(shoppinglist.getProviderId()))
                    .filter(Objects::nonNull)
                    .anyMatch(supplier ->
                        supplier.getEntityBuilder().getEntityState() == EntityState.MAINTENANCE)
                ) {
                    continue;
                }

                if (entityBuilder.getAnalysisSettingsBuilder().getControllable()) {
                    // It's currently controllable, and about to be marked non-controllable.
                    ++numModified;
                } // end if
                entityBuilder.getAnalysisSettingsBuilder().setControllable(false);
                logger.trace("Applying controllable false for entity {}.",
                    entityBuilder.getDisplayName());
            } // end if
        } // end foreach

        entityActionDao.deleteMoveActions(oidsToRemoveFromDB);
        return numModified;
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
        entityActionDao.getNonSuspendableEntityIds().stream()
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
        entityActionDao.ineligibleForScaleEntityIds().stream()
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
        entityActionDao.ineligibleForResizeDownEntityIds().stream()
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
