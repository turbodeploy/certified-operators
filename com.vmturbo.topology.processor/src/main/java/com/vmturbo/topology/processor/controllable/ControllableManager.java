package com.vmturbo.topology.processor.controllable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.AutomationLevel;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;

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

    private final boolean accountForVendorAutomation;

    private final EntityMaintenanceTimeDao entityMaintenanceTimeDao;

    private static final Logger logger = LogManager.getLogger();

    public ControllableManager(@Nonnull final EntityActionDao entityActionDao,
                               @Nonnull final EntityMaintenanceTimeDao entityMaintenanceTimeDao,
                               final boolean accountForVendorAutomation) {
        this.entityActionDao = Objects.requireNonNull(entityActionDao);
        this.entityMaintenanceTimeDao = Objects.requireNonNull(entityMaintenanceTimeDao);
        this.accountForVendorAutomation = accountForVendorAutomation;
    }

    /**
     * First get all out of controllable entity ids, and change their TopologyEntityDTO controllable
     * flag to false.
     *
     * @param topology a topology graph.
     * @return Number of modified entities.
     */
    public int applyControllable(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        final Set<Long> oidModified = new HashSet<>();
        oidModified.addAll(applyControllableEntityAction(topology));
        oidModified.addAll(markVMsOnFailoverOrUnknownHostAsNotControllable(topology));
        oidModified.addAll(markSuspendedEntitiesAsNotControllable(topology));
        if (accountForVendorAutomation) {
            oidModified.addAll(applyControllableAutomationLevel(topology));
            oidModified.addAll(keepControllableFalseAfterExitingMaintenanceMode(topology));
        }
        return oidModified.size();
    }

    /**
     * Mark pods which are in ready state and are set uncontrollable to controllable so that they can
     * be considered in plan. This handles cases where such pods will remain non intuitively unplaced
     * for example if the VM which they are currently on gets suspended, or such pods are added
     * as cloned pods in the plan.
     *
     * @param topology a topology graph.
     * @return Number of modified entities.
     */
    public int setUncontrollablePodsToControllableInPlan(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        return topology.entitiesOfType(EntityDTO.EntityType.CONTAINER_POD)
                .filter(entity -> entity.getTopologyEntityDtoBuilder().getEntityState() == EntityState.POWERED_ON
                        && entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder().getControllable() == false)
                .filter(entity -> {
                    Set<Long> providerOids = entity.getTopologyEntityDtoBuilder()
                            .getCommoditiesBoughtFromProvidersList().stream()
                            .filter(comms -> comms.getProviderEntityType() == EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE
                                    && comms.hasProviderId())
                            .map(TopologyEntityDTO.CommoditiesBoughtFromProvider::getProviderId)
                            .collect(Collectors.toSet());
                    for (Long providerOid : providerOids) {
                        // We would ideally have only one VM provider for each pod
                        if (providerOid < 0 || (topology.getEntity(providerOid).isPresent()
                        && topology.getEntity(providerOid).get().getTopologyEntityDtoBuilder()
                                .getEditBuilder().hasRemoved())) {
                            // filter cloned pods or those pods for which the VM has been removed
                            return true;
                        }
                    }
                    return false;
                })
                .map(entity -> {
                    entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder().setControllable(true);
                    logger.trace("Mark Pod {} as controllable in plan", entity);
                    return entity.getOid();
                })
                .collect(Collectors.toSet()).size();
    }

    /**
     * First get all out of controllable entity ids, and change their TopologyEntityDTO controllable
     * flag to false.
     *
     * @param topology a topology graph.
     * @return a set of modified entity oids.
     */
    private Set<Long> applyControllableEntityAction(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        final Set<Long> oidModified = new HashSet<>();
        List<Long> oidsToRemoveFromDB = new ArrayList<>();

        for (long entityOid : entityActionDao.getNonControllableEntityIds()) {
            Optional<TopologyEntity> entityOptional = topology.getEntity(entityOid);
            if (entityOptional.isPresent()) {
                TopologyEntityDTO.Builder entityBuilder = entityOptional.get().getTopologyEntityDtoBuilder();

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
                    .map(shoppinglist -> topology.getEntity(shoppinglist.getProviderId()))
                    .filter(Optional::isPresent)
                    .anyMatch(supplier ->
                        supplier.get().getTopologyEntityDtoBuilder().getEntityState() == EntityState.MAINTENANCE)) {
                    continue;
                }

                entityBuilder.getAnalysisSettingsBuilder().setControllable(false);
                oidModified.add(entityOid);
                logger.trace("Applying controllable false for entity {}.",
                    entityBuilder.getDisplayName());
            } // end if
        } // end foreach

        entityActionDao.deleteMoveActions(oidsToRemoveFromDB);
        return oidModified;
    }

    /**
     * Mark VMs on a failover or unknown host as not controllable.
     * We don't need to mark failover or unknown hosts non-controllable. We skip them in market analysis.
     *
     * @param topology a topology graph.
     * @return a set of modified entity oids.
     */
    private Set<Long> markVMsOnFailoverOrUnknownHostAsNotControllable(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        return topology.entitiesOfType(EntityDTO.EntityType.PHYSICAL_MACHINE)
            .filter(entity -> entity.getTopologyEntityDtoBuilder().getEntityState() == EntityState.FAILOVER
                || entity.getTopologyEntityDtoBuilder().getEntityState() == EntityState.UNKNOWN)
            .flatMap(entity -> entity.getConsumers().stream())
            .filter(entity -> entity.getEntityType() == EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
            .map(entity -> {
                entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder().setControllable(false);
                logger.trace("Mark VM {} as not controllable", entity);
                return entity.getOid();
            })
            .collect(Collectors.toSet());
    }

    /**
     * Mark suspended entities as not controllable.
     *
     * @param topology a topology graph.
     * @return a set of modified entity oids.
     */
    private Set<Long> markSuspendedEntitiesAsNotControllable(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        return topology.entities()
            .filter(entity -> entity.getTopologyEntityDtoBuilder().getEntityState() == EntityState.SUSPENDED)
            .map(entity -> {
                entity.getTopologyEntityDtoBuilder().getAnalysisSettingsBuilder().setControllable(false);
                logger.trace("Mark suspended entity {}({}) as not controllable", entity.getDisplayName(), entity.getOid());
                return entity.getOid();
            })
            .collect(Collectors.toSet());
    }

    /**
     * Set controllable to false for hosts in maintenance mode if automation level is FULLY_AUTOMATED.
     *
     * @param topology a topology graph.
     * @return a set of modified entity oids.
     */
    private Set<Long> applyControllableAutomationLevel(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        return topology.entitiesOfType(EntityDTO.EntityType.PHYSICAL_MACHINE)
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .filter(entity -> entity.getEntityState() == EntityState.MAINTENANCE)
            .filter(entity -> entity.getTypeSpecificInfoOrBuilder().hasPhysicalMachine())
            .filter(entity -> entity.getTypeSpecificInfoOrBuilder().getPhysicalMachineOrBuilder().hasAutomationLevel())
            .filter(entity -> entity.getTypeSpecificInfoOrBuilder().getPhysicalMachineOrBuilder().getAutomationLevel() == AutomationLevel.FULLY_AUTOMATED)
            .map(entity -> {
                entity.getAnalysisSettingsBuilder().setControllable(false);
                logger.trace("Mark suspended entity {} as not controllable", entity);
                return entity.getOid();
            })
            .collect(Collectors.toSet());
    }

    /**
     * Keep controllable false for hosts exit maintenance mode for some time.
     *
     * @param topology topology a topology graph.
     * @return a set of modified entity oids.
     */
    private Set<Long> keepControllableFalseAfterExitingMaintenanceMode(final TopologyGraph<TopologyEntity> topology) {
        return entityMaintenanceTimeDao.getControllableFalseHost().stream()
            .map(topology::getEntity)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .map(entity -> {
                entity.getAnalysisSettingsBuilder().setControllable(false);
                logger.trace("Set controllable to false for host {}", entity);
                return entity.getOid();
            })
            .collect(Collectors.toSet());
    }

    /**
     * If entity has an activate action in the table, it means the entity is about or
     * has been activated. It should not be suspendable in analysis.
     *
     * @param topology a topology graph.
     * @return Number of modified entities.
     */
    public int applySuspendable(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        final AtomicInteger numModified = new AtomicInteger(0);
        entityActionDao.getNonSuspendableEntityIds().stream()
            .map(topology::getEntity)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
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
     * @param topology a topology graph.
     * @return Number of modified entities.
     */
    public int applyScaleEligibility(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        final AtomicInteger numModified = new AtomicInteger(0);
        entityActionDao.ineligibleForScaleEntityIds().stream()
                .map(topology::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                // Set flag only for VMs and DBSs
                .filter(entityBuilder -> {
                    int entityType = entityBuilder.getEntityType();
                    return EntityType.VIRTUAL_MACHINE.getValue() == entityType
                            || EntityType.DATABASE_SERVER.getValue() == entityType;
                })
                .forEach(entityBuilder -> {
                    if (entityBuilder.getAnalysisSettingsBuilder().getIsEligibleForScale()) {
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
     * Update the resizable flag.
     *
     * @param topology a topology graph.
     * @return Number of modified entities.
     */
    public int applyResizable(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        final Set<Long> oidModified = new HashSet<>();
        oidModified.addAll(applyResizeDownEligibility(topology));
        oidModified.addAll(markVMsOnMaintenanceHostAsNotResizable(topology));
        return oidModified.size();
    }

    /**
     * If entity has an RIGHT_SIZE (resize actions on prem) action in the table, it means the entity is about or
     * has been resized. It should not be to resize down in analysis.
     *
     * @param topology a topology graph.
     * @return a set of modified entity oids.
     */
    private Set<Long> applyResizeDownEligibility(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        return entityActionDao.ineligibleForResizeDownEntityIds().stream()
                .map(topology::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                // Set flag only for VMs
                .filter(entityBuilder -> entityBuilder.getEntityType() == EntityType.VIRTUAL_MACHINE.getValue())
                .map(entityBuilder -> {
                    entityBuilder.getAnalysisSettingsBuilder().setIsEligibleForResizeDown(false);
                    logger.trace("Applying IsEligibleForResizeDown false for entity {}.",
                            entityBuilder.getDisplayName());
                    return entityBuilder.getOid();
                }).collect(Collectors.toSet());
    }

    /**
     * Mark VMs on a maintenance host as not resizable.
     *
     * @param topology a topology graph.
     * @return a set of modified entity oids.
     */
    private Set<Long> markVMsOnMaintenanceHostAsNotResizable(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        return topology.entitiesOfType(EntityDTO.EntityType.PHYSICAL_MACHINE)
            .filter(entity -> entity.getTopologyEntityDtoBuilder().getEntityState() == EntityState.MAINTENANCE)
            .flatMap(entity -> entity.getConsumers().stream())
            .filter(entity -> entity.getEntityType() == EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
            .map(entity -> {
                entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()
                    .forEach(commSold -> commSold.setIsResizeable(false));
                logger.trace("Mark VM {} as not resizable", entity);
                return entity.getOid();
            })
            .collect(Collectors.toSet());
    }
}
