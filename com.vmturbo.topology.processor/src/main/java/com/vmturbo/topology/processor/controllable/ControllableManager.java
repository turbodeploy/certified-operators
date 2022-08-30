package com.vmturbo.topology.processor.controllable;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.AutomationLevel;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.GraphWithSettings;

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
    private static final Logger logger = LogManager.getLogger();

    private final EntityActionDao entityActionDao;

    private final EntityMaintenanceTimeDao entityMaintenanceTimeDao;

    private final Clock clock;

    public ControllableManager(@Nonnull final EntityActionDao entityActionDao,
                               @Nonnull final EntityMaintenanceTimeDao entityMaintenanceTimeDao,
                               @Nonnull Clock clock) {
        this.entityActionDao = Objects.requireNonNull(entityActionDao);
        this.entityMaintenanceTimeDao = Objects.requireNonNull(entityMaintenanceTimeDao);
        this.clock = clock;
    }

    /**
     * First get all out of controllable entity ids, and change their TopologyEntityDTO controllable
     * flag to false.
     *
     * @param topology a topology graph.
     * @return Number of modified entities.
     */
    public int applyControllable(@Nonnull final GraphWithSettings topology) {
        final Set<Long> oidModified = new HashSet<>();
        TopologyGraph<TopologyEntity> topologyGraph = topology.getTopologyGraph();
        oidModified.addAll(applyControllableEntityAction(topologyGraph));
        oidModified.addAll(markVMsOnFailoverOrUnknownHostAsNotControllable(topologyGraph));
        oidModified.addAll(markSuspendedEntitiesAsNotControllable(topologyGraph));
        oidModified.addAll(applyControllableAutomationLevel(topologyGraph));
        if (FeatureFlags.STORAGE_MAINTENANCE_CONTROLLABLE.isEnabled()) {
            oidModified.addAll(setStoragesInMaintenanceToNonControllable(topologyGraph));
        }
        oidModified.addAll(keepControllableFalseAfterExitingMaintenanceMode(topology));
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
                .filter(entity -> entity.getTopologyEntityImpl().getEntityState() == EntityState.POWERED_ON
                        && entity.getTopologyEntityImpl().getOrCreateAnalysisSettings().getControllable() == false)
                .filter(entity -> {
                    Set<Long> providerOids = entity.getTopologyEntityImpl()
                            .getCommoditiesBoughtFromProvidersList().stream()
                            .filter(comms -> comms.getProviderEntityType() == EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE
                                    && comms.hasProviderId())
                            .map(CommoditiesBoughtFromProviderView::getProviderId)
                            .collect(Collectors.toSet());
                    for (Long providerOid : providerOids) {
                        // We would ideally have only one VM provider for each pod
                        if (providerOid < 0 || (topology.getEntity(providerOid).isPresent()
                        && topology.getEntity(providerOid).get().getTopologyEntityImpl()
                                .getOrCreateEdit().hasRemoved())) {
                            // filter cloned pods or those pods for which the VM has been removed
                            return true;
                        }
                    }
                    return false;
                })
                .map(entity -> {
                    entity.getTopologyEntityImpl().getOrCreateAnalysisSettings().setControllable(true);
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
                TopologyEntityImpl entityBuilder = entityOptional.get().getTopologyEntityImpl();

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
                        supplier.get().getTopologyEntityImpl().getEntityState() == EntityState.MAINTENANCE)) {
                    continue;
                }

                entityBuilder.getOrCreateAnalysisSettings().setControllable(false);
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
            .filter(entity -> entity.getTopologyEntityImpl().getEntityState() == EntityState.FAILOVER
                || entity.getTopologyEntityImpl().getEntityState() == EntityState.UNKNOWN)
            .flatMap(entity -> entity.getConsumers().stream())
            .filter(entity -> entity.getEntityType() == EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
            .map(entity -> {
                entity.getTopologyEntityImpl().getOrCreateAnalysisSettings().setControllable(false);
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
            .filter(entity -> entity.getTopologyEntityImpl().getEntityState() == EntityState.SUSPENDED)
            .map(entity -> {
                entity.getTopologyEntityImpl().getOrCreateAnalysisSettings().setControllable(false);
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
            .map(TopologyEntity::getTopologyEntityImpl)
            .filter(entity -> entity.getEntityState() == EntityState.MAINTENANCE)
            .filter(entity -> entity.getOrCreateTypeSpecificInfo().hasPhysicalMachine())
            .filter(entity -> entity.getOrCreateTypeSpecificInfo().getOrCreatePhysicalMachine().hasAutomationLevel())
            .filter(entity -> entity.getOrCreateTypeSpecificInfo().getOrCreatePhysicalMachine().getAutomationLevel() == AutomationLevel.FULLY_AUTOMATED)
            .map(entity -> {
                entity.getOrCreateAnalysisSettings().setControllable(false);
                logger.trace("Mark entity in maintenance {}({}) as not controllable",
                                entity.getDisplayName(), entity.getOid());
                return entity.getOid();
            })
            .collect(Collectors.toSet());
    }

    /**
     * Set controllable to false for Storages in maintenance mode.
     *
     * @param topology a topology graph.
     * @return a set of modified entity oids.
     */
    private Set<Long> setStoragesInMaintenanceToNonControllable(@Nonnull final TopologyGraph<TopologyEntity> topology) {
        return topology.entitiesOfType(EntityDTO.EntityType.STORAGE)
                .map(TopologyEntity::getTopologyEntityImpl)
                .filter(entity -> entity.getEntityState() == EntityState.MAINTENANCE)
                .map(entity -> {
                    entity.getOrCreateAnalysisSettings().setControllable(false);
                    logger.trace("Mark entity in maintenance {}({}) as not controllable",
                            entity.getDisplayName(), entity.getOid());
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
    private Set<Long> keepControllableFalseAfterExitingMaintenanceMode(final GraphWithSettings topology) {
        entityMaintenanceTimeDao.onEntities(topology.getTopologyGraph());
        Map<Long, Long> oids2exitMaintenances = entityMaintenanceTimeDao.getHostsThatLeftMaintenance();
        Set<Long> oidsToKeep = new HashSet<>();
        long now = clock.millis();

        logger.trace("Hosts that left maintenance: {}", oids2exitMaintenances);
        for (Map.Entry<Long, Long> oid2exitMaintenance : oids2exitMaintenances.entrySet()) {
            long oid = oid2exitMaintenance.getKey();
            Optional<TopologyEntity> host = topology.getTopologyGraph().getEntity(oid);
            if (!host.isPresent()) {
                // could be already removed from topology, will expire in maintenance table at max setting window
                continue;
            }
            Collection<Setting> hostSettings = topology.getSettingsForEntity(oid);
            if (CollectionUtils.isEmpty(hostSettings)) {
                continue;
            }
            long delay = (long)getNumericSettingValue(hostSettings, EntitySettingSpecs.DrsMaintenanceProtectionWindow);
            if (delay > 0 && Instant.ofEpochMilli(now).minus(delay, ChronoUnit.MINUTES).compareTo(
                            Instant.ofEpochSecond(oid2exitMaintenance.getValue())) < 0) {
                logger.trace("Keeping controllable false for host that left maintenance {}", host.get());
                host.get().getTopologyEntityImpl().getOrCreateAnalysisSettings().setControllable(false);
                oidsToKeep.add(oid);
            }
        }
        return oidsToKeep;
    }

    private static double getNumericSettingValue(Collection<Setting> settings,
                    EntitySettingSpecs spec) {
        for (Setting setting : settings) {
            if (spec.getSettingName().equals(setting.getSettingSpecName())
                            && setting.hasNumericSettingValue()) {
                return setting.getNumericSettingValue().getValue();
            }
        }
        return spec.getNumericDefault();
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
            .map(oid -> topology.getEntity(oid))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(TopologyEntity::getTopologyEntityImpl)
            .forEach(entityBuilder -> {
                if (entityBuilder.getOrCreateAnalysisSettings().getSuspendable()) {
                    // It's currently suspendable, and about to be marked
                    // non-suspendable.
                    numModified.incrementAndGet();
                }
                entityBuilder.getOrCreateAnalysisSettings().setSuspendable(false);
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
                .map(oid -> topology.getEntity(oid))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(TopologyEntity::getTopologyEntityImpl)
                // Set flag only for VMs and DBSs
                .filter(entityBuilder -> {
                    int entityType = entityBuilder.getEntityType();
                    return EntityType.VIRTUAL_MACHINE.getValue() == entityType
                            || EntityType.DATABASE_SERVER.getValue() == entityType;
                })
                .forEach(entityBuilder -> {
                    if (entityBuilder.getOrCreateAnalysisSettings().getIsEligibleForScale()) {
                        // It's currently eligible for scale, and about to be marked
                        // ineligible.
                        numModified.incrementAndGet();
                    }
                    entityBuilder.getOrCreateAnalysisSettings().setIsEligibleForScale(false);
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
                .map(oid -> topology.getEntity(oid))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(TopologyEntity::getTopologyEntityImpl)
                // Set flag only for VMs
                .filter(entityBuilder -> entityBuilder.getEntityType() == EntityType.VIRTUAL_MACHINE.getValue())
                .map(entityBuilder -> {
                    entityBuilder.getOrCreateAnalysisSettings().setIsEligibleForResizeDown(false);
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
            .filter(entity -> entity.getTopologyEntityImpl().getEntityState() == EntityState.MAINTENANCE)
            .flatMap(entity -> entity.getConsumers().stream())
            .filter(entity -> entity.getEntityType() == EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE)
            .map(entity -> {
                entity.getTopologyEntityImpl().getCommoditySoldListImplList()
                    .forEach(commSold -> commSold.setIsResizeable(false));
                logger.trace("Mark VM {} as not resizable", entity);
                return entity.getOid();
            })
            .collect(Collectors.toSet());
    }
}
