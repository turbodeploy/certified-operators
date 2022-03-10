/*
 * (C) Turbonomic 2019.
 */

package com.vmturbo.topology.processor.group.settings.applicators;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTOREST.Action.PrerequisiteType;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.ComputeTierInfoView;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator.SingleSettingApplicator;

/**
 * {@link InstanceStoreSettingApplicator} applies instance store setting to VMs. In case there is at
 * least one VM with active Instance Store aware scaling setting, then it will additionally update
 * all compute tiers with appropriate sold commodities.
 */
@NotThreadSafe
public class InstanceStoreSettingApplicator extends SingleSettingApplicator {
    private static final Collection<Predicate<ComputeTierInfoView>> INSTANCE_STORE_PREDICATES =
                    ImmutableList.of(InstanceStoreCommoditiesCreator.INSTANCE_DISK_SIZE_PREDICATE,
                                    InstanceStoreCommoditiesCreator.INSTANCE_DISK_TYPE_PREDICATE,
                                    InstanceStoreCommoditiesCreator.INSTANCE_DISK_COUNTS_PREDICATE);

    private final Logger logger = LogManager.getLogger(getClass());
    private final TopologyGraph<TopologyEntity> topologyGraph;
    private final VmInstanceStoreCommoditiesCreator vmCommoditiesCreator;
    private final ComputeTierInstanceStoreCommoditiesCreator computeTierCommoditiesCreator;
    private boolean computeTiersCommoditiesCreated;
    private final TopologyInfo topologyInfo;

    /**
     * Creates {@link InstanceStoreSettingApplicator} instance.
     *
     * @param topologyGraph topology graph used to find related entities by theirs
     *                 oids.
     * @param vmCommoditiesCreator creates commodities for VM which is residing on
     *                 instance store aware template.
     * @param computeTierCommoditiesCreator creates commodities for tier instances
     *                 which are supporting instance store feature.
     * @param topologyInfo the topologyInfo associated with topology graph.
     */
    public InstanceStoreSettingApplicator(@Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                    @Nonnull VmInstanceStoreCommoditiesCreator vmCommoditiesCreator,
                    @Nonnull ComputeTierInstanceStoreCommoditiesCreator computeTierCommoditiesCreator,
                    @Nonnull TopologyInfo topologyInfo) {
        super(EntitySettingSpecs.InstanceStoreAwareScaling);
        this.topologyGraph = Objects.requireNonNull(topologyGraph);
        this.vmCommoditiesCreator = Objects.requireNonNull(vmCommoditiesCreator);
        this.computeTierCommoditiesCreator = Objects.requireNonNull(computeTierCommoditiesCreator);
        this.topologyInfo = topologyInfo;
    }

    /**
     * Applies setting for the specified entity. In case the setting is applicable for the entity.
     *
     * @param entity for which we want to try to apply the setting.
     * @param setting that we want to apply for the entity.
     */
    @Override
    protected void apply(@Nonnull TopologyEntityImpl entity, @Nonnull Setting setting) {
        // Currently only AWS VMs with "instance store aware scaling" enabled will be populated
        // with instance store related commodities including INSTANCE_DISK_SIZE, INSTANCE_DISK_TYPE
        // and INSTANCE_DISK_COUNT. On prem VMs and Azure VMs do not buy or sell them. Instead of
        // figuring out the exact cloud migration scenario, we can skip the instance store commodity
        // creation in all migration cases, e.g: on prem to AWS/Azure, AWS to Azure, Azure to AWS.
        if (TopologyDTOUtil.isCloudMigrationPlan(topologyInfo)) {
            return;
        }
        if (isApplicable(entity, setting)) {
            populateInstanceStoreCommodities(entity);
        }
    }

    /**
     * Checks whether specified setting is applicable for the entity (VM).
     *
     * @param entity for which we want to try to apply the setting.
     * @param setting that we want to apply for the entity.
     * @return {@code true} in case setting is applicable for entity, otherwise returns
     *                 {@code false}.
     */
    @VisibleForTesting
    public static boolean isApplicable(@Nonnull TopologyEntityImpl entity, @Nonnull Setting setting) {
        // Check whether entity has the proper type (supported for VMs only).
        if (entity.getEntityType() != EntityType.VIRTUAL_MACHINE.getNumber()) {
            return false;
        }
        // If local SSD based execution constraints are marked (only for GCP VMs with local SSDs),
        // then perform instance store aware based scaling, irrespective of the policy flag.
        final String executionConstraint = entity.getEntityPropertyMapMap()
                .get(TopologyDTOUtil.EXECUTION_CONSTRAINT_PROPERTY);
        if (PrerequisiteType.LOCAL_SSD_ATTACHED.name().equals(executionConstraint)) {
            return true;
        }
        // Otherwise, (e.g for AWS VMs) check that policy setting is active and enabled.
        return setting.hasBooleanSettingValue() && setting.getBooleanSettingValue().getValue();
    }

    private void populateInstanceStoreCommodities(@Nonnull TopologyEntityImpl entity) {
        final CommoditiesBoughtFromProviderImpl computeTierProvider =
                getProvider(entity, EntityType.COMPUTE_TIER);
        if (computeTierProvider == null) {
            // Normal use-case for on-prem VMs
            return;
        }
        final Optional<ComputeTierInfoView> ct =
                topologyGraph.getEntity(computeTierProvider.getProviderId())
                        .map(TopologyEntity::getTopologyEntityImpl)
                        .filter(InstanceStoreSettingApplicator::hasComputeTierInfo)
                        .map(item -> item.getTypeSpecificInfo().getComputeTier());
        ct.ifPresent(computeTierInfo -> {
            final Integer usedEphemeralDisks = getUsedEphemeralDisks(entity, ct.get());
            final Collection<CommodityBoughtImpl> boughtCommodities = vmCommoditiesCreator
                    .create(CommoditiesBoughtFromProviderImpl::getCommodityBoughtImplList,
                            computeTierProvider, computeTierInfo, usedEphemeralDisks);
            if (!boughtCommodities.isEmpty()) {
                boughtCommodities.forEach(computeTierProvider::addCommodityBought);
                if (!computeTiersCommoditiesCreated) {
                    createSoldCommodities();
                }
            }
        });
    }

    /**
     * Used to get number of ephemeral disks currently attached to the VM.
     *
     * @param vmEntity Tries to get from VM's TopologyEntityImpl 'numEphemeralStorages' field.
     * @param computeTierInfo If not available from VM, gets it from Compute Tier's info.
     * @return Ephemeral storage used count, or 0.
     */
    @VisibleForTesting
    public static int getUsedEphemeralDisks(@Nonnull final TopologyEntityImpl vmEntity,
            @Nonnull final ComputeTierInfoView computeTierInfo) {
        final Integer vmEphemeralDisks = vmEntity.hasTypeSpecificInfo()
                && vmEntity.getTypeSpecificInfo().hasVirtualMachine()
                && vmEntity.getTypeSpecificInfo().getVirtualMachine().hasNumEphemeralStorages()
                ? vmEntity.getTypeSpecificInfo().getVirtualMachine().getNumEphemeralStorages()
                : null;
        if (vmEphemeralDisks != null) {
            return vmEphemeralDisks;
        }
        // Try and get it from computeTierInfo, this is more for backward compatibility if
        // 'numEphemeralStorages' value is not provided for the VM from probe.
        if (computeTierInfo.getInstanceDiskCountsCount() > 0) {
            return computeTierInfo.getInstanceDiskCounts(0);
        }
        return 0;
    }

    private void createSoldCommodities() {
        final Stream<TopologyEntity> computeTiers =
                topologyGraph.entitiesOfType(EntityType.COMPUTE_TIER);
        computeTiers.map(TopologyEntity::getTopologyEntityImpl).forEach(ct -> {
            final Collection<CommoditySoldImpl> ctCommodities = computeTierCommoditiesCreator
                    .create(TopologyEntityImpl::getCommoditySoldListImplList, ct,
                            ct.getTypeSpecificInfo().getComputeTier(), null);
            ctCommodities.forEach(ct::addCommoditySoldList);
        });
        computeTiersCommoditiesCreated = true;
    }

    /**
     * Checks whether entity has a compute tier info related to it or not.
     *
     * @param entity which we want to check availability of the compute tier info.
     * @return {@code true} in case entity has compute tier info associated with it,
     *                 otherwise returns {@code false}.
     */
    private static boolean hasComputeTierInfo(@Nonnull TopologyEntityImpl entity) {
        // Check that entity has type specific info
        if (!entity.hasTypeSpecificInfo()) {
            return false;
        }
        // Checks that entity has appropriate type specific info instance
        if (!entity.getTypeSpecificInfo().hasComputeTier()) {
            return false;
        }
        final ComputeTierInfoView computeTier = entity.getTypeSpecificInfo().getComputeTier();
        return INSTANCE_STORE_PREDICATES.stream().allMatch(p -> p.test(computeTier));
    }

    @Nullable
    private CommoditiesBoughtFromProviderImpl getProvider(@Nonnull TopologyEntityImpl entity,
                    @Nonnull EntityType providerType) {
        final Collection<CommoditiesBoughtFromProviderImpl> boughtFromComputeTiers =
                        entity.getCommoditiesBoughtFromProvidersImplList().stream()
                                        .filter(provider -> provider.getProviderEntityType()
                                                        == providerType.getNumber())
                                        .collect(Collectors.toSet());
        final int numberOfComputeTierProviders = boughtFromComputeTiers.size();
        if (numberOfComputeTierProviders == 0) {
            // Normal use-case for on-prem VMs
            logger.debug("There are no '{}' providers for '{}'", providerType, entity.getOid());
            return null;
        }
        final CommoditiesBoughtFromProviderImpl result =
                        boughtFromComputeTiers.iterator().next();
        if (numberOfComputeTierProviders > 1) {
            logger.warn("There are more than one '{}' provider for '{}', using first one '{}'.",
                            providerType, entity.getOid(), result.getProviderId());
        }
        return result;
    }
}
