package com.vmturbo.topology.processor.topology;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.COMPUTE_TIER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_TIER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_TIER_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetMembersRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.MigrationReference;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.OSMigration;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.mediation.hybrid.cloud.common.OsType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.processor.entity.EntityNotFoundException;
import com.vmturbo.topology.processor.group.ResolvedGroup;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.policy.application.PlacementPolicy;
import com.vmturbo.topology.processor.group.settings.SettingPolicyEditor;
import com.vmturbo.topology.processor.topology.CloudStorageMigrationHelper.IopsToStorageRatios;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;

/**
 * Utility class with some helper functions used during Cloud migration plan.
 */
public class CloudMigrationPlanHelper {
    private final Logger logger = LogManager.getLogger();

    /**
     * Policy group description.
     */
    private static final String POLICY_GROUP_DESCRIPTION = "Cloud migration policy generated group";

    /**
     * Service for group resolution requests.
     */
    final GroupServiceBlockingStub groupServiceBlockingStub;

    /**
     * For Cloud migration allocation (Lift_n_Shift) plan, we only support GP2 & managed_premium.
     */
    private static final Set<String> ALLOCATION_PLAN_KEEP_STORAGE_TIERS = ImmutableSet.of(
            "GP2",
            "MANAGED_PREMIUM"
    );

    /**
     * Non-EBS AWS storage types are skipped for Cloud migration consumption plan.
     */
    private static final Set<String> CONSUMPTION_PLAN_SKIP_STORAGE_TIERS = ImmutableSet.of(
            "HDD",
            "SSD"
    );

    /**
     * Which commodities don't apply to cloud. During cloud migration, if an on-prem VM is buying
     * any of these commodities, we skip these from the topologyEntity for that VM, so that we
     * will not get into migration problem as cloud tiers don't sell these commodity types.
     * Similar to Classic's MarketConstants::commClassToSkipForCrossCloud set.
     */
    @VisibleForTesting
    static final Set<CommodityType> COMMODITIES_TO_SKIP = ImmutableSet.of(
            CommodityType.Q1_VCPU,
            CommodityType.Q2_VCPU,
            CommodityType.Q3_VCPU,
            CommodityType.Q4_VCPU,
            CommodityType.Q5_VCPU,
            CommodityType.Q6_VCPU,
            CommodityType.Q7_VCPU,
            CommodityType.Q8_VCPU,
            CommodityType.Q16_VCPU,
            CommodityType.Q32_VCPU,
            CommodityType.Q64_VCPU,
            CommodityType.QN_VCPU,
            CommodityType.SWAPPING,
            CommodityType.BALLOONING,
            CommodityType.FLOW,
            CommodityType.HOT_STORAGE,
            CommodityType.CPU_PROVISIONED,
            CommodityType.MEM_PROVISIONED,
            CommodityType.INSTANCE_DISK_SIZE,
            CommodityType.INSTANCE_DISK_TYPE
    );

    /**
     * Certain providers (mostly on-prem) we need to do some special processing - making sure
     * they are controllable, non-suspendable etc.
     */
    private static final Set<EntityType> PROVIDER_TYPES = ImmutableSet.of(
            PHYSICAL_MACHINE,
            STORAGE,
            STORAGE_TIER,
            VIRTUAL_VOLUME);

    /**
     * Providers from which LICENSE_ACCESS can be bought. Physical Machine only would
     * buy it in the case of on-prem VMs which are migrating to the cloud.
     */
    private static final Set<EntityType> LICENSE_PROVIDER_TYPES = ImmutableSet.of(
            COMPUTE_TIER,
            PHYSICAL_MACHINE);

    /**
     * IOPS to Storage ratios: used for adjusting storage amount based on IOPS.
     */
    private IopsToStorageRatios iopsToStorageRatios;

    /**
     * Constructor called by migration stage.
     *
     * @param groupServiceBlockingStub For group resolution source entities.
     */
    CloudMigrationPlanHelper(@Nonnull final GroupServiceBlockingStub groupServiceBlockingStub) {
        this.groupServiceBlockingStub = groupServiceBlockingStub;
    }

    /**
     * Main entry point, called from stage during pipeline execution. Updates/filters commodities
     * for source and target entities, sets up policy groups, removes non-migrating workloads.
     *
     * @param context Pipeline context.
     * @param inputGraph Graph coming in from previous pipeline stages.
     * @param planScope Scope of the plan.
     * @param changes Migration changes specified by user.
     * @return Output graph, mostly same as input, except non-migrating workloads/volumes removed.
     * @throws PipelineStageException Thrown on stage execution issue.
     */
    public TopologyGraph<TopologyEntity> executeStage(
            @Nonnull final TopologyPipelineContext context,
            @Nonnull final TopologyGraph<TopologyEntity> inputGraph,
            @Nullable final PlanScope planScope,
            @Nonnull final List<ScenarioChange> changes) throws PipelineStageException {
        if (!isApplicable(context, planScope)) {
            return inputGraph;
        }
        TopologyMigration migrationChange = changes
                .stream()
                .filter(ScenarioChange::hasTopologyMigration)
                .map(ScenarioChange::getTopologyMigration)
                .findFirst()
                .orElseThrow(() -> new PipelineStageException("Missing cloud migration change"));

        TopologyGraph<TopologyEntity> outputGraph = removeNonMigratingWorkloads(context, inputGraph,
                migrationChange);

        // Prepare source entities for migration.
        iopsToStorageRatios = CloudStorageMigrationHelper.populateMaxIopsRatioAndCapacity(inputGraph);
        prepareEntities(context, outputGraph, migrationChange);
        prepareProviders(outputGraph, context.getTopologyInfo());
        savePolicyGroups(context, outputGraph, migrationChange);

        if (migrationChange.getDestinationEntityType()
            .equals(TopologyMigration.DestinationEntityType.VIRTUAL_MACHINE)) {
            context.addSettingPolicyEditor(new CloudMigrationSettingsPolicyEditor(
                context.getSourceEntities()));
        }

        return outputGraph;
    }

    /**
     * Checks whether the plan is of cloud migration type.
     *
     * @param context Pipeline context.
     * @param planScope Scope info for plan.
     * @return True if this is a valid migration plan.
     */
    private boolean isApplicable(@Nonnull final TopologyPipelineContext context,
                                 @Nullable final PlanScope planScope) {
        if (planScope == null || planScope.getScopeEntriesList().isEmpty()) {
            return false;
        }
        return TopologyDTOUtil.isCloudMigrationPlan(context.getTopologyInfo());
    }

    /**
     * Prepares entities that are being migrated. Checks to make sure all are in topology map.
     * Sets shopAlone to false for them, will get set to true later after market fixes to support
     * shopTogether properly. Updates bought commodities.
     *
     * @param context Plan pipeline context containing source entities being migrated.
     * @param graph Topology graph.
     * @param migrationChange User specified migration scenario change.
     * @throws PipelineStageException Thrown when entity lookup by oid fails.
     */
    private void prepareEntities(@Nonnull final TopologyPipelineContext context,
                                @Nonnull final TopologyGraph<TopologyEntity> graph,
                                @Nonnull final TopologyMigration migrationChange)
            throws PipelineStageException {
        Set<Long> sourceEntities = context.getSourceEntities();

        final Map<OSType, OsType> licenseCommodityKeyByOS = computeLicenseCommodityKeysByOS(
                migrationChange);

        for (Long oid : sourceEntities) {
            Optional<TopologyEntity> optionalEntity = graph.getEntity(oid);
            if (!optionalEntity.isPresent()) {
                throw new PipelineStageException("Could not look up source entity " + oid
                        + " for cloud migration.");
            }
            final TopologyEntity entity = optionalEntity.get();
            final TopologyEntityDTO.Builder builder = entity.getTopologyEntityDtoBuilder();

            // It could be overridden in settingsApplicator
            builder.getAnalysisSettingsBuilder().setShopTogether(true);

            // Analysis needs to treat the entity as if it's a cloud entity for purposes of
            // applying template exclusions, obtaining pricing, etc.
            if (builder.getEnvironmentType().equals(EnvironmentType.ON_PREM)
                    && builder.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                // Set environment type of associated virtual volumes of on-prem VMs to CLOUD.
                entity.getOutboundAssociatedEntities().stream()
                        .filter(e -> e.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                        .map(TopologyEntity::getTopologyEntityDtoBuilder)
                        .forEach(b -> b.setEnvironmentType(EnvironmentType.CLOUD));
                builder.setEnvironmentType(EnvironmentType.CLOUD);
            }

            // Remove non-applicable commodities first here, before other stages add some bought
            // commodities like segmentation.
            prepareBoughtCommodities(builder, context.getTopologyInfo(), true);

            // Add coupon commodity to allow existing RIs to be utilized
            addCouponCommodity(entity);

            // Add license access commodities for VMs being migrated.
            updateLicenseAccessCommodities(entity, licenseCommodityKeyByOS);
        }
    }

    /**
     * In order to allow a migrating workload to utilize an existing reserved instance, a coupon
     * commodity must be on it's shopping list. This function recreates the
     * CommoditiesBoughtFromProviders list of each cloud-bound migrating VM to include that
     * commodity if it's not already present.
     *
     * @param vm the {@link TopologyEntity} source entity being processed
     */
    private void addCouponCommodity(final TopologyEntity vm) {
        if (vm.getEntityType() != VIRTUAL_MACHINE_VALUE) {
            // Only updating coupon commodity for VMs
            return;
        }

        final TopologyEntityDTO.Builder vmBuilder = vm.getTopologyEntityDtoBuilder();
        final List<CommoditiesBoughtFromProvider> originalCommBoughtGroupings =
                vmBuilder.getCommoditiesBoughtFromProvidersList();
        final List<CommoditiesBoughtFromProvider> newCommBoughtGroupings = Lists.newArrayList();

        originalCommBoughtGroupings.forEach(commoditiesBoughtFromProvider -> {
            // first, verify that we're dealing with a compute provider, and that this VM isn't
            // already buying a coupon commodity from it
            if (EntityType.PHYSICAL_MACHINE_VALUE == commoditiesBoughtFromProvider.getProviderEntityType()
                    && !commoditiesBoughtFromProvider.getCommodityBoughtList().stream()
                        .anyMatch(commodityBoughtDTO -> CommodityType.COUPON.equals(commodityBoughtDTO.getCommodityType()))) {
                //Add CouponCommodity
                CommoditiesBoughtFromProvider newCommoditiesBoughtFromProvider =
                        CommoditiesBoughtFromProvider.newBuilder(commoditiesBoughtFromProvider)
                        .addCommodityBought(CommodityBoughtDTO.newBuilder()
                                .setCommodityType(TopologyDTO.CommodityType.newBuilder().setType(
                                        CommodityType.COUPON_VALUE).build())
                                .setPeak(0)
                                .setUsed(0)
                                .build())
                        .build();
                newCommBoughtGroupings.add(newCommoditiesBoughtFromProvider);
            } else {
                newCommBoughtGroupings.add(commoditiesBoughtFromProvider);
            }
        });

        vmBuilder.clearCommoditiesBoughtFromProviders();
        vmBuilder.addAllCommoditiesBoughtFromProviders(newCommBoughtGroupings);
    }

    /**
     * Creates license access commodity for the VM based on user configuration.
     *
     * @param vm VM being migrated.
     * @param licenseCommodityKeyByOS Map of license key to OS type.
     */
    private void updateLicenseAccessCommodities(final TopologyEntity vm,
                                                final Map<OSType, OsType> licenseCommodityKeyByOS) {
        if (vm.getEntityType() != VIRTUAL_MACHINE_VALUE) {
            // Only updating licenses for VMs.
            return;
        }
        OsType licenseCommodityKey = licenseCommodityKeyByOS.getOrDefault(
                vm.getTypeSpecificInfo().getVirtualMachine().getGuestOsInfo().getGuestOsType(),
                OsType.LINUX
        );

        updateAccessCommodityForVmAndProviders(vm, LICENSE_PROVIDER_TYPES,
                CommodityType.LICENSE_ACCESS, licenseCommodityKey);

        // TODO: this would also be the place, for migrations to Azure, where
        // if the migrating VM is not buying IO_THROUGHPUT from storage,
        // to add it in proportion to STORAGE_ACCESS so that we get reasonable
        // cost estimates for Ultra Disk.
    }

    /**
     * Updates VM entity by adding the access commodity of the specified commodityType and key,
     * for the matching providerTypes.
     *
     * @param vm VM entity to add access key for.
     * @param providerTypes Types of providers.
     * @param commodityType Commodity type, e.g LICENSE_ACCESS
     * @param osType New key to use for the access commodity being added.
     */
    private void updateAccessCommodityForVmAndProviders(@Nonnull TopologyEntity vm,
                                                        @Nonnull Set<EntityType> providerTypes,
                                                        @Nonnull CommodityType commodityType,
                                                        @Nonnull OsType osType) {
        TopologyEntityDTO.Builder vmBuilder = vm.getTopologyEntityDtoBuilder();

        List<CommoditiesBoughtFromProvider> originalCommBoughtGroupings =
                vmBuilder.getCommoditiesBoughtFromProvidersList();
        List<CommoditiesBoughtFromProvider> newCommBoughtGroupings = new ArrayList<>();
        boolean updated = false;

        for (CommoditiesBoughtFromProvider commBoughtGrouping : originalCommBoughtGroupings) {
            if (providerTypes.contains(EntityType.forNumber(
                    commBoughtGrouping.getProviderEntityType()))) {
                // Bought commodity key will be like 'Linux' which is what compute tier sells.
                newCommBoughtGroupings.add(updateAccessCommodityKey(
                        commBoughtGrouping, commodityType, osType.getName()));
                updated = true;
            } else {
                // Add this commBoughtGrouping as-is, without any updates.
                newCommBoughtGroupings.add(commBoughtGrouping);
            }
        }
        if (updated) {
            // Do clear at the end once we know we need to do it, otherwise in some cases,
            // when scoped to DC etc., original commodity list becomes empty, it is probably
            // affected by whether build() has been called on it or not.
            vmBuilder.clearCommoditiesBoughtFromProviders();
            vmBuilder.addAllCommoditiesBoughtFromProviders(newCommBoughtGroupings);

            // We are storing the DTO equivalent 'OSType' in the property, that is read from TC.
            vmBuilder.putEntityPropertyMap(StringConstants.PLAN_NEW_OS_TYPE_PROPERTY,
                    osType.getDtoOS().name());
            // Set display name for OS type for projected entities.
            vmBuilder.putEntityPropertyMap(StringConstants.PLAN_NEW_OS_NAME_PROPERTY,
                    osType.getDisplayName());
        }
    }

    /**
     * Adds a new access commodity with specified key to the commBoughtGrouping.
     *
     * @param commoditiesBoughtFromProvider CommBoughtGrouping to add access commodity to.
     * @param commodityType Type of access commodity to add.
     * @param newKey Key of access commodity.
     * @return Updated commBoughtGrouping with key added.
     */
    private CommoditiesBoughtFromProvider updateAccessCommodityKey(
            @Nonnull CommoditiesBoughtFromProvider commoditiesBoughtFromProvider,
            @Nonnull CommodityType commodityType,
            @Nonnull String newKey) {

        CommoditiesBoughtFromProvider.Builder newCommoditiesBoughtFromProviderBuilder
                = commoditiesBoughtFromProvider.toBuilder().clearCommodityBought();

        boolean foundCommodity = false;

        for (CommodityBoughtDTO commodityBought
                : commoditiesBoughtFromProvider.getCommodityBoughtList()) {

            // If this is the commodity type we're looking to change, rebuild it with the new key
            if (commodityBought.getCommodityType().getType() == commodityType.getNumber()) {
                commodityBought = commodityBought.toBuilder().setCommodityType(
                        commodityBought.getCommodityType().toBuilder().setKey(newKey)
                ).build();

                foundCommodity = true;
            }

            newCommoditiesBoughtFromProviderBuilder.addCommodityBought(commodityBought);
        }

        if (!foundCommodity) {
            // Add commodity since it wasn't present
            newCommoditiesBoughtFromProviderBuilder.addCommodityBought(
                    CommodityBoughtDTO.newBuilder().setCommodityType(
                            TopologyDTO.CommodityType.newBuilder()
                                    .setType(commodityType.getNumber())
                                    .setKey(newKey)
                    )
            );
        }

        return newCommoditiesBoughtFromProviderBuilder.build();
    }

    /**
     * Updates providers and sets their bought and sold commodities as needed.
     *
     * @param graph Topology graph to look for provider types.
     * @param topologyInfo Plan topology info.
     */
    private void prepareProviders(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                         @Nonnull final TopologyInfo topologyInfo) {
        PROVIDER_TYPES
                .stream()
                .flatMap(graph::entitiesOfType)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .forEach(providerDtoBuilder -> {

                    // Set suspendable false, so we don't see suspend actions for these providers.
                    providerDtoBuilder.getAnalysisSettingsBuilder().setSuspendable(false);

                    // Need to set movable/scalable true for provider commBought.
                    prepareBoughtCommodities(providerDtoBuilder, topologyInfo, true);
                    prepareSoldCommodities(providerDtoBuilder);
                });
    }

    /**
     * Certain providers don't have capacity set correctly for some commodities, this ensures
     * that capacity is set to high value if it is found to be 0. This is needed so entity
     * validator stage later doesn't make the provider non-controllable.
     *
     * @param dtoBuilder Provider DTO being updated.
     */
    // TODO: Temporary, not needed once these issues are fixed in probe, with real-time VV support.
    void prepareSoldCommodities(@Nonnull final TopologyEntityDTO.Builder dtoBuilder) {
        if (dtoBuilder.getEntityType() == VIRTUAL_VOLUME_VALUE) {
            // For vol providers, storage_access commodity doesn't have capacity, so set it.
            // Not doing this makes volumes non-controllable later in EntityValidator.
            // Hopefully this block can be removed when the probe issue is fixed.
            dtoBuilder.getCommoditySoldListBuilderList()
                    .forEach(commSoldBuilder -> {
                        if (commSoldBuilder.getCapacity() == 0d) {
                            commSoldBuilder.setCapacity(1E9);
                        }
                    });
        }
    }

    /**
     * Prepares the CommoditiesBoughtFromProvider for either the source entities being migrated,
     * or providers. Specifically:
     * 1. For source entities, skips non-applicable commodities and access commodities.
     * 2. If CommoditiesBoughtFromProvider has movable/scalable false, then sets it to true.
     * 3. For providers also, like Virtual Volumes in cloud->cloud migration, we need to skip
     * non-applicable commodities. This is needed as volumes buy commodities like
     * 'STORAGE_CLUSTER|Group::MANAGED_PREMIUM', which need to be filtered out so that the volume
     * can be migrated to new CSP's storage tier. When we use collapsed trader later in TC for
     * volumes, we take commBoughtGrouping b/w Volume -> StorageTier, but then we add any missing
     * bought commodities b/w VM -> Volume commBoughtProvider. This was needed so that we can
     * still have segmentation commodities that we had created earlier in the pipeline.
     * Not doing such filtering for provider commodities causes reconfigure action instead of
     * expected move migration action for storage volumes.
     *
     * @param dtoBuilder Source entity or provider DTO being updated.
     * @param topologyInfo Info about plan topology.
     * @param isConsumer true if updating commBought of the consumer (i.e. VM) false if updating the
     *                   provider of the VM.
     */
    @VisibleForTesting
    void prepareBoughtCommodities(@Nonnull final TopologyEntityDTO.Builder dtoBuilder,
                                  @Nonnull final TopologyInfo topologyInfo,
                                  boolean isConsumer) {
        List<CommoditiesBoughtFromProvider> newCommoditiesByProvider = new ArrayList<>();
        // Go over grouping of comm bought along with their providers.
        for (CommoditiesBoughtFromProvider commBoughtGrouping
                : dtoBuilder.getCommoditiesBoughtFromProvidersList()) {

            // We need to skip some on-prem specific commodities bought to allow cloud migration.
            List<CommodityBoughtDTO> commoditiesToInclude = getUpdatedCommBought(
                    commBoughtGrouping, topologyInfo, dtoBuilder.getOid(), isConsumer);
            if (commoditiesToInclude.size() == 0) {
                // Don't keep this group if there are no valid bought commodities from it.
                continue;
            }
            CommoditiesBoughtFromProvider.Builder newCommBoughtGrouping =
                    CommoditiesBoughtFromProvider.newBuilder(commBoughtGrouping);
            newCommBoughtGrouping.clearCommodityBought();
            newCommBoughtGrouping.addAllCommodityBought(commoditiesToInclude);

            // Verify grouping is movable and scalable, otherwise shopping list will not be.
            boolean isMovable = commBoughtGrouping.hasMovable()
                    && commBoughtGrouping.getMovable();
            boolean isScalable = commBoughtGrouping.hasScalable()
                    && commBoughtGrouping.getScalable();
            if (!isMovable || !isScalable) {
                newCommBoughtGrouping
                        .setMovable(true)
                        .setScalable(true);
            }
            newCommoditiesByProvider.add(newCommBoughtGrouping.build());
        }
        dtoBuilder.clearCommoditiesBoughtFromProviders();
        dtoBuilder.addAllCommoditiesBoughtFromProviders(newCommoditiesByProvider);
    }

    /**
     * Gets updated list of CommBoughtDTO, after filtering out those that need to be skipped.
     *
     * @param commBoughtGrouping Grouping to look for commBoughtDTO in.
     * @param topologyInfo Plan topology info.
     * @param entityOid entity OID
     * @param isConsumer true if updating commBought of the consumer (i.e. VM) false if updating the
     *                   provider of the VM.
     * @return Updated list of only applicable CommBoughtDTOs.
     */
    @Nonnull
    private List<CommodityBoughtDTO> getUpdatedCommBought(
            @Nonnull final CommoditiesBoughtFromProvider commBoughtGrouping,
            @Nonnull final TopologyInfo topologyInfo,
            final long entityOid,
            boolean isConsumer) {
        List<CommodityBoughtDTO> commoditiesToInclude = new ArrayList<>();
        for (CommodityBoughtDTO dtoBought : commBoughtGrouping.getCommodityBoughtList()) {
            CommodityType commodityType = CommodityType.forNumber(dtoBought
                    .getCommodityType().getType());
            // Skip any commodities from the known set or if it is an access commodity
            // (with keys)
            if (COMMODITIES_TO_SKIP.contains(commodityType)
                    || dtoBought.getCommodityType().hasKey()) {
                continue;
            }
            if (commodityType == CommodityType.IO_THROUGHPUT
                    || commodityType == CommodityType.STORAGE_PROVISIONED) {
                // Disable the IO_THROUGHPUT and STORAGE_PROVISIONED commodity because it is not
                // used in migration decision.
                CommodityBoughtDTO dtoBoughtUpdated = CommodityBoughtDTO
                        .newBuilder(dtoBought)
                        .setActive(false)
                        .build();
                commoditiesToInclude.add(dtoBoughtUpdated);
            } else if (isConsumer && commodityType == CommodityType.STORAGE_ACCESS) {
                // Use historical max value for storage access.
                CommodityBoughtDTO.Builder commodityBoughtDTO = CloudStorageMigrationHelper.getHistoricalMaxIOPS(dtoBought);
                if (!TopologyDTOUtil.isResizableCloudMigrationPlan(topologyInfo)) {
                    // Disable Storage Access commodity for Lift and Shift plan so it can always
                    // fit in GP2 or Azure Managed Premium.
                    commodityBoughtDTO.setActive(false);
                }
                commoditiesToInclude.add(commodityBoughtDTO.build());
            } else if (isConsumer && commodityType == CommodityType.STORAGE_AMOUNT) {
                if (TopologyDTOUtil.isResizableCloudMigrationPlan(topologyInfo)) {
                    // Assign storage provisioned used value for storage amount.
                    // Also adjust storage amount based on IOPS value if necessary.
                    commoditiesToInclude.add(CloudStorageMigrationHelper
                            .adjustStorageAmountForCloudMigration(dtoBought, commBoughtGrouping,
                                    iopsToStorageRatios, entityOid));
                } else {
                    // Assign storage provisioned used value for storage amount
                    commoditiesToInclude.add(CloudStorageMigrationHelper
                            .updateStorageAmountCommodityBought(dtoBought, commBoughtGrouping));
                }
            } else {
                commoditiesToInclude.add(dtoBought);
            }
        }
        return commoditiesToInclude;
    }

    /**
     * Whether to include the cloud storage tier in the destination set of placement policy.
     * Certain storage tiers need to be skipped from cloud migration plans.
     * For Lift_n_Shift: Skip all except GP2 and ManagedPremium.
     * For Optimized: Skip HDD and SSD.
     *
     * @param cloudStorageTier Cloud storage tier entity.
     * @param topologyInfo TopologyInfo having info about plan sub type.
     * @return True if this storage tier needs to be included, e.g GP2 for Allocation plan.
     */
    private boolean includeCloudStorageTier(@Nonnull final TopologyEntity cloudStorageTier,
                                                  @Nonnull final TopologyInfo topologyInfo) {
        // Don't see another way to get storage type, other than looking at display name.
        String storageType = cloudStorageTier.getTopologyEntityDtoBuilder().getDisplayName();
        if (TopologyDTOUtil.isResizableCloudMigrationPlan(topologyInfo)) {
            // Optimized plan (with resize enabled). We are skipping HDD/SSD etc, so
            // need to return false in those cases.
            return !CONSUMPTION_PLAN_SKIP_STORAGE_TIERS.contains(storageType);
        }
        return ALLOCATION_PLAN_KEEP_STORAGE_TIERS.contains(storageType);
    }

    /**
     * Removes workloads (VM/DB/DBS) and volumes that don't apply for migration, so that we don't
     * unnecessarily send them to market. This is the entities in the scoped target regions, as
     * we use generic OCP scoping in earlier stages which ends up picking all entities in target
     * region as well, so we need to filter them out here in migration stage.
     *
     * @param context Pipeline context.
     * @param graph Topology graph.
     * @param migrationChange User specified migration scenario change.
     * @return Updated topology graph with non-migrating workloads filtered out.
     */
    @Nonnull
    private TopologyGraph<TopologyEntity> removeNonMigratingWorkloads(
            @Nonnull final TopologyPipelineContext context,
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final TopologyMigration migrationChange) {
        if (!migrationChange.getRemoveNonMigratingWorkloads()) {
            return graph;
        }

        context.setSourceEntities(removeInactiveEntities(graph, context.getSourceEntities()));

        final Set<Long> sourceEntities = context.getSourceEntities();

        // We look at the workload entities being migrated, and skip any workload not in that set.
        // Also look for attached volumes to migrating workloads, and skip any other volumes.
        final Map<EntityType, Set<TopologyEntity>> filteredEntityByType = new HashMap<>();
        for (EntityType type : TopologyDTOUtil.WORKLOAD_TYPES) {
            filteredEntityByType.put(type, graph.entitiesOfType(type)
                    .filter(e -> sourceEntities.contains(e.getOid()))
                    .collect(Collectors.toSet()));
        }
        // Get volumes attached to the VMs that are in scope.
        filteredEntityByType.put(VIRTUAL_VOLUME,
                filteredEntityByType.get(EntityType.VIRTUAL_MACHINE)
                        .stream()
                        .flatMap(e -> Stream.concat(e.getOutboundAssociatedEntities().stream(),
                                e.getProviders().stream()))
                        .filter(e -> e.getEntityType() == VIRTUAL_VOLUME_VALUE)
                        .collect(Collectors.toSet()));

        final Set<EntityType> allEntityTypes = graph.entityTypes()
                .stream()
                .map(EntityType::forNumber)
                .collect(Collectors.toSet());
        final Long2ObjectMap<Builder> resultEntityMap = new Long2ObjectOpenHashMap<>();
        for (EntityType type : allEntityTypes) {
            Set<TopologyEntity> filteredEntities = filteredEntityByType.get(type);
            if (filteredEntities != null) {
                // Only pick up entities that are in source scope.
                resultEntityMap.putAll(filteredEntities
                        .stream()
                        .map(TopologyEntity::getTopologyEntityDtoBuilder)
                        .collect(Collectors.toMap(TopologyEntityDTO.Builder::getOid,
                                TopologyEntity::newBuilder)));
            } else {
                // Pick all entities of this type as-is.
                resultEntityMap.putAll(graph.entitiesOfType(type)
                        .map(TopologyEntity::getTopologyEntityDtoBuilder)
                        .collect(Collectors.toMap(TopologyEntityDTO.Builder::getOid,
                                TopologyEntity::newBuilder)));
            }
        }
        return new TopologyGraphCreator<>(resultEntityMap).build();
    }

    /**
     * Creates and saves policy groupings into the plan pipeline context, so that they can later
     * be used by PolicyManager to create segmentation policies for workoads and volumes, needed
     * for migration to work (to force workloads to new target compute/storage tiers).
     *
     * @param context Plan pipeline context
     * @param graph Topology graph.
     * @param migrationChange User specified migration scenario change.
     */
    private void savePolicyGroups(@Nonnull final TopologyPipelineContext context,
                                 @Nonnull final TopologyGraph<TopologyEntity> graph,
                                 @Nonnull final TopologyMigration migrationChange) {
        context.clearPolicyGroups();

        saveWorkloadPolicyGroups(context, graph, migrationChange);
        saveVolumePolicyGroups(context, graph);
    }

    /**
     * Gets a placement policy to place the workloads (VM/DBS) being migrated, onto the cloud tiers.
     * Create groups corresponding to the source and destination arguments of the
     * {@param MigrateObjectApiDTO}, and add cloud migration policies and
     * {@link ScenarioChange.SettingOverride}s. To facilitate a cloud migration using segmentation
     * commodities, create source and destination groupings (unregistered with the group component)
     * with which to create a BindToGroupPolicy. This will enforce constraints that bind the
     * consumers (workloads) to the set provider(s) (regions).
     *
     * @param context Pipeline context to save policy group in.
     * @param graph Topology graph to look up entities.
     * @param topologyMigration the object symbolizing an entity/group to migrate
     */
    private void saveWorkloadPolicyGroups(
            @Nonnull final TopologyPipelineContext context,
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final TopologyMigration topologyMigration) {
        final Set<Long> sourceEntityOids = context.getSourceEntities();
        final EntityType workloadType = topologyMigration.getDestinationEntityType().equals(
                TopologyMigration.DestinationEntityType.VIRTUAL_MACHINE)
                ? EntityType.VIRTUAL_MACHINE
                : EntityType.DATABASE_SERVER;

        // We need to create one policy per source provider type. E.g if source is a group
        // with multiple VMs (on-prem and cloud), then we need to set provider type (PM or
        // Compute Tier) correctly for that case.
        final Map<EntityType, List<MigrationReference>> refsByProviderType =
                getWorkloadProviders(graph, sourceEntityOids);
        refsByProviderType
                .forEach((entityType, migrationReferenceList) -> createPolicyGroup(context,
                        migrationReferenceList, workloadType,
                        topologyMigration.getDestinationList(), entityType));
    }

    /**
     * Saves a set of segmentation policies related to cloud migration of volumes. Source volumes
     * are forced into cloud storage tiers via these segmentation policies.
     *
     * @param context Pipeline context to save policy group in.
     * @param graph Topology graph.
     */
    private void saveVolumePolicyGroups(
            @Nonnull final TopologyPipelineContext context,
            @Nonnull final TopologyGraph<TopologyEntity> graph) {
        final TopologyInfo topologyInfo = context.getTopologyInfo();
        for (Long oid : context.getSourceEntities()) {
            final TopologyEntity sourceEntity = graph.getEntity(oid)
                    .orElseThrow(() -> new EntityNotFoundException("Missing Cloud Migration "
                            + "source entity " + oid));
            // We need to create applicable cloud tiers on each entity basis, as we could have
            // source entities belonging to multiple cloud CSP types, so in that case, we need
            // to exclude the cloud tiers accordingly.
            Optional<TopologyEntity> currentCsp = getCloudServiceProvider(sourceEntity);

            Set<Long> applicableCloudTiers = getCloudStorageTiers(graph, topologyInfo,
                    currentCsp.orElse(null));
            final List<MigrationReference> destinationTierRefs = applicableCloudTiers.stream()
                    .map(id -> MigrationReference.newBuilder().setOid(id).build())
                    .collect(Collectors.toList());

            final Map<EntityType, List<MigrationReference>> providersByType =
                    getStorageProviders(sourceEntity);

            providersByType
                    .forEach((providerType, sourceVolumeRefs) -> {
                        for (MigrationReference volumeRef : sourceVolumeRefs) {
                            // Save a policy group for each volume id.
                            createPolicyGroup(context, Collections.singletonList(volumeRef),
                                    VIRTUAL_VOLUME,
                                    destinationTierRefs, providerType);
                        }
                    });
        }
    }

    /**
     * Gets a set of applicable cloud storage tiers, taking into account the tiers that are
     * restricted based on the plan type. E.g for Lift_n_Shift, we only allow GP2 or ManagedPremium.
     * Also, we need to get tiers that are not in the same CSP as the CSP of the current storage
     * tier, so that we can get a migration done to new CSP storage tiers.
     *
     * @param graph Topology graph.
     * @param topologyInfo Info about plan topology.
     * @param currentCsp We don't want any tiers that the entity is currently on, or any that
     *                   belong to the same CSP, so these storage tiers need to be skipped.
     * @return Ids of applicable cloud storage tiers based on plan type.
     */
    @Nonnull
    private Set<Long> getCloudStorageTiers(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                           @Nonnull final TopologyInfo topologyInfo,
                                           @Nullable final TopologyEntity currentCsp) {
        // Go over all known storage tiers, for all CSPs.
        return graph.entitiesOfType(STORAGE_TIER)
                .filter(storageTier -> {
                    // If this is an already known tier that we want to skip (e.g for Lift_n_Shift
                    // plan, we only want GP2 and Managed_Premium), then apply that filter.
                    if (!includeCloudStorageTier(storageTier, topologyInfo)) {
                        return false;
                    }
                    // Check CSP, we don't want another storage tier with the same CSP.
                    if (currentCsp == null) {
                        // For on-prem where current CSP is null, all CSPs are allowed.
                        return true;
                    }
                    // Current CSP, e.g if Azure, we want to allow only tiers that belong to AWS.
                    if (storageTier.getOwner().isPresent()) {
                        TopologyEntity cloudService = storageTier.getOwner().get();
                        if (cloudService.getOwner().isPresent()) {
                            TopologyEntity serviceProvider = cloudService.getOwner().get();
                            return currentCsp != serviceProvider;
                        }
                    }
                    // Owner info not set for storage tier for some reason, skip these.
                    return false;
                })
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet());
    }

    /**
     * Gets a map keyed off of provider type of source entity, value is a set of migration
     * references for entities having that provider type. One workload placement policy is created
     * per returned entry.
     *
     * @param graph Topology graph to lookup entities.
     * @param sourceEntityOids Set of source entities being migrated.
     * @return MigrationReference list per provider type.
     */
    @Nonnull
    private Map<EntityType, List<MigrationReference>> getWorkloadProviders(
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final Set<Long> sourceEntityOids) {
        Map<EntityType, List<MigrationReference>> workloadsByHostType = new HashMap<>();
        sourceEntityOids
                .forEach(workloadId -> {
                    final TopologyEntity workload = graph.getEntity(workloadId)
                            .orElseThrow(() -> new EntityNotFoundException("Missing Cloud Migration"
                                    + " workload source entity " + workloadId));
                    // Get provider type (PM or ComputeTier) for this workload.
                    EntityType providerType = PHYSICAL_MACHINE;
                    for (CommoditiesBoughtFromProvider commBought
                            : workload.getTopologyEntityDtoBuilder()
                            .getCommoditiesBoughtFromProvidersList()) {
                        EntityType eachProviderType = EntityType.forNumber(commBought
                                .getProviderEntityType());
                        if (eachProviderType == PHYSICAL_MACHINE
                                || eachProviderType == COMPUTE_TIER) {
                            providerType = eachProviderType;
                            break;
                        }
                    }
                    workloadsByHostType.computeIfAbsent(providerType,
                            k -> new ArrayList<>()).add(MigrationReference.newBuilder()
                            .setOid(workloadId).build());
                });
        return workloadsByHostType;
    }

    /**
     * Gets a map of storage provider type to the List of volume ids that are buying from that.
     * One placement policy is created for each such provider type.
     *
     * @param inputEntity Source VM being migrated.
     * @return Provider to volumes map.
     */
    @Nonnull
    private Map<EntityType, List<MigrationReference>> getStorageProviders(
            @Nonnull final TopologyEntity inputEntity) {
        Map<EntityType, List<MigrationReference>> volumesByStorageType = new HashMap<>();

        // If we are getting a cloud VM, then we need to process its volumes instead. Policy
        // in this case needs to be created on those volumes, with providers being storage tiers.
        Stream.of(inputEntity)
                .map(entity -> {
                    if (!isCloudEntity(entity)) {
                        return Collections.singletonList(entity);
                    }
                    return entity.getProviders()
                            .stream()
                            .filter(e -> e.getEntityType() == VIRTUAL_VOLUME_VALUE)
                            .collect(Collectors.toSet());
                })
                .flatMap(Collection::stream)
                .forEach(entity -> {
                    // This entity is a VM in case of on-prem source. For cloud sources, this is
                    // the cloud volume, with provider being storage tier.
                    entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList()
                            .stream()
                            .filter(commBought -> commBought.hasProviderId()
                                    && (commBought.getProviderEntityType() == STORAGE_VALUE
                                    || commBought.getProviderEntityType() == STORAGE_TIER_VALUE))
                            .forEach(commBought -> {
                                long volumeId = commBought.getVolumeId();
                                if (volumeId == 0) {
                                    // For cloud volume, entity itself it the volume, so get its id.
                                    volumeId = entity.getOid();
                                }
                                volumesByStorageType.computeIfAbsent(EntityType.forNumber(
                                        commBought.getProviderEntityType()),
                                        k -> new ArrayList<>()).add(MigrationReference.newBuilder()
                                        .setOid(volumeId).build());
                            });
                });
        return volumesByStorageType;
    }

    /**
     * Gets the current CSP for the entity being migrated. Needed so that we can make sure
     * the storage tiers that entity is migrating to don't belong to this CSP.
     *
     * @param entity Workload being migrated.
     * @return Optional entity's current CSP if present, or empty for e.g for on-prem.
     */
    private Optional<TopologyEntity> getCloudServiceProvider(
            @Nonnull final TopologyEntity entity) {
        if (!entity.getOwner().isPresent()) {
            return Optional.empty();
        }
        TopologyEntity account = entity.getOwner().get();
        return account.getAggregators()
                .stream()
                .filter(e -> e.getEntityType() == EntityType.SERVICE_PROVIDER_VALUE)
                .findFirst();
    }

    /**
     * Creates and adds one policy grouping to pipeline context.
     *
     * @param context Plan pipeline context.
     * @param sourceRefs List of source entity references, typically 1 workload being migrated.
     * @param sourceType Type of source workload.
     * @param destinationRefs Destination region reference.
     * @param destinationType Type of destination tier.
     */
    private void createPolicyGroup(@Nonnull final TopologyPipelineContext context,
                                 @Nonnull final List<MigrationReference> sourceRefs,
                                 EntityType sourceType,
                                 @Nonnull final List<MigrationReference> destinationRefs,
                                 EntityType destinationType) {
        final Grouping source = getStaticMigrationGroup(sourceRefs,
                sourceType.getNumber());
        final Grouping destination = getStaticMigrationGroup(destinationRefs,
                destinationType.getNumber());
        context.addPolicyGroup(new Pair<>(source, destination));
    }

    /**
     * Create a {@link Grouping} from a list of {@link MigrationReference} and a given type.
     * The resulting {@link Grouping} is never submitted to the group component,
     * nor persisted elsewhere.
     *
     * @param entities a list of {@link MigrationReference} from which a single group is created
     * @param destinationEntityType the type of the group to be created
     * @return a {@link Grouping} to be used for creating a {@link PlacementPolicy}
     */
    private Grouping getStaticMigrationGroup(
            @Nonnull final List<MigrationReference> entities,
            int destinationEntityType) {
        boolean areGroups = true;
        Map<Boolean, Set<Long>> areGroupsToSourceOids = entities.stream()
                .collect(Collectors.groupingBy(MigrationReference::hasGroupType,
                        Collectors.mapping(MigrationReference::getOid, Collectors.toSet())));

        Set<Long> staticGroupMemberOids = Sets.newHashSet();
        final Set<Long> groupEntities = areGroupsToSourceOids.get(areGroups);
        if (CollectionUtils.isNotEmpty(groupEntities)) {
            // Add the members of expanded groups
            groupServiceBlockingStub.getMembers(GetMembersRequest.newBuilder()
                    .addAllId(groupEntities)
                    .build())
                    .forEachRemaining(sourceGroup -> staticGroupMemberOids.addAll(
                            sourceGroup.getMemberIdList()));
        }
        final Set<Long> nonGroupEntities = areGroupsToSourceOids.get(!areGroups);
        if (CollectionUtils.isNotEmpty(nonGroupEntities)) {
            // Add individual entities
            staticGroupMemberOids.addAll(nonGroupEntities);
        }
        return PolicyManager.generateStaticGroup(
                staticGroupMemberOids,
                destinationEntityType,
                POLICY_GROUP_DESCRIPTION);
    }

    /**
     * Compute a map of each source OS value to the LICENSE_ACCESS commodity key to buy,
     * considering any configuration provided in the scenario regarding remapped OSes
     * and Bring Your Own License options.
     *
     * @param migrationScenario May contain a lit of os migration options to apply which
     *                          alter the default mapping.
     * @return A map from each possible OS type to the LICENSE_ACCESS commodity OsType key to buy.
     */
    @Nonnull
    private Map<OSType, OsType> computeLicenseCommodityKeysByOS(
            @Nonnull TopologyMigration migrationScenario) {
        Map<OsType, OSMigration> licenseTranslations = migrationScenario.getOsMigrationsList()
                .stream().collect(Collectors.toMap(
                        migration -> OsType.fromDtoOS(migration.getFromOs()),
                        Function.identity()));

        ImmutableMap.Builder<OSType, OsType> licensingMap = ImmutableMap.builder();
        for (OSType os : OSType.values()) {
            // 1. Convert subtypes like LINUX_FOO/WINDOWS_FOO to the general category OS
            OsType categoryOs = OsType.fromDtoOS(os).getCategoryOs();

            // if the VM has UNKNOWN license, force migration to Linux (free)
            if (categoryOs == OsType.UNKNOWN) {
                categoryOs = OsType.LINUX;
            }

            // 2. Apply changes from the scenario, or default to migrate to the same OS without
            // Bring Your Own License otherwise.
            OsType destinationOS = categoryOs;
            boolean byol = false;
            OSMigration osMigration = licenseTranslations.get(categoryOs);
            if (osMigration != null) {
                destinationOS = OsType.fromDtoOS(osMigration.getToOs());
                byol = osMigration.getByol();
            }

            // 3. Look up the appropriate license commodity key given the target OS and whether
            // the customer is going to BYOL.
            if (byol) {
                destinationOS = destinationOS.getByolOs();
            }

            licensingMap.put(os, destinationOS);
        }

        return licensingMap.build();
    }

    /**
     * Checks if entity has an associated business account, if so, it is considered cloud entity.
     * Env type doesn't seem to be set reliably, so cannot be considered for the check.
     *
     * @param entity Entity to check.
     * @return Whether entity is a cloud entity or not.
     */
    private boolean isCloudEntity(@Nonnull final TopologyEntity entity) {
        return entity.getOwner().isPresent() && entity.getOwner().get().getEntityType()
                == BUSINESS_ACCOUNT_VALUE;
    }

    /**
     * Given a set of oids, return the subset which are in an active state and thus
     * allowed to migrate.
     *
     * @param graph Topology graph
     * @param entityOids the set of oids to consider migrating
     * @return the set of oids whose status allows migration
     */
    @VisibleForTesting
    @Nonnull
    Set<Long> removeInactiveEntities(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                     @Nonnull final Set<Long> entityOids) {
        return entityOids.stream()
            .filter(oid -> graph.getEntity(oid)
                .map(entity -> entity.getEntityState() == EntityState.POWERED_ON)
                .orElse(false))
            .collect(Collectors.toSet());
    }

    /**
     * Finds discovered cloud tier exclusion policies used to keep VMs on "standard" tiers
     * and extend them so that migrating VMs also stick to these tiers.
     */
    public static class CloudMigrationSettingsPolicyEditor implements SettingPolicyEditor {
        // NOTE: trailing colon is important so we don't pick up other family groups whose
        // names start with standard, like standardNCSv3Family
        private static final String AWS_STANDARD_POLICY = "Cloud Compute Tier AWS:standard:";
        private static final String AZURE_STANDARD_POLICY = "Cloud Compute Tier Azure:standard:";

        private ResolvedGroup resolvedMigratingVmGroup;

        CloudMigrationSettingsPolicyEditor(@Nonnull final Set<Long> migratingVmOids) {
            this.resolvedMigratingVmGroup = new ResolvedGroup(
                PolicyManager.generateStaticGroup(
                    migratingVmOids,
                    VIRTUAL_MACHINE_VALUE,
                    POLICY_GROUP_DESCRIPTION),
                Collections.singletonMap(ApiEntityType.VIRTUAL_MACHINE, migratingVmOids));
        }

        @Nonnull
        @Override
        public List<SettingPolicy> applyEdits(@Nonnull final List<SettingPolicy> settingPolicies,
                                              @Nonnull final Map<Long, ResolvedGroup> groups) {
            groups.put(resolvedMigratingVmGroup.getGroup().getId(), resolvedMigratingVmGroup);

            return settingPolicies.stream().map(this::editSettingPolicy).collect(Collectors.toList());
        }

        /**
         * Check if the setting policy is for discovered template exclusion policies for
         * "standard" compute tiers, and if so return a new one with its scope expanded
         * to include the migrating VMs.
         *
         * <p>We can only recognize these by name currently, but since we're only looking
         * at discovered policies and no other probes should create policies whose
         * names reference AWS and Azure cloud tiers, it should be safe.
         *
         * <p>For the time being we apply these policies from all subscriptions and for
         * both AWS and Azure regardless of the migration target, because the policies
         * are the same for all subscriptions and it doesn't hurt to exclude AWS compute
         * tiers from a migration to Azure or vice versa.
         *
         * <p>TODO: it would be nice if a probe could explicitly indicate policies
         * that it wants to apply to migrations. This could be done with the
         * addition of new fields, or perhaps using tags. This could also allow
         * the probe to indicate policies that are region-specific and/or
         * subscription-specific, such as expired Promo exclusions. It would also be nice
         * if the existing groups in the scope had the owner set so we could limit
         * application by subscription easily.
         *
         * @param settingPolicy a setting policy to check and possibly replace
         * @return Either the original setting policy, or a new one if its scope
         * needed to be expanded.
         */
        private SettingPolicy editSettingPolicy(@Nonnull final SettingPolicy settingPolicy) {
            if (Type.DISCOVERED.equals(settingPolicy.getSettingPolicyType())) {
                SettingPolicyInfo info = settingPolicy.getInfo();
                if (hasTemplateExclusionSetting(info.getSettingsList())) {
                    if (info.getName().contains(AWS_STANDARD_POLICY)
                        || info.getName().contains(AZURE_STANDARD_POLICY)) {

                        // Add the group of migrating VMs to the scope of this SettingPolicy.
                        Scope newScope = info.getScope().toBuilder().addGroups(
                            resolvedMigratingVmGroup.getGroup().getId()).build();
                        SettingPolicyInfo newInfo = info.toBuilder().setScope(newScope).build();

                        return settingPolicy.toBuilder().setInfo(newInfo).build();
                    }
                }
            }

            return settingPolicy;
        }

        private boolean hasTemplateExclusionSetting(final @Nonnull List<Setting> settings) {
            return settings.stream().anyMatch(
                setting -> setting.getSettingSpecName()
                    .equals(EntitySettingSpecs.ExcludedTemplates.getSettingName()));
        }
    }
}
