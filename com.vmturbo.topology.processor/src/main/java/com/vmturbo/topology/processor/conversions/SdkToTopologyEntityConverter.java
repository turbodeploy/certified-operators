package com.vmturbo.topology.processor.conversions;

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
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.tag.Tag;
import com.vmturbo.common.protobuf.tag.Tag.TagValuesDTO;
import com.vmturbo.common.protobuf.topology.StitchingErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.HotResizeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO.RatioDependency;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.EntityPipelineErrors;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.UtilizationData;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.logmessagegrouper.LogMessageGrouper;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.VCpuData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.VMemData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.TagValues;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.poststitching.GuestLoadAppPostStitchingOperation;
import com.vmturbo.stitching.poststitching.StorageAccessCapacityPostStitchingOperation;
import com.vmturbo.stitching.utilities.CommoditiesBought;
import com.vmturbo.stitching.utilities.CopyActionEligibility;
import com.vmturbo.topology.processor.conversions.typespecific.CloudApplicationInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.ApplicationInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.ApplicationServiceInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.BusinessAccountInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.BusinessUserMapper;
import com.vmturbo.topology.processor.conversions.typespecific.CloudCommitmentInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.ComputeTierInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.ContainerInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.ContainerPlatformClusterInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.ContainerPodInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.DatabaseInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.DatabaseServerTierInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.DatabaseTierInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.DesktopPoolInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.DiskArrayInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.LogicalPoolInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.NamespaceInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.PhysicalMachineInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.RegionInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.ServiceInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.StorageControllerInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.StorageInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.TypeSpecificInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.VirtualMachineInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.VirtualVolumeInfoMapper;
import com.vmturbo.topology.processor.conversions.typespecific.WorkloadControllerInfoMapper;
import com.vmturbo.topology.processor.stitching.ResoldCommodityCache;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;

/**
 * Convert entity DTOs produced by SDK probes to topology processor's entity DTOs.
 */
public class SdkToTopologyEntityConverter {

    /**
     * Session ID for LogMessageGrouper so that we can group certain log messages together to
     * avoid log pollution.
     */
    public static final String LOGMESSAGEGROUPER_SESSION_ID = "SdkToTopologyEntityConverter";

    /**
     * Map from an {@link EntityType} to a {@link TypeSpecificInfoMapper} instance  that will
     * extract the relevent data from an @link EntityDTO} and populate a {@link TypeSpecificInfo}.
     *
     * <p/>This map has placeholders in comments for the full set of TypeSpecificInfo we may want for the future
     */
    private static final Map<EntityType, TypeSpecificInfoMapper> TYPE_SPECIFIC_INFO_MAPPERS =
            ImmutableMap.<EntityType, TypeSpecificInfoMapper>builder()
                    .put(EntityType.APPLICATION, new ApplicationInfoMapper())
                    .put(EntityType.APPLICATION_COMPONENT, new ApplicationInfoMapper())
                    .put(EntityType.APPLICATION_COMPONENT_SPEC, new CloudApplicationInfoMapper())
                    .put(EntityType.VIRTUAL_MACHINE_SPEC, new ApplicationServiceInfoMapper())
                    // Databases get their type-specific info sent via application data
                    .put(EntityType.DATABASE_SERVER, new DatabaseInfoMapper())
                    .put(EntityType.DATABASE, new DatabaseInfoMapper())
                    .put(EntityType.BUSINESS_ACCOUNT, new BusinessAccountInfoMapper())
                    .put(EntityType.REGION, new RegionInfoMapper())
                    .put(EntityType.COMPUTE_TIER, new ComputeTierInfoMapper())
                    .put(EntityType.DATABASE_TIER, new DatabaseTierInfoMapper())
                    .put(EntityType.DATABASE_SERVER_TIER, new DatabaseServerTierInfoMapper())
                    // CONTAINER_DATA
                    // CONTAINER_POD_DATA
                    .put(EntityType.PHYSICAL_MACHINE, new PhysicalMachineInfoMapper())
                    // PROCESSOR_POOL_DATA
                    // RESERVED_INSTANCE_DATA
                    .put(EntityType.STORAGE, new StorageInfoMapper())
                    .put(EntityType.DISK_ARRAY, new DiskArrayInfoMapper())
                    .put(EntityType.LOGICAL_POOL, new LogicalPoolInfoMapper())
                    .put(EntityType.STORAGE_CONTROLLER, new StorageControllerInfoMapper())
                    // VIRTUAL_APPLICATION_DATA
                    // VIRTUAL_DATACENTER_DATA
                    .put(EntityType.VIRTUAL_MACHINE, new VirtualMachineInfoMapper())
                    .put(EntityType.VIRTUAL_VOLUME, new VirtualVolumeInfoMapper())
                    .put(EntityType.DESKTOP_POOL, new DesktopPoolInfoMapper())
                    .put(EntityType.BUSINESS_USER, new BusinessUserMapper())
                    .put(EntityType.WORKLOAD_CONTROLLER, new WorkloadControllerInfoMapper())
                    .put(EntityType.CLOUD_COMMITMENT, new CloudCommitmentInfoMapper())
                    .put(EntityType.NAMESPACE, new NamespaceInfoMapper())
                    .put(EntityType.CONTAINER_PLATFORM_CLUSTER, new ContainerPlatformClusterInfoMapper())
                    .put(EntityType.CONTAINER, new ContainerInfoMapper())
                    .put(EntityType.CONTAINER_POD, new ContainerPodInfoMapper())
                    .put(EntityType.SERVICE, new ServiceInfoMapper())
                    .build();

    private static final Set<CommodityDTO.CommodityType> DSPM_OR_DATASTORE =
                    Sets.newHashSet(CommodityDTO.CommodityType.DSPM_ACCESS, CommodityDTO.CommodityType.DATASTORE);

    private static Set<CommodityDTO.CommodityType> reservedCommodityType =
            Sets.newHashSet(CommodityDTO.CommodityType.CPU, CommodityDTO.CommodityType.MEM,
                    CommodityDTO.CommodityType.VCPU, CommodityDTO.CommodityType.VMEM,
                    CommodityDTO.CommodityType.STORAGE_AMOUNT);

    private static final Set<EntityType> APPLICATION_ENTITY_TYPES =
            Sets.newHashSet(EntityType.BUSINESS_APPLICATION,
                            EntityType.BUSINESS_TRANSACTION,
                            EntityType.SERVICE,
                            EntityType.APPLICATION_SERVER,
                            EntityType.APPLICATION,
                            EntityType.APPLICATION_COMPONENT,
                            EntityType.DATABASE_SERVER,
                            EntityType.DATABASE,
                            EntityType.APPLICATION_COMPONENT_SPEC,
                            EntityType.VIRTUAL_MACHINE_SPEC);

    private static Set<CommodityDTO.CommodityType> SLO_COMMODITIES =
            Sets.newHashSet(CommodityDTO.CommodityType.RESPONSE_TIME,
                    CommodityDTO.CommodityType.TRANSACTION);

    private static final Logger logger = LogManager.getLogger();

    /**
     * TODO: this string constant should change, because the feature of entity tags is not VC-specific
     * The property should just be "TAGS".  We should create a task for this.
     * All probes using tags should be modified
     */
    public static final String TAG_NAMESPACE = "VCTAGS";

    /**
     * Property to use for sending the discovering target id.  This is used by
     * BusinessAccountInfoMapper to set the discovering target id for some business accounts.
     */
    public static final String DISCOVERING_TARGET_ID = "discoveringTargetId";

    private SdkToTopologyEntityConverter() {}

    private static int type(CommonDTO.EntityDTOOrBuilder dto) {
        return dto.getEntityType().getNumber();
    }

    private static int type(CommonDTO.CommodityDTOOrBuilder dto) {
        final UICommodityType uiCommType = UICommodityType.fromType(dto.getCommodityType().getNumber());
        if (uiCommType.sdkType() == CommodityDTO.CommodityType.UNKNOWN && dto.getCommodityType() != CommodityDTO.CommodityType.UNKNOWN) {
            // This is not a fatal error because we don't use UICommodityType everywhere
            // (in particular, history is using a different way of formatting commodity names),
            // but should be fixed ASAP.
            logger.error("Commodity type {} not supported by UICommodityType.", dto.getCommodityType());
        }
        return dto.getCommodityType().getNumber();
    }

    /**
     * Convert one probe entity DTO to one topology entity DTO.
     *
     * @param entity probe entity DTO.
     * @param resoldCommodityCache cache to look up which commodities are resold.
     * @return topology entity DTOs.
     */
    public static TopologyDTO.TopologyEntityDTO.Builder newTopologyEntityDTO(
            @Nonnull final TopologyStitchingEntity entity,
            @Nonnull final ResoldCommodityCache resoldCommodityCache) {
        final TopologyDTO.TopologyEntityDTO.Builder retVal =
                newTopologyEntityDTO(entity, resoldCommodityCache, false);
        final LogMessageGrouper msgGrouper = LogMessageGrouper.getInstance();
        final List<String> logMessages = msgGrouper.getMessages(LOGMESSAGEGROUPER_SESSION_ID);
        logMessages.forEach(logger::warn);
        msgGrouper.clear(LOGMESSAGEGROUPER_SESSION_ID);
        return retVal;
    }

    /**
     * Convert one probe entity DTO to one topology entity DTO.
     *
     * @param entity probe entity DTO.
     * @param resoldCommodityCache cache to look up which commodities are resold.
     * @param entityDetailsEnabled whether entity details are supported.
     * @return topology entity DTOs.
     */
    public static TopologyDTO.TopologyEntityDTO.Builder newTopologyEntityDTO(
            @Nonnull final TopologyStitchingEntity entity,
            @Nonnull final ResoldCommodityCache resoldCommodityCache,
            final boolean entityDetailsEnabled) {
        final Set<String> commodityTypesMissingProviders = new HashSet<>();
        final CommonDTO.EntityDTOOrBuilder dto = entity.getEntityBuilder();

        final int entityType = type(dto);
        // use id for displayName if it is not set in probe
        final String displayName = dto.hasDisplayName() ? dto.getDisplayName() : dto.getId();
        final TopologyDTO.EntityState entityState = entityState(dto);

        final TopologyEntityDTO.Builder result = TopologyDTO.TopologyEntityDTO.newBuilder()
            .setEntityType(entityType)
            .setOid(entity.getOid())
            .setDisplayName(displayName)
            .setEntityState(entityState);

        final Map<String, TagValuesDTO.Builder> entityTags = extractTags(dto);
        entityTags.forEach((key, val) -> {
            result.getTagsBuilder().putTags(key, val.build());
        });


        for (TopologyStitchingEntity.CommoditySold commoditySold : entity.getTopologyCommoditiesSold()) {
            TopologyDTO.CommoditySoldDTO.Builder builder = newCommoditySoldDTOBuilder(commoditySold.sold);
            if (commoditySold.accesses != null) {
                builder.setAccesses(commoditySold.accesses.getOid());
            }

            // Set the "isResold" flag on the commodity based on information originally passed in the
            // supply chain. "isResold" will be true if at least one discovering target marked it
            // as resold. Note that we purposely ignore key. If we have a use case where we need
            // to consider matching keys, we may need this flag on the actual CommoditySold in the SDK
            // rather than on the supply chain.
            entity.getDiscoveringTargetIds().forEach(
                    targetId -> resoldCommodityCache.getIsResold(targetId, entityType,
                            commoditySold.sold.getCommodityType().getNumber())
                            .ifPresent(isResold -> builder.setIsResold(builder.getIsResold() || isResold)));
            result.addCommoditySoldList(builder.build());
        }

        // list of commodities bought from different providers (there may be multiple
        // CommoditiesBoughtFromProvider for same provider)
        entity.getCommodityBoughtListByProvider().forEach((e, comms) -> {
            final CommoditiesBoughtFromProvider.Builder cbBuilder =
                    CommoditiesBoughtFromProvider.newBuilder()
                            .setProviderId(e.getOid())
                            .setProviderEntityType(e.getEntityType().getNumber());
            for (CommoditiesBought commodityBought : comms) {
                commodityBought.getBoughtList().forEach(b -> {
                    cbBuilder.addCommodityBought(newCommodityBoughtDTO(b, e.getCommoditiesSold(),
                            commodityTypesMissingProviders));
                });
                // Transfer the action eligibility settings from the
                // TopologyStitchingEntity's CommoditiesBought (if they were set)
                // to the TopologyEntityDTO's CommoditiesBoughtFromProvider
                CopyActionEligibility.transferActionEligibilitySettingsFromCommoditiesBought(
                        commodityBought, cbBuilder);
            }
            result.addCommoditiesBoughtFromProviders(cbBuilder.build());
        });

        // Log error messages through the LogMessageGrouper so that they can be consolidated by
        // commodity type for the whole topology, reducing log pollution.
        final String logMessageFormat = "The following entities could not have percentiles "
                + "populated for commodity %s because a corresponding sold commodity was not found."
                + " Enable DEBUG logging for more information. ";
        LogMessageGrouper logMessageGrouper = LogMessageGrouper.getInstance();

        commodityTypesMissingProviders.forEach(commString -> {
            logMessageGrouper.register(LOGMESSAGEGROUPER_SESSION_ID, commString,
                    String.format(logMessageFormat, commString));
            logMessageGrouper.log(LOGMESSAGEGROUPER_SESSION_ID, commString,
                    String.valueOf(entity.getOid()));
        });

        // Allocate this set outside the inner loop so we can reuse it for each connection type.
        final LongSet dedupeSet = new LongOpenHashSet();
        entity.getConnectedToByType().forEach((connectionType, connectedEntities) -> {
            // We use a set to distinguish connected entities of the same type because the same
            // stitchingEntity can appear multiple times in the
            // connected entity set: e.g. one business account can be discovered by multiple targets.
            // We only need to keep one.
            for (StitchingEntity connection : connectedEntities) {
                final boolean firstSeen = dedupeSet.add(connection.getOid());
                if (firstSeen) {
                    // create a ConnectedEntity to represent this connection
                    result.addConnectedEntityList(ConnectedEntity.newBuilder()
                            .setConnectedEntityId(connection.getOid())
                            .setConnectedEntityType(connection.getEntityType()
                                    .getNumber())
                            .setConnectionType(connectionType)
                            .build());
                }
            }
            // Clear the set after each connection type, because two entities may be connected
            // by two connections of separate types.
            dedupeSet.clear();
        });

        result.setStale(entity.isStale());

        injectEntityProperties(entity, result.getMutableEntityPropertyMap());

        if (entityDetailsEnabled) {
            result.addAllDetails(dto.getDetailsList());
        }

        result.setAnalysisSettings(buildAnalysisSettings(entity));
        result.setTypeSpecificInfo(
                mapToTypeSpecificInfo(entity, dto, result.getMutableEntityPropertyMap()));

        final StitchingErrors combinedStitchingErrors = entity.combinedEntityErrors();
        if (!combinedStitchingErrors.isNone()) {
            result.setPipelineErrors(EntityPipelineErrors.newBuilder()
                .setStitchingErrors(combinedStitchingErrors.getCode()));
        }
        return result;
    }

    /**
     * Inject the properties from the input entity into the input property map.
     * We inject the properties into the existing map instead of returning a new one to avoid
     * extra allocations, which slow down large topology construction.
     *
     * @param entity The entity.
     * @param propertyMap The property map. This is modified as part of this method.
     */
    private static void injectEntityProperties(@Nonnull final TopologyStitchingEntity entity,
            @Nonnull final  Map<String, String> propertyMap) {
        final EntityDTOOrBuilder dto = entity.getEntityBuilder();
        // Copy properties map from probe DTO to topology DTO
        // TODO: Support for namespaces and proper handling of duplicate properties (see
        // OM-20545 for description of probe expectations related to duplicate properties).
        final LogMessageGrouper msgGrouper = LogMessageGrouper.getInstance();
        msgGrouper.register(LOGMESSAGEGROUPER_SESSION_ID, "ENTITY_WITH_DUPLICATE_PROPERTIES",
                "The following entity properties (EntityOid : Property Name) are duplicated. Turn on debug log for more details\n");
        dto.getEntityPropertiesList().stream()
                .filter(SdkToTopologyEntityConverter::entityPropertyFilter)
                .forEach(prop -> {
                    if (propertyMap.containsKey(prop.getName())) {
                        msgGrouper.log(LOGMESSAGEGROUPER_SESSION_ID, "ENTITY_WITH_DUPLICATE_PROPERTIES",
                                String.valueOf(entity.getOid()) + ":" + prop.getName());
                        logger.debug("Duplicate entity property with values \"{}\", \"{}"
                                        + "\" detected on entity {} (name: {}).",
                                prop.getValue(), propertyMap.get(prop.getName()),
                                entity.getOid(), entity.getDisplayName());
                    } else {
                        propertyMap.put(prop.getName(), prop.getValue());
                    }
                });

        // TODO (roman, Jan 31 2020) OM-55033 Get rid of application data.
        // Only used in GuestLoadAppStitchingOperation.
        if (dto.hasApplicationData()) {
            propertyMap.put(GuestLoadAppPostStitchingOperation.APPLICATION_TYPE_PATH,
                    dto.getApplicationData().getType());
        }

        // TODO (roman, Jan 31 2020) OM-55034: Get rid of these three.
        // Only used in StorageAccessCapacityPostStitchingOperation.
        if (dto.hasDiskArrayData()) {
            propertyMap.put(StorageAccessCapacityPostStitchingOperation.DISK_ARRAY_PROP,
                    dto.getDiskArrayData().getIopsComputeData().toString());
        } else if (dto.hasStorageControllerData()) {
            propertyMap.put(StorageAccessCapacityPostStitchingOperation.STORAGE_CONTROLLER_PROP,
                    dto.getStorageControllerData().getIopsComputeData().toString());
        } else if (dto.hasLogicalPoolData()) {
            propertyMap.put(StorageAccessCapacityPostStitchingOperation.LOGICAL_POOL_PROP,
                    dto.getLogicalPoolData().getIopsComputeData().toString());
        }

        if (dto.hasVirtualMachineData()
                && MapUtils.isNotEmpty(dto.getVirtualMachineData().getDiskToStorageMap())) {
            // Subtract 1 from the diskToStorageCount to account for the OS disk
            propertyMap.put(StringConstants.NUM_VIRTUAL_DISKS, String.valueOf(Math.max(0, dto.getVirtualMachineData().getDiskToStorageCount() - 1)));
        }

        if (dto.hasStorageData()) {
            // set local attribute to true for local storages
            propertyMap.put("local", String.valueOf(isLocalStorage(entity)));
        }

        if (dto.hasBusinessAccountData()) {
            propertyMap.put(DISCOVERING_TARGET_ID, String.valueOf(entity.getTargetId()));
        }

    }

    @Nonnull
    private static AnalysisSettings buildAnalysisSettings(@Nonnull final TopologyStitchingEntity entity) {
        final EntityDTOOrBuilder dto = entity.getEntityBuilder();
        final boolean availableAsProvider = dto.getProviderPolicy().getAvailableForPlacement();
        final boolean isShopTogether = dto.getConsumerPolicy().getShopsTogether();
        final boolean isControllable = dto.getConsumerPolicy().getControllable();
        final boolean isProviderMustClone = dto.getConsumerPolicy().getProviderMustClone();
        final boolean isDeletable = dto.getConsumerPolicy().getDeletable();
        final boolean isMonitored = dto.getMonitored();

        // Check the ActionEligibility in the entity DTO and if values
        // for suspend and provision actions are supplied, else leave them unset
        Optional<Boolean> suspendable = Optional.empty();
        Optional<Boolean> cloneable = Optional.empty();
        if (dto.hasActionEligibility()) {
            CommonDTO.EntityDTO.ActionEligibility  actionEligibility = dto.getActionEligibility();

            // Keep the suspendable setting from the entityDTO
            if (actionEligibility.hasSuspendable()) {
                suspendable = Optional.of(actionEligibility.getSuspendable());
            }

            // Keep the provisionable setting from the entityDTO
            if (actionEligibility.hasCloneable()) {
                cloneable = Optional.of(actionEligibility.getCloneable());
            }
        }

        final Optional<Boolean> isDaemon = getDaemonSetting(dto);

        // Calculate suspendable flag if no info is provided from ActionEligibility
        if (!suspendable.isPresent()) {
            suspendable = calculateSuspendabilityWithStitchingEntity(entity);
        }

        AnalysisSettings.Builder analysisSettingsBuilder =
                TopologyDTO.TopologyEntityDTO.AnalysisSettings.newBuilder()
                        .setShopTogether(isShopTogether)
                        // Either monitored or controllable is false, set XL controllable to false.
                        // Explanations: some probes still send "Monitored = false", but XL doesn't have "Monitored" property,
                        // given the the semantic is the same, setting XL controllable to false.
                        // When probes send "Controllable = false", set XL controllable to false.
                        .setControllable(isControllable(isControllable, isMonitored))
                        .setProviderMustClone(isProviderMustClone)
                        .setIsAvailableAsProvider(availableAsProvider)
                        .setDeletable(isDeletable);

        // Check if the suspend and provision values are supplied
        if (suspendable.isPresent()) {
            boolean suspendableValue = suspendable.get();
            analysisSettingsBuilder.setSuspendable(suspendableValue);

            // If it is an application, set cloneable value to same as suspendable value
            // to represent horizontal scalability.
            if (entity.getEntityType() == EntityType.APPLICATION
                    || entity.getEntityType() == EntityType.APPLICATION_COMPONENT) {
                analysisSettingsBuilder.setCloneable(suspendableValue);
            }
        }

        cloneable.ifPresent(analysisSettingsBuilder::setCloneable);
        isDaemon.ifPresent(analysisSettingsBuilder::setDaemon);

        return analysisSettingsBuilder.build();
    }

    /**
     * Extract the daemon setting from the entity DTO.  This setting can either come directly from
     * the probe, or can be set if entity is a GuestLoad.
     * @param entityDTO entity DTO to check
     * @return Optional.empty if the setting is not present. Optional.of(false) if the setting is
     * present and set to false.  Optional.of(true) if the setting is present and set to true or if
     * the entity is a GuestLoad Application.
     */
    private static Optional<Boolean> getDaemonSetting(final EntityDTOOrBuilder entityDTO) {
        /*
         * If the consumer policy exists and has a daemon setting, we need Optional.of to indicate
         * that the setting was explicitly set.
         */
        boolean daemonSetting = false;
        boolean daemonSettingPresent = entityDTO.getConsumerPolicy().hasDaemon();
        if (daemonSettingPresent) {
            daemonSetting = entityDTO.getConsumerPolicy().getDaemon();
        }
        if (!daemonSetting && entityDTO.hasApplicationData()) {
            daemonSetting |= entityDTO.getApplicationData().getType().equals("GuestLoad");
        }
        return daemonSettingPresent || daemonSetting
            ? Optional.of(daemonSetting)
            : Optional.empty();
    }

    /**
     * Check if we'll keep this property or not.
     * {@code true} means keep it. {@code false} means don't keep it.
     * filter out based on VCTAGS namespace additionally
     * @param property the property we need to check
     * @return keep this property or not
     */
    static boolean entityPropertyFilter(@Nonnull final EntityProperty property) {
        return !(SupplyChainConstants.LOCAL_NAME.equals(property.getName())
            || property.getName().startsWith(StringConstants.CORE_QUOTA_PREFIX) || TAG_NAMESPACE.equals(property.getNamespace()));
    }

    /**
     * Map the entity-specific data contained in an {@link EntityDTO} to a
     * {@link TypeSpecificInfo} object that can be embedded into a {@link TopologyEntityDTO}.
     *
     * @param entity probe entity DTO.
     * @param sdkEntity The {@link EntityDTO} containing the entity-specific data.o
     * @param entityPropertyMap Entity properties (mutable) that can be passed to the individual sub-mappers.
     * @return The {@link TypeSpecificInfo} contained in the input {@link EntityDTO}.
     */
    @Nonnull
    private static TypeSpecificInfo mapToTypeSpecificInfo(
            @Nonnull final TopologyStitchingEntity entity,
            @Nonnull final CommonDTO.EntityDTOOrBuilder sdkEntity,
            @Nonnull final Map<String, String> entityPropertyMap) {
        Objects.requireNonNull(sdkEntity, "sdkEntity parameter must not be null");
        return Optional.ofNullable(TYPE_SPECIFIC_INFO_MAPPERS.get(sdkEntity.getEntityType()))
                .map(mapper -> mapper.mapEntityDtoToTypeSpecificInfo(entity, sdkEntity,
                        entityPropertyMap))
                .orElseGet(TypeSpecificInfo::getDefaultInstance);
    }

    /**
     * Convert one probe entity DTO state to one topology entity DTO state.
     *
     * @param entityDTO probe entity DTO..
     * @return topology entity DTO.
     */
    public static TopologyDTO.EntityState entityState(EntityDTOOrBuilder entityDTO) {
        TopologyDTO.EntityState entityState = TopologyDTO.EntityState.UNKNOWN;

        // retrieve entity state from dto
        CommonDTO.EntityDTO.PowerState powerState = entityDTO.getPowerState();
        switch (powerState) {
            case POWERED_OFF:
                entityState = TopologyDTO.EntityState.POWERED_OFF;
                break;
            case POWERED_ON:
                entityState = TopologyDTO.EntityState.POWERED_ON;
                break;
            case SUSPENDED:
                entityState = TopologyDTO.EntityState.SUSPENDED;
                break;
        }

        // Handle some power states that are specific for PMs
        if (entityDTO.getEntityType() == EntityType.PHYSICAL_MACHINE) {

            // Some hypervisors (like VC) can have a PM in maintenance and failover at the same time.
            // In this case, given that we want to show only a single state of the entity, we choose
            // to show maintenance, as the stronger of the 2 states.
            // So a server in maintenance will override a failover state.
            if (entityDTO.getMaintenance()) {
                entityState = EntityState.MAINTENANCE;
            } else if (entityDTO.getPhysicalMachineData().getPmState().getFailover()) {
                entityState = EntityState.FAILOVER;
            }
        } else if (FeatureFlags.STORAGE_MAINTENANCE_CONTROLLABLE.isEnabled()
                && entityDTO.getEntityType() == EntityType.STORAGE) {
            if (entityDTO.getMaintenance()) {
                entityState = EntityState.MAINTENANCE;
            }
        }


        return entityState;
    }

    @VisibleForTesting
    static boolean isControllable(final boolean isControllable, final boolean isMonitored) {
        return isControllable && isMonitored;
    }


    private static TopologyDTO.CommodityBoughtDTO.Builder newCommodityBoughtDTO(
            @Nonnull CommonDTO.CommodityDTOOrBuilder commDTO,
            @Nonnull Stream<CommodityDTO.Builder> providerSoldCommodities,
            @Nonnull Set<String> commodityTypesMissingProviders) {
        final TopologyDTO.CommodityBoughtDTO.Builder retCommBoughtBuilder =
            TopologyDTO.CommodityBoughtDTO.newBuilder()
                .setCommodityType(commodityType(commDTO))
                .setActive(commDTO.getActive())
                .setDisplayName(commDTO.getDisplayName());
        commDTO.getPropMapList().stream()
            .filter(prop -> prop.getName().equals(SDKConstants.AGGREGATES))
            .flatMap(prop -> prop.getValuesList().stream())
            .forEach(retCommBoughtBuilder::addAggregates);

        setUsedAndPeakForBoughtCommodity(retCommBoughtBuilder, commDTO, providerSoldCommodities,
                commodityTypesMissingProviders);

        // Only set reservedCapacity for specific commodityType
        if (reservedCommodityType.contains(commDTO.getCommodityType())) {
            retCommBoughtBuilder.setReservedCapacity(commDTO.getReservation());
        }
        if (commDTO.hasUtilizationData()) {
            CommonDTO.CommodityDTO.UtilizationData data = commDTO.getUtilizationData();
            retCommBoughtBuilder.setUtilizationData(UtilizationData.newBuilder()
                            .setIntervalMs(data.getIntervalMs())
                            .setLastPointTimestampMs(data.getLastPointTimestampMs())
                            .addAllPoint(data.getPointList()));
        }
        return retCommBoughtBuilder;
    }

    private static TopologyDTO.CommoditySoldDTO.Builder newCommoditySoldDTOBuilder(
        @Nonnull final CommonDTO.CommodityDTOOrBuilder commDTO) {

        if (commDTO.getPeak() < 0) {
            logger.error("Peak quantity = {} for commodity type {}", commDTO.getPeak(),
                commDTO.getCommodityType());
        }

        final TopologyDTO.CommoditySoldDTO.Builder retCommSoldBuilder =
            TopologyDTO.CommoditySoldDTO.newBuilder()
                .setCommodityType(commodityType(commDTO))
                .setIsThin(commDTO.getThin())
                .setActive(commDTO.getActive())
                .setIsResizeable(commDTO.getResizable())
                .setDisplayName(commDTO.getDisplayName());
        commDTO.getPropMapList().stream()
                .filter(prop -> prop.getName().equals(SDKConstants.AGGREGATES))
                .flatMap(prop -> prop.getValuesList().stream())
                .forEach(retCommSoldBuilder::addAggregates);
        // retain mediation values here, EntityValidator may change later
        if (commDTO.hasCapacity()) {
            retCommSoldBuilder.setCapacity(commDTO.getCapacity());
        }
        if (commDTO.hasUsed()) {
            retCommSoldBuilder.setUsed(adjustedUsed(commDTO));
        }
        if (commDTO.hasPeak()) {
            retCommSoldBuilder.setPeak(adjustedPeak(commDTO));
        }

        if (commDTO.hasLimit() && (commDTO.getLimit() > 0)
                && commDTO.hasCapacity() && (commDTO.getCapacity() > 0)) {
            // if limit < capacity, set the effective capacity percentage to limit / capacity as
            // a percentage.
            if (commDTO.getLimit() < commDTO.getCapacity()) {
                retCommSoldBuilder.setEffectiveCapacityPercentage(
                        100.0 * commDTO.getLimit() / commDTO.getCapacity());
            }
        }

        if (commDTO.hasUtilizationThresholdPct()) {
            // set or adjust the effective capacity percentage based on utilization threshold percentage
            double newEffectiveCapacityPercentage = commDTO.getUtilizationThresholdPct();
            if (retCommSoldBuilder.hasEffectiveCapacityPercentage()) {
                // this is an unexpected case -- we don't expect both limit and util threshold % to
                // co-exist at the same time, so let's take a note.
                logger.warn("{} commodity sold has both a 'limit' ({}) and "
                    + "'utilizationThresholdPct' ({}) set.", commDTO.getDisplayName(),
                    commDTO.getLimit(), commDTO.getUtilizationThresholdPct());
                // update the new effective capacity to reflect both the limit and the util threshold
                newEffectiveCapacityPercentage *= (retCommSoldBuilder.getEffectiveCapacityPercentage() / 100.0);
            }
            retCommSoldBuilder.setEffectiveCapacityPercentage(newEffectiveCapacityPercentage);
        }
        if (commDTO.hasUsedIncrement()) {
            retCommSoldBuilder.setCapacityIncrement((float)commDTO.getUsedIncrement());
        }
        if (commDTO.hasMaxAmountForConsumer()) {
            retCommSoldBuilder.setMaxAmountForConsumer(commDTO.getMaxAmountForConsumer());
        }
        if (commDTO.hasMinAmountForConsumer()) {
            retCommSoldBuilder.setMinAmountForConsumer(commDTO.getMinAmountForConsumer());
        }
        if (commDTO.hasCheckMinAmountForConsumer()) {
            retCommSoldBuilder.setCheckMinAmountForConsumer(commDTO.getCheckMinAmountForConsumer());
        }
        if (commDTO.hasRangeDependency()) {
            retCommSoldBuilder.setRangeDependency(commDTO.getRangeDependency());
        }
        if (commDTO.hasRatioDependency()) {
            RatioDependency.Builder ratioDependencyBuilder = TopologyDTO.CommoditySoldDTO.RatioDependency.newBuilder()
                    .setBaseCommodity(TopologyDTO.CommodityType.newBuilder()
                            .setType(commDTO.getRatioDependency().getBaseCommodity().getNumber()))
                    .setMaxRatio(commDTO.getRatioDependency().getMaxRatio());
            if (commDTO.getRatioDependency().hasMinRatio()) {
                ratioDependencyBuilder.setMinRatio(commDTO.getRatioDependency().getMinRatio());
            }
            if (commDTO.getRatioDependency().hasIncreaseBaseAmountDefaultSupported()) {
                ratioDependencyBuilder.setIncreaseBaseAmountDefaultSupported(
                        commDTO.getRatioDependency().getIncreaseBaseAmountDefaultSupported());
            }
            retCommSoldBuilder.setRatioDependency(ratioDependencyBuilder);
        }
        if (commDTO.hasUtilizationData()) {
            CommonDTO.CommodityDTO.UtilizationData data = commDTO.getUtilizationData();
            retCommSoldBuilder.setUtilizationData(UtilizationData.newBuilder()
                            .setIntervalMs(data.getIntervalMs())
                            .setLastPointTimestampMs(data.getLastPointTimestampMs())
                            .addAllPoint(data.getPointList()));
        }
        if (commDTO.getCommodityType() == CommodityDTO.CommodityType.VCPU && commDTO.hasVcpuData()) {
            VCpuData vCPUData = commDTO.getVcpuData();
            retCommSoldBuilder.setHotResizeInfo(HotResizeInfo.newBuilder()
                .setHotReplaceSupported(vCPUData.getHotAddSupported() || vCPUData.getHotRemoveSupported())
                .setHotAddSupported(vCPUData.getHotAddSupported())
                .setHotRemoveSupported(vCPUData.getHotRemoveSupported()));
        } else if (commDTO.getCommodityType() == CommodityDTO.CommodityType.VMEM && commDTO.hasVmemData()) {
            VMemData vMemData = commDTO.getVmemData();
            final HotResizeInfo.Builder hotResizeInfo = HotResizeInfo.newBuilder()
                .setHotReplaceSupported(vMemData.getHotAddSupported() || vMemData.getHotRemoveSupported())
                .setHotAddSupported(vMemData.getHotAddSupported());
            if (vMemData.hasHotRemoveSupported()) {
                hotResizeInfo.setHotRemoveSupported(vMemData.getHotRemoveSupported());
            }
            retCommSoldBuilder.setHotResizeInfo(hotResizeInfo);
        }

        return retCommSoldBuilder;
    }

    /**
     * Adjusted value of commodity used, based on whether 'used' is a percentage or not.
     *
     * @param commDTO the sold commodity to get used value for.
     * @return the adjusted value for given sold commodity
     */
    private static double adjustedUsed(
            @Nonnull final CommonDTO.CommodityDTOOrBuilder commDTO) {
        // missing capacity to be processed by EntityValidator
        if (commDTO.getIsUsedPct() && commDTO.hasCapacity()) {
            return commDTO.getUsed() * commDTO.getCapacity() / 100;
        } else {
            logger.trace("Capacity is unset, unable to calculate pct used for {}", () -> commDTO);
            return commDTO.getUsed();
        }
    }

    /**
     * Adjusted value of commodity peak, based on whether 'peak' is a percentage or not.
     *
     * @param commDTO the sold commodity to get peak value for.
     * @return the adjusted value for given sold commodity
     */
    private static double adjustedPeak(
            @Nonnull final CommonDTO.CommodityDTOOrBuilder commDTO) {
        // missing capacity to be processed by EntityValidator
        if (commDTO.getIsPeakPct() && commDTO.hasCapacity()) {
            return commDTO.getPeak() * commDTO.getCapacity() / 100;
        } else {
            logger.trace("Capacity is unset, unable to calculate pct peak for {}", () -> commDTO);
            return commDTO.getPeak();
        }
    }

    private static String getCommodityTypeString(@Nonnull CommonDTO.CommodityDTOOrBuilder commDTO) {
        if (StringUtils.isNotBlank(commDTO.getKey())) {
            return commDTO.getCommodityType().toString() + "::" + commDTO.getKey();
        } else {
            return commDTO.getCommodityType().toString();
        }
    }

    /**
     * Set the used and peak value for the bought commodity.
     * If the peak or used are percentage based then calculate the respective value(s)
     * based on capacity of the corresponding sold commodity.
     * Note that this method uses {@link LogMessageGrouper} to consolidate log messages,
     * so callers must make sure to dump the accumulated log messages and clear the messages
     * when they are done.
     *
     * @param builder builder for topology bought commodity
     * @param commDTO the commodity to get used/peak values for
     * @param providerSoldCommodities stream of sold commodities on provider side
     * @param commodityTypesMissingProviders a set of Strings describing the commodity types for
     * which we were supposed to deliver percentage based values but we cannot because the sold
     * commodity is missing. This is provided so calling method can log an appropriate message with
     * context.
     */
    private static void setUsedAndPeakForBoughtCommodity(
            @Nonnull TopologyDTO.CommodityBoughtDTO.Builder builder,
            @Nonnull final CommonDTO.CommodityDTOOrBuilder commDTO,
            @Nonnull Stream<CommodityDTO.Builder> providerSoldCommodities,
            @Nonnull Set<String> commodityTypesMissingProviders) {
        if (!commDTO.hasUsed() && !commDTO.hasPeak()) {
            return;
        }
        // if used and peak are not percentage based, return the values immediately
        if (!commDTO.getIsUsedPct() && !commDTO.getIsPeakPct()) {
            if (commDTO.hasUsed()) {
                builder.setUsed(commDTO.getUsed());
            }
            if (commDTO.hasPeak()) {
                builder.setPeak(commDTO.getPeak());
            }
            return;
        }

        // Find the corresponding sold commodity
        Optional<CommodityDTO.Builder> commSold = providerSoldCommodities
            .filter(soldComm -> soldComm.getCommodityType() == commDTO.getCommodityType()
                && StringUtils.equals(soldComm.getKey(), commDTO.getKey()))
            .findAny();

        if (!commSold.isPresent()) {
            commodityTypesMissingProviders.add(getCommodityTypeString(commDTO));
            // This message is creating a lot of noise when unstitched proxies are deleted after
            // stitching. We log it at DEBUG level, and wrap multiple messages of the same
            // commodity type into a single message that the caller will log at WARN level.
            logger.debug("No matching sold commodity of type {} and key {} on provider, "
                + "using percentage for used ({}) and peak ({}) values",
                commDTO.getCommodityType(), commDTO.getKey(),
                commDTO.getUsed(), commDTO.getPeak());
        }
        double factor = commSold
                .filter(CommodityDTO.Builder::hasCapacity)
                .map(CommodityDTO.Builder::getCapacity)
                .map(capacity -> capacity / 100)
                .orElse(1.0);
        if (commDTO.hasUsed()) {
            builder.setUsed(commDTO.getIsUsedPct() ? commDTO.getUsed() * factor : commDTO.getUsed());
        }
        if (commDTO.hasPeak()) {
            builder.setPeak(commDTO.getIsPeakPct() ? commDTO.getPeak() * factor : commDTO.getPeak());
        }
    }

    private static CommodityType.Builder commodityType(CommonDTO.CommodityDTOOrBuilder commDTO) {
        final CommodityType.Builder commodityTypeBuilder = CommodityType.newBuilder()
                .setType(type(commDTO));
        if (commDTO.hasKey()) {
            commodityTypeBuilder.setKey(commDTO.getKey());
        }
        return commodityTypeBuilder;
    }

    /**
     * Parse the access key of the commodity if its a DSPM or Datastore commodity.
     *
     * @param commDTO The input commodity.
     * @return The key, if the input commodity is of the right type.
     */
    public static Optional<String> parseAccessKey(@Nonnull final CommonDTO.CommodityDTOOrBuilder commDTO) {
        if (DSPM_OR_DATASTORE.contains(commDTO.getCommodityType())) {
            return Optional.ofNullable(keyToUuid(commDTO.getKey()));
        }

        return Optional.empty();
    }

    /**
     * Split the key of DSPMAccess commodity and DatastoreCommodity.
     * The keys look like "PhysicalMachine::7cd62bff-d6c8-e011-0000-00000000000f"
     * for DSPMAcess and "Storage::5787bc1e-357c82ea-47c4-0025b500038f" for
     * DatastoreCommodity, where the part after the colons is the uuid.
     *
     * <p/>For cloud targets the key looks like PhysicalMachine::aws::us-west-2::PM::us-west-2b
     *
     * @param key original key
     * @return the uuid part of the key. Returns null if the key does not match the access key format.
     */
    @Nullable
    public static String keyToUuid(String key) {
        int index = key.indexOf(ActionDTOUtil.COMMODITY_KEY_SEPARATOR);
        if (index >= 0) {
            return key.substring(index + ActionDTOUtil.COMMODITY_KEY_SEPARATOR_LENGTH);
        }
        return null;
    }

    /**
     * Use {@link TopologyStitchingEntity} to check if entity should be suspendable or not.
     * Discovered entities may be suspended. Proxy/replacable entities should never be suspended by the market.
     * They are often the top of the supply chain if they are not removed or replaced during stitching.
     * Thus, only unstitched proxy/replaceable entities will ever ben converted here.
     * And also if entity is a local storage, it should not be suspendable.
     *
     * <p/>TODO: Proxy/Replacable should be removed when we no longer need to support classic. Do not rely on it here.
     *
     * @param entity The entity whose suspendability should be calculated.
     * @return If the entity is discovered, an empty value to indicate the default suspendability should
     *         be retained. If the entity origin is not discovered or the entity is a local storage,
     *         an Optional of false to indicate the entity should never be suspended by the market.
     */
    @VisibleForTesting
    static Optional<Boolean> calculateSuspendabilityWithStitchingEntity(
            @Nonnull final TopologyStitchingEntity entity) {
        if (shouldDisableSuspension(entity)) {
            // Set the suspendability to false to disable suspension of the entity
            return Optional.of(false);
        }
        // Do not set suspendability of the entity to retain default setting
        return Optional.empty();
    }

    /**
     * Check if we should disable suspension for an entity.
     *
     * @param entity the entity to check.
     * @return a boolean.
     */
    private static boolean shouldDisableSuspension(@Nonnull final TopologyStitchingEntity entity) {
        // If the entity origin is not discovered, we should disable suspension
        if (entity.getEntityBuilder().getOrigin() != EntityOrigin.DISCOVERED) {
            return true;
        }
        // For application related entities
        if (APPLICATION_ENTITY_TYPES.contains(entity.getEntityType())) {
            // If the entity does not provide to a service with any non-zero SLO metrics,
            // we should disable suspension
            return shouldDisableAppSuspension(entity);
        }
        // If the entity is a local storage, we should disable suspension
        return isLocalStorage(entity);
    }

    /**
     * Check if we should disable suspension for an application related entity, as defined
     * in the applicationEntityTypes {@link Set}. If the entity does not provide to a service,
     * or if the entity provide to a service but there is no measured SLO metrics, we should
     * disable suspension of the entity.
     *
     * @param entity the application related entity to check
     * @return a boolean
     */
    private static boolean shouldDisableAppSuspension(TopologyStitchingEntity entity) {
        return entity.getConsumers().stream()
                    .map(StitchingEntity::getEntityType)
                    .noneMatch(EntityType.SERVICE::equals)
               || entity.getCommoditiesSold()
                    .noneMatch(commodity -> SLO_COMMODITIES.contains(commodity.getCommodityType())
                            && commodity.getUsed() > 0);
    }

    /**
     * Check if the entity is a local storage, which means the entity type is storage and its
     * localSupport is true and also attached to only one host.
     *
     * @param entity The entity need to check if is a local storage.
     * @return a boolean.
     */
    private static boolean isLocalStorage(@Nonnull final TopologyStitchingEntity entity) {
        return  entity.getEntityType() == EntityType.STORAGE
            && entity.getEntityBuilder().hasProviderPolicy()
            && entity.getEntityBuilder().getProviderPolicy().getLocalSupported()
            && attachedOnlyOneHost(entity);
    }

    /**
     * Check if the entity has only one Host in its consumers.
     *
     * @param entity The entity need to check.
     * @return a boolean.
     */
    private static boolean attachedOnlyOneHost(@Nonnull final TopologyStitchingEntity entity) {
        return entity.getConsumers().stream()
                .filter(providerEntity -> providerEntity.getEntityType() == EntityType.PHYSICAL_MACHINE)
                .count() == 1;
    }

    /**
     * Extract entity tags from an {@link EntityDTO} message to a map that will be inserted into a
     * {@link TopologyEntityDTO} message.
     *
     * <p/>An entity tag is a key/value pair associated with an entity.  A key may be associated with multiple
     * values within the tags of an entity, which is why the tags of an entity can be thought of as a map
     * from strings (key) to lists of strings (values).  The exact implementation is a map that maps
     * each string key to a {@link TagValuesDTO} object, which is a wrapper protobuf message that contains
     * a list of strings.
     *
     * <p/>In the {@link EntityDTO} message, entity tags are the following:
     * <ul>
     *  <li> for any triplet (namespace, key, value) that appears under entity_properties, the pair (key, value)
     *   is a tag iff the namespace is equal to the string constant TAG_NAMESPACE.</li>
     *  <li> if the entity is a VM, then all annotation notes are also tags.</li>
     * </ul>
     *
     * @param dto the {@link EntityDTO} message.
     * @return a map from string keys to {@link TagValuesDTO} objects.
     */
    @Nonnull
    private static Map<String, TagValuesDTO.Builder> extractTags(@Nonnull CommonDTO.EntityDTOOrBuilder dto) {
        final Map<String, TagValuesDTO.Builder> entityTags = extractTags(dto.getEntityPropertiesList());

        // find VM annotations
        if (dto.hasVirtualMachineData()) {
            dto.getVirtualMachineData().getAnnotationNoteList().forEach(
                annotation ->
                    // insert annotation as tag
                    entityTags.computeIfAbsent(annotation.getKey(), k -> TagValuesDTO.newBuilder())
                        .addValues(annotation.getValue()));
        }

        // call build on all TagValuesDTO builders
        return entityTags;
    }

    /**
     * Extract tags from a list of {@link EntityProperty} objects.
     * For any triplet (namespace, key, value) that appears under
     * this list, the pair (key, value) is a tag iff the namespace is equal
     * to the string constant {@link #TAG_NAMESPACE}.
     *
     * @param entityProperties a list of {@link EntityProperty} objects.
     * @return the corresponding tags.
     */
    @Nonnull
    public static Map<String, TagValuesDTO.Builder> extractTags(
            @Nonnull List<EntityProperty> entityProperties) {
        final Map<String, TagValuesDTO.Builder> result = new HashMap<>();

        // find tags under entity_properties
        // note that the namespace is used only to distinguish which properties are tags
        // and does not appear in the output
        entityProperties.stream()
                .filter(entityProperty -> TAG_NAMESPACE.equals(entityProperty.getNamespace()))
                .forEach(entityProperty -> {
                    // insert new tag
                    result.computeIfAbsent(entityProperty.getName(), k -> TagValuesDTO.newBuilder())
                            .addValues(entityProperty.getValue());
                });
        return result;
    }

    /**
     * Convert tags related to group.
     *
     * @param groupDTO group dto being converted
     * @return the {@link Tag.Tags} object if the group has tags and empty otherwise.
     */
    @Nonnull
    public static Optional<Tag.Tags> convertGroupTags(@Nonnull final CommonDTO.GroupDTO groupDTO) {
        final Map<String, TagValuesDTO> groupTags = new HashMap<>();
        // Add tags in the tags map such as resource group tags
        if (groupDTO.getTagsMap() != null && groupDTO.getTagsMap().size() > 0) {
            for (Entry<String, TagValues> entry : groupDTO.getTagsMap().entrySet()) {
                final TagValuesDTO tagValuesDTO =
                    TagValuesDTO.newBuilder().addAllValues(entry.getValue().getValueList()).build();
                groupTags.put(entry.getKey(), tagValuesDTO);
            }
            logger.trace("Tags `{}` were discovered in the tags map of discovered group `{}`.",
                () -> Joiner.on(",").withKeyValueSeparator("=").join(groupTags),
                () -> groupDTO.getDisplayName());
        } else {
            // Add VCTAGS
            groupTags.putAll(SdkToTopologyEntityConverter.extractTags(groupDTO.getEntityPropertiesList())
                .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                    entry -> entry.getValue().build())));
            logger.trace("Tags `{}` were discovered in the VCTAGS of discovered group `{}`.",
                () -> Joiner.on(",").withKeyValueSeparator("=").join(groupTags),
                () -> groupDTO.getDisplayName());
        }

        if (groupTags.size() > 0) {
            return Optional.of(Tag.Tags.newBuilder().putAllTags(groupTags).build());
        }

        logger.trace("No tags were found for group `{}`", () -> groupDTO.getDisplayName());
        return Optional.empty();
    }
}
