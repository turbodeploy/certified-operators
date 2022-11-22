package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.CostProtoUtil.CATEGORIES_TO_INCLUDE_FOR_ON_DEMAND_COST;
import static com.vmturbo.common.protobuf.CostProtoUtil.SOURCES_TO_EXCLUDE_FOR_ON_DEMAND_COST;
import static com.vmturbo.common.protobuf.utils.StringConstants.COMPUTE_TIER;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMappingContextFactory.ActionSpecMappingContext;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper.NotFoundCloudTypeException;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper.NotFoundMatchOfferingClassException;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper.NotFoundMatchPaymentOptionException;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper.NotFoundMatchTenancyException;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.mapper.converter.CloudSavingsDetailsDtoConverter;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.conversion.entity.CommodityTypeMapping;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionDetailsApiDTO;
import com.vmturbo.api.dto.action.ActionExecutionAuditApiDTO;
import com.vmturbo.api.dto.action.ActionExecutionCharacteristicApiDTO;
import com.vmturbo.api.dto.action.ActionScheduleApiDTO;
import com.vmturbo.api.dto.action.CloudProvisionActionDetailsApiDTO;
import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.action.CloudSuspendActionDetailsApiDTO;
import com.vmturbo.api.dto.action.CpuChangeDetailsApiDTO;
import com.vmturbo.api.dto.action.NoDetailsApiDTO;
import com.vmturbo.api.dto.action.OnPremResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.action.RIBuyActionDetailsApiDTO;
import com.vmturbo.api.dto.action.ReconfigureActionDetailsApiDTO;
import com.vmturbo.api.dto.entity.DiscoveredEntityApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VMEntityAspectApiDTO;
import com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO;
import com.vmturbo.api.dto.entityaspect.VirtualDisksAspectApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.ActionCostType;
import com.vmturbo.api.enums.ActionDetailLevel;
import com.vmturbo.api.enums.ActionDisruptiveness;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionReversibility;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.ConversionException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.Pair;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.api.mappers.EnvironmentTypeMapper;
import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSavingsAmountRangeFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Allocate;
import com.vmturbo.common.protobuf.action.ActionDTO.AtomicResize;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure.SettingChange;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTO.ResizeInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ResourceGroupFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.Scale;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.RiskUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.EntityFilter;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.CostSourceFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.StatValue;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIBuyContextFetchServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.utils.HCIUtils;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.Units;
import com.vmturbo.components.common.setting.OsMigrationSettingsEnum.OperatingSystem;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Map an ActionSpec returned from the ActionOrchestrator into an {@link ActionApiDTO} to be
 * returned from the API.
 */
public class ActionSpecMapper {

    /**
     * Define the String format with which to encode floating point (double) values in actions.
     *
     * <p>We use this particular format for backwards-compatibility with API v2.</p>
     */
    private static final String FORMAT_FOR_ACTION_VALUES = "%.1f";

    private static final Map<Integer, String> COMMODITY_TYPE_TO_FORMAT_FOR_ACTION_VALUES =
            ImmutableMap.of(CommodityType.PROCESSING_UNITS.getNumber(), "%.2f");

    // START - Strings representing action categories in the API.
    // These should be synchronized with the strings in stringUtils.js
    private static final String API_CATEGORY_PERFORMANCE_ASSURANCE = "Performance Assurance";
    private static final String API_CATEGORY_EFFICIENCY_IMPROVEMENT = "Efficiency Improvement";
    private static final String API_CATEGORY_PREVENTION = "Prevention";
    private static final String API_CATEGORY_COMPLIANCE = "Compliance";
    private static final String API_CATEGORY_UNKNOWN = "Unknown";
    // END - Strings representing action categories in the API.

    private static final Set<String> SCALE_TIER_VALUES = ImmutableSet.of(
            ApiEntityType.COMPUTE_TIER.apiStr(), ApiEntityType.DATABASE_SERVER_TIER.apiStr(),
            ApiEntityType.DATABASE_TIER.apiStr());

    private static final Set<String> CLOUD_ACTIONS_TIER_VALUES = new ImmutableSet.Builder<String>()
                    .addAll(SCALE_TIER_VALUES)
                    .add(ApiEntityType.STORAGE_TIER.apiStr())
                    .build();

    /**
     * Map of entity types to shortened versions for action descriptions.
     */
    private static final Map<String, String> SHORTENED_ENTITY_TYPES = ImmutableMap.of(
        ApiEntityType.VIRTUAL_VOLUME.apiStr(), "Volume"
    );

    private final ActionSpecMappingContextFactory actionSpecMappingContextFactory;

    private final long realtimeTopologyContextId;

    private static final Logger logger = LogManager.getLogger();

    private final ReservedInstanceMapper reservedInstanceMapper;

    private final RIBuyContextFetchServiceGrpc.RIBuyContextFetchServiceBlockingStub riStub;

    private final ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub
            reservedInstanceUtilizationCoverageServiceBlockingStub;

    private final CostServiceBlockingStub costServiceBlockingStub;

    private final BuyRiScopeHandler buyRiScopeHandler;

    private final UuidMapper uuidMapper;

    private final CloudSavingsDetailsDtoConverter cloudSavingsDetailsDtoConverter;

    private final GroupExpander groupExpander;

    /**
     * Flag that enables all action uuids come from the stable recommendation oid instead of the
     * unstable action instance id.
     */
    private final boolean useStableActionIdAsUuid;

    private final Map<Long, Map<EntityFilter, Map<Long, Cost.EntityReservedInstanceCoverage>>> topologyContextIdToEntityFilterToEntityRiCoverage =
            Maps.newHashMap();

    private static final Predicate<ActionState> IN_PROGRESS_PREDICATE = (state) ->
            state == ActionState.IN_PROGRESS
                    || state == ActionState.PRE_IN_PROGRESS
                    || state == ActionState.POST_IN_PROGRESS;

    /**
     * The set of action states for operational actions (ie actions that have not
     * completed execution).
     */
    public static final List<ActionDTO.ActionState> OPERATIONAL_ACTION_STATES = ImmutableList.of(
        ActionDTO.ActionState.READY,
        ActionDTO.ActionState.ACCEPTED,
        ActionDTO.ActionState.QUEUED,
        ActionDTO.ActionState.IN_PROGRESS
    );

    /**
     * Initialized the ActionSpecMapper with the provided implementations.
     *
     * @param actionSpecMappingContextFactory factory for getting AO contexts.
     * @param reservedInstanceMapper converts between API and XL representation of RIs.
     * @param riStub service for grabbing RI info.
     * @param costServiceBlockingStub service for grabbing cost info.
     * @param reservedInstanceUtilizationCoverageServiceBlockingStub service for grabbing RI Util info.
     * @param buyRiScopeHandler service for grabbing buy RI info.
     * @param realtimeTopologyContextId the topology id of the live, real market.
     * @param uuidMapper coverts between API ids and XL ids.
     * @param cloudSavingsDetailsDtoConverter the {@link CloudSavingsDetailsDtoConverter}.
     * @param groupExpander expands groups.
     * @param useStableActionIdAsUuid true when should use stable action recommendation oid instead
     *                                   of legacy action instance id as the uuid.
     */
    public ActionSpecMapper(@Nonnull ActionSpecMappingContextFactory actionSpecMappingContextFactory,
                            @Nonnull final ReservedInstanceMapper reservedInstanceMapper,
                            @Nullable final RIBuyContextFetchServiceGrpc.RIBuyContextFetchServiceBlockingStub riStub,
                            @Nonnull final CostServiceBlockingStub costServiceBlockingStub,
                            @Nonnull final ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub reservedInstanceUtilizationCoverageServiceBlockingStub,
                            @Nonnull final BuyRiScopeHandler buyRiScopeHandler,
                            final long realtimeTopologyContextId,
                            @Nonnull final UuidMapper uuidMapper,
                            @Nonnull final CloudSavingsDetailsDtoConverter cloudSavingsDetailsDtoConverter,
                            @Nonnull GroupExpander groupExpander,
                            final boolean useStableActionIdAsUuid) {
        this.actionSpecMappingContextFactory = Objects.requireNonNull(actionSpecMappingContextFactory);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.reservedInstanceMapper = Objects.requireNonNull(reservedInstanceMapper);
        this.costServiceBlockingStub = Objects.requireNonNull(costServiceBlockingStub);
        this.riStub = riStub;
        this.reservedInstanceUtilizationCoverageServiceBlockingStub = reservedInstanceUtilizationCoverageServiceBlockingStub;
        this.buyRiScopeHandler = buyRiScopeHandler;
        this.uuidMapper = uuidMapper;
        this.cloudSavingsDetailsDtoConverter = Objects.requireNonNull(cloudSavingsDetailsDtoConverter);
        this.groupExpander = Objects.requireNonNull(groupExpander);
        this.useStableActionIdAsUuid = useStableActionIdAsUuid;
    }

    /**
     * The equivalent of {@link ActionSpecMapper#mapActionSpecToActionApiDTO(ActionSpec, long)}
     * for a collection of {@link ActionSpec}s.
     *
     * <p>Processes the input specs atomically. If there is an error processing an individual action spec
     * that action is skipped and an error is logged.
     *
     * @param actionSpecs The collection of {@link ActionSpec}s to convert.
     * @param topologyContextId The topology context within which the {@link ActionSpec}s were
     *                          produced. We need this to get the right information from related
     *                          entities.
     * @return A collection of {@link ActionApiDTO}s in the same order as the incoming actionSpecs.
     * @throws UnsupportedActionException If the action type of the {@link ActionSpec}
     * is not supported.
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     * @throws ExecutionException on error mapping action spec
     */
    @Nonnull
    public List<ActionApiDTO> mapActionSpecsToActionApiDTOs(
            @Nonnull final Collection<ActionSpec> actionSpecs, final long topologyContextId)
            throws UnsupportedActionException, ExecutionException, InterruptedException,
            ConversionException {
        return mapActionSpecsToActionApiDTOs(actionSpecs, topologyContextId, ActionDetailLevel.STANDARD);
    }

    /**
     * The equivalent of {@link ActionSpecMapper#mapActionSpecToActionApiDTO(ActionSpec, long, ActionDetailLevel)}
     * for a collection of {@link ActionSpec}s.
     *
     * <p>Processes the input specs atomically. If there is an error processing an individual action spec
     * that action is skipped and an error is logged.</p>
     *
     * @param actionSpecs       The collection of {@link ActionSpec}s to convert.
     * @param topologyContextId The topology context within which the {@link ActionSpec}s were
     *                          produced. We need this to get the right information from related
     *                          entities.
     * @param detailLevel       Level of action details requested, used to include or exclude certain information.
     * @return A collection of {@link ActionApiDTO}s in the same order as the incoming actionSpecs.
     * @throws UnsupportedActionException If the action type of the {@link ActionSpec}
     *                                    is not supported.
     * @throws UnsupportedActionException when the action type is not supported.
     * @throws ExecutionException on failure getting entities.
     * @throws InterruptedException if thread has been interrupted.
     * @throws ConversionException if errors faced during converting data to API DTOs.
     */
    @Nonnull
    public List<ActionApiDTO> mapActionSpecsToActionApiDTOs(
            @Nonnull final Collection<ActionSpec> actionSpecs,
            final long topologyContextId,
            @Nullable final ActionDetailLevel detailLevel)
            throws UnsupportedActionException, ExecutionException, InterruptedException,
            ConversionException {
        if (actionSpecs.isEmpty()) {
            return Collections.emptyList();
        }
        final List<ActionDTO.Action> recommendations = actionSpecs.stream()
                .map(ActionSpec::getRecommendation)
                .collect(Collectors.toList());
        final ActionSpecMappingContext context =
                actionSpecMappingContextFactory.createActionSpecMappingContext(recommendations,
                        topologyContextId, uuidMapper);
        final ImmutableList.Builder<ActionApiDTO> actionApiDTOS = ImmutableList.builder();
        final List<Long> cloudEntityUuids = actionSpecs.stream()
                .map(ActionSpecMapper::getCloudEntityUuidFromActionSpec)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        final Map<Long, Cost.EntityReservedInstanceCoverage> coverageMap = getEntityRiCoverageMap(
                topologyContextId,
                EntityFilter.newBuilder().addAllEntityId(cloudEntityUuids).build());
        for (ActionSpec spec : actionSpecs) {
            final ActionApiDTO actionApiDTO =
                    mapActionSpecToActionApiDTOInternal(spec, context, topologyContextId, coverageMap, detailLevel);
            actionApiDTOS.add(actionApiDTO);
        }

        return actionApiDTOS.build();
    }

    /**
     * Map an ActionSpec returned from the ActionOrchestratorComponent into an {@link ActionApiDTO}
     * to be returned from the API.
     *
     * <p>The detail level returned in the {@link ActionApiDTO} is STANDARD.</p>
     *
     * <p>When required, a displayName value for a given Service Entity ID is gathered from the
     * Repository service.</p>
     *
     * <p>Some fields are returned as a constant. Some fields are ignored.</p>
     *
     * @param actionSpec The {@link ActionSpec} object to be mapped into an {@link ActionApiDTO}.
     * @param topologyContextId The topology context within which the {@link ActionSpec} was
     *                          produced. We need this to get the right information froGm related
     *                          entities.
     * @return an {@link ActionApiDTO} object populated from the given ActionSpec
     * @throws UnsupportedActionException If the action type of the {@link ActionSpec} is not
     * supported.
     * @throws ExecutionException on failure getting entities
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nonnull
    public ActionApiDTO mapActionSpecToActionApiDTO(@Nonnull final ActionSpec actionSpec,
                                                    final long topologyContextId)
            throws UnsupportedActionException, ExecutionException,
            InterruptedException, ConversionException {
        return mapActionSpecToActionApiDTO(actionSpec, topologyContextId, ActionDetailLevel.STANDARD);
    }

    /**
     * Map an ActionSpec returned from the ActionOrchestratorComponent into an {@link ActionApiDTO}
     * to be returned from the API.
     *
     * <p>When required, a displayName value for a given Service Entity ID is gathered from the
     * Repository service.</p>
     *
     * <p>Some fields are returned as a constant. Some fields are ignored.</p>
     *
     * @param actionSpec        The {@link ActionSpec} object to be mapped into an {@link ActionApiDTO}.
     * @param topologyContextId The topology context within which the {@link ActionSpec} was
     *                          produced. We need this to get the right information froGm related
     *                          entities.
     * @param detailLevel       Level of action details requested, used to include or exclude certain information.
     * @return an {@link ActionApiDTO} object populated from the given ActionSpec
     * @throws UnsupportedActionException If the action type of the {@link ActionSpec} is not
     *                                    supported.
     * @throws ExecutionException on failure getting entities
     * @throws InterruptedException if thread has been interrupted
     * @throws ConversionException if errors faced during converting data to API DTOs
     */
    @Nonnull
    public ActionApiDTO mapActionSpecToActionApiDTO(@Nonnull final ActionSpec actionSpec,
                                                    final long topologyContextId,
                                                    @Nullable final ActionDetailLevel detailLevel)
            throws UnsupportedActionException, ExecutionException,
            InterruptedException, ConversionException {
        final ActionSpecMappingContext context =
                actionSpecMappingContextFactory.createActionSpecMappingContext(
                        Lists.newArrayList(actionSpec.getRecommendation()), topologyContextId,
                        uuidMapper);
        return mapActionSpecToActionApiDTOInternal(actionSpec, context, topologyContextId, Maps.newHashMap(), detailLevel);
    }

    /**
     * Map an XL category to an equivalent API category string.
     *
     * @param category The {@link ActionDTO.ActionCategory}.
     * @return A string representing the action category that will be understandable by the UI.
     */
    @Nonnull
    public static String mapXlActionCategoryToApi(@Nonnull final ActionDTO.ActionCategory category) {
        switch (category) {
            case PERFORMANCE_ASSURANCE:
                return API_CATEGORY_PERFORMANCE_ASSURANCE;
            case EFFICIENCY_IMPROVEMENT:
                return API_CATEGORY_EFFICIENCY_IMPROVEMENT;
            case PREVENTION:
                return API_CATEGORY_PREVENTION;
            case COMPLIANCE:
                return API_CATEGORY_COMPLIANCE;
            default:
                return API_CATEGORY_UNKNOWN;
        }
    }

    /**
     * Converts an action orchestrator action state into an API action state.
     *
     * @param actionState the state of the action reported by action orchestrator.
     * @return the API action state representing the action orchestrator action state.
     */
    @Nonnull
    public static ActionState mapXlActionStateToApi(
            @Nonnull final ActionDTO.ActionState actionState) {
        switch (actionState) {
            case PRE_IN_PROGRESS:
            case POST_IN_PROGRESS:
                return ActionState.IN_PROGRESS;
            default:
                return ActionState.valueOf(actionState.name());
        }
    }

    /**
     * Map an {@link ActionDTO.ActionState} to a {@link ActionState},
     * return null if it's not an execution state.
     *
     * @param actionState state of the action
     * @return an {@link ActionState} enum, null if it's not an execution state
     */
    @Nullable
    private static ActionState mapXlActionStateToExecutionApi(@Nonnull final ActionDTO.ActionState actionState) {
        switch (actionState) {
            case PRE_IN_PROGRESS:
                return ActionState.PRE_IN_PROGRESS;
            case POST_IN_PROGRESS:
                return ActionState.POST_IN_PROGRESS;
            case IN_PROGRESS:
                return ActionState.IN_PROGRESS;
            case FAILED:
                return ActionState.FAILED;
            case SUCCEEDED:
                return ActionState.SUCCEEDED;
            case QUEUED:
                return ActionState.QUEUED;
            case ACCEPTED:
                return ActionState.ACCEPTED;
            default:
                return null;
        }
    }

    /**
     * Map an API category string to an equivalent XL category.
     *
     * @param category The string representing the action category in the UI.
     * @return An optional containing a {@link ActionDTO.ActionCategory}, or an empty optional if
     *         no equivalent category exists in XL.
     */
    @Nonnull
    public static Optional<ActionDTO.ActionCategory> mapApiActionCategoryToXl(
            @Nonnull final String category) {
        switch (category) {
            case API_CATEGORY_PERFORMANCE_ASSURANCE:
                return Optional.of(ActionCategory.PERFORMANCE_ASSURANCE);
            case API_CATEGORY_EFFICIENCY_IMPROVEMENT:
                return Optional.of(ActionCategory.EFFICIENCY_IMPROVEMENT);
            case API_CATEGORY_PREVENTION:
                return Optional.of(ActionCategory.PREVENTION);
            case API_CATEGORY_COMPLIANCE:
                return Optional.of(ActionCategory.COMPLIANCE);
            default:
                return Optional.empty();
        }
    }

    @Nonnull
    private ActionApiDTO mapActionSpecToActionApiDTOInternal(
            @Nonnull final ActionSpec actionSpec,
            @Nonnull final ActionSpecMappingContext context,
            final long topologyContextId,
            @Nonnull final Map<Long, Cost.EntityReservedInstanceCoverage> coverageMap,
            @Nullable final ActionDetailLevel detailLevel)
            throws UnsupportedActionException {
        // Construct a response ActionApiDTO to return
        final ActionApiDTO actionApiDTO = new ActionApiDTO();
        final long actionId = getActionId(actionSpec.getRecommendation().getId(), actionSpec.getRecommendationId(),
                topologyContextId);
        actionApiDTO.setUuid(Long.toString(actionId));
        actionApiDTO.setActionID(actionId);

        // Populate the action OID
        actionApiDTO.setActionImpactID(actionSpec.getRecommendationId());
        // set ID of topology/market for which the action is generated
        actionApiDTO.setMarketID(topologyContextId);
        // actionMode is direct translation
        final ActionDTO.ActionMode actionMode = actionSpec.getActionMode();
        actionApiDTO.setActionMode(ActionMode.valueOf(actionMode.name()));

        // For plan action, set the state to successes, so it will not be selectable
        // TODO (Gary, Jan 17 2019): handle case when realtimeTopologyContextId is changed (if needed)
        if (topologyContextId == realtimeTopologyContextId) {
            actionApiDTO.setActionState(mapXlActionStateToApi(
                actionSpec.getActionState()));
        } else {
            // In classic all the plan actions have "Succeeded" state; in XL all the plan actions
            // have default state (ready). Set the state to "Succeeded" here to make it Not selectable
            // on plan UI.
            actionApiDTO.setActionState(ActionState.SUCCEEDED);
        }

        actionApiDTO.setDisplayName(actionMode.name());

        // Set prerequisites for actionApiDTO if actionSpec has any pre-requisite description.
        if (!actionSpec.getPrerequisiteDescriptionList().isEmpty()) {
            actionApiDTO.setPrerequisites(actionSpec.getPrerequisiteDescriptionList().stream()
                .map(description -> RiskUtil.translateExplanation(description,
                    oid -> context.getEntity(oid).map(BaseApiDTO::getDisplayName).orElse(null)))
                .collect(Collectors.toList()));
        }

        // map the recommendation info
        final ActionDTO.Action recommendation = actionSpec.getRecommendation();
        LogEntryApiDTO risk = new LogEntryApiDTO();
        actionApiDTO.setImportance((float)0.0);
        risk.setImportance((float)0.0);
        // set the explanation string

        risk.setDescription(createRiskDescription(actionSpec, context));
        risk.setSubCategory(mapXlActionCategoryToApi(actionSpec.getCategory()));
        risk.setSeverity(mapSeverityToApi(actionSpec.getSeverity()));
        actionApiDTO.setRisk(risk);

        // The target definition
        actionApiDTO.setStats(createStats(actionSpec));

        // Action details has been set in AO
        actionApiDTO.setDetails(actionSpec.getDescription());

        final ActionDTO.ActionInfo info = recommendation.getInfo();
        ActionDTO.ActionType actionType = ActionDTOUtil.getActionInfoActionType(recommendation);

        // handle different action types
        switch (actionType) {
            case MOVE:
                addMoveInfo(actionApiDTO, recommendation, context, ActionType.MOVE);
                break;
            case SCALE:
                addMoveInfo(actionApiDTO, recommendation, context, ActionType.SCALE);
                break;
            case ALLOCATE:
                addAllocateInfo(actionApiDTO, actionSpec, context);
                break;
            case RECONFIGURE:
                addReconfigureInfo(actionApiDTO, recommendation, info.getReconfigure(), context);
                break;
            case PROVISION:
                addProvisionInfo(actionApiDTO, info.getProvision(), recommendation, context);
                break;
            case RESIZE:
                if (info.hasAtomicResize()) {
                    addAtomicResizeInfo(actionApiDTO, info.getAtomicResize(), context);
                } else {
                    addResizeInfo(actionApiDTO, recommendation, info.getResize(), context);
                }
                break;
            case ACTIVATE:
                // if the ACTIVATE action was originally a MOVE, we need to set the action details
                // as if it was a MOVE, otherwise we call the ACTIVATE method.
                if (info.getActionTypeCase() == ActionTypeCase.MOVE) {
                    addMoveInfo(actionApiDTO, recommendation, context, ActionType.START);
                } else {
                    addActivateInfo(actionApiDTO, recommendation, context);
                }
                break;
            case DEACTIVATE:
                addDeactivateInfo(actionApiDTO, recommendation, context);
                break;
            case DELETE:
                addDeleteInfo(actionApiDTO, info.getDelete(),
                    recommendation.getExplanation().getDelete(), context);
                break;
            case BUY_RI:
                addBuyRIInfo(actionApiDTO, info.getBuyRi(), context);
                break;
            default:
                throw new UnsupportedActionException(recommendation);
        }

        // Populate the placement and setting policies information that is related to this action.
        if (shouldPopulateRelatedSettingsPolicy(recommendation)) {
            populateSettingsPolicyForActionApiDto(recommendation, actionApiDTO, context);
        }
        if (!isCloudReconfigure(recommendation))  {
            populatePolicyForActionApiDto(recommendation, actionApiDTO, context);
        }

        // record the times for this action
        final String createTime = DateTimeUtil.toString(actionSpec.getRecommendationTime());
        actionApiDTO.setCreateTime(createTime);

        if (actionSpec.hasDecision()) {
            final ActionDecision decision = actionSpec.getDecision();
            final String decisionTime = DateTimeUtil.toString(decision.getDecisionTime());
            actionApiDTO.setUpdateTime(decisionTime);
            // was this action cleared?
            if (decision.hasClearingDecision()) {
                actionApiDTO.setClearTime(decisionTime);
            }
            if (decision.hasExecutionDecision()) {
                final ActionDecision.ExecutionDecision executionDecision =
                        decision.getExecutionDecision();
                final String decisionUserUUid = executionDecision.getUserUuid();
                if (!StringUtils.isBlank(decisionUserUUid)) {
                    actionApiDTO.setUserName(getUserName(decisionUserUUid));
                    // update actionMode based on decision uer id
                    updateActionMode(actionApiDTO, decisionUserUUid);
                }
            }
        }

        // update actionApiDTO with more info for realtime or plan actions
        addMoreInfoToActionApiDTO(actionApiDTO, context, recommendation, coverageMap);

        // add the Execution status
        if (ActionDetailLevel.EXECUTION == detailLevel) {
            actionApiDTO.setExecutionStatus(createActionExecutionAuditApiDTO(actionSpec));
        }

        // add the action schedule details
        if (actionSpec.hasActionSchedule()) {
            actionApiDTO.setActionSchedule(createActionSchedule(actionSpec.getActionSchedule()));
        }

        if (actionSpec.hasExternalActionName()) {
            actionApiDTO.setExternalActionName(actionSpec.getExternalActionName());
        }

        if (actionSpec.hasExternalActionUrl()) {
            actionApiDTO.setExternalActionUrl(actionSpec.getExternalActionUrl());
        }

        // Set the related action count by type of impacting relation
        actionApiDTO.setRelatedActionsCountByType(RelatedActionMapper.countRelatedActionsByType(actionSpec));

        // Set action execution characteristics
        if (recommendation.hasDisruptive() || recommendation.hasReversible()) {
            ActionExecutionCharacteristicApiDTO actionExecutionCharacteristicsDTO
                    = new ActionExecutionCharacteristicApiDTO();
            if (recommendation.hasDisruptive()) {
                actionExecutionCharacteristicsDTO.setDisruptiveness(recommendation.getDisruptive()
                        ? ActionDisruptiveness.DISRUPTIVE : ActionDisruptiveness.NON_DISRUPTIVE);
            }
            if (recommendation.hasReversible()) {
                actionExecutionCharacteristicsDTO.setReversibility(recommendation.getReversible()
                        ? ActionReversibility.REVERSIBLE : ActionReversibility.IRREVERSIBLE);
            }
            actionApiDTO.setExecutionCharacteristics(actionExecutionCharacteristicsDTO);
        }

        return actionApiDTO;
    }

    /**
     * Gets the ID that we show at the API level based an feature flag.
     *
     * <p>The recommendation (stable) id will be only used for real-time market and plan market always uses
     * the action instance id.</p>
     *
     * @param instanceId The instance id for the action.
     * @param recommendationId The recommendation (stable) id for the action.
     * @param topologyContextId the topology context the action in.
     *
     * @return the id we use in API.
     */
    public long getActionId(long instanceId, long recommendationId, long topologyContextId) {
        if (useStableActionIdAsUuid && topologyContextId == realtimeTopologyContextId) {
            return recommendationId;
        } else {
            return instanceId;
        }
    }

    /**
     * Determines whether we should append the automation policies to the action API DTO. This is
     * mainly for improving performance.
     *
     * @param recommendedAction The recommended action
     * @return Whether we should add the automation policies to the action API DTO
     */
    private boolean shouldPopulateRelatedSettingsPolicy(@Nonnull ActionDTO.Action recommendedAction) {
        // We currently want to add the automation policies when we have cloud reconfigure actions
        // or when you have a VM reconfigure action.
        final ActionInfo actionInfo = recommendedAction.getInfo();
        if (actionInfo.hasReconfigure()) {
            final ActionEntity reconfigureEntity = actionInfo.getReconfigure().getTarget();
            return reconfigureEntity.getEnvironmentType().equals(EnvironmentType.CLOUD)
                    || EntityType.VIRTUAL_MACHINE_VALUE == reconfigureEntity.getType();
        }
        return false;
    }

    /**
     * Determine whether the action is a cloud-reconfigure action.
     *
     * @param action the {@link Action}
     * @return Whether the action is cloud-reconfigure
     * */
    private boolean isCloudReconfigure(@Nonnull Action action) {
        final ActionInfo actionInfo = action.getInfo();
        if (actionInfo.hasReconfigure()) {
            final Reconfigure reconfigure = actionInfo.getReconfigure();
            return reconfigure.hasTarget()
                    && actionInfo.getReconfigure().getTarget().getEnvironmentType()
                    == EnvironmentType.CLOUD;
        }
        return false;
    }


    /**
     * Creates the API schedule object associated to the action.
     *
     * @param actionSchedule The input protobuf object.
     * @return The API schedule object.
     */
    private ActionScheduleApiDTO createActionSchedule(ActionSpec.ActionSchedule actionSchedule) {
        ActionScheduleApiDTO apiDTO = new ActionScheduleApiDTO();
        apiDTO.setUuid(String.valueOf(actionSchedule.getScheduleId()));
        apiDTO.setDisplayName(actionSchedule.getScheduleDisplayName());
        apiDTO.setTimeZone(actionSchedule.getScheduleTimezoneId());

        final ActionDTO.ActionMode executionWindowActionMode =
                actionSchedule.getExecutionWindowActionMode();
        if (executionWindowActionMode == ActionDTO.ActionMode.MANUAL
                || executionWindowActionMode == ActionDTO.ActionMode.EXTERNAL_APPROVAL) {
            apiDTO.setMode(ActionMode.valueOf(executionWindowActionMode.name()));
            if (actionSchedule.hasAcceptingUser()) {
                apiDTO.setAcceptedByUserForMaintenanceWindow(true);
                apiDTO.setUserName(actionSchedule.getAcceptingUser());
            } else {
                apiDTO.setAcceptedByUserForMaintenanceWindow(false);
            }
        } else {
            apiDTO.setAcceptedByUserForMaintenanceWindow(true);
            apiDTO.setMode(ActionMode.AUTOMATIC);
        }

        if (actionSchedule.hasEndTimestamp()
            && (!actionSchedule.hasStartTimestamp()
            || actionSchedule.getEndTimestamp() < actionSchedule.getStartTimestamp()
            || actionSchedule.getStartTimestamp() < System.currentTimeMillis())
            && actionSchedule.getEndTimestamp() > System.currentTimeMillis()) {
            apiDTO.setRemaingTimeActiveInMs(actionSchedule.getEndTimestamp() - System.currentTimeMillis());
        }

        if (actionSchedule.hasStartTimestamp()
            && actionSchedule.getStartTimestamp() > System.currentTimeMillis()) {
            apiDTO.setNextOccurrenceTimestamp(actionSchedule.getStartTimestamp());
            final TimeZone tz = actionSchedule.getScheduleTimezoneId() != null
                ? TimeZone.getTimeZone(actionSchedule.getScheduleTimezoneId()) : null;
            apiDTO.setNextOccurrence(DateTimeUtil.toString(actionSchedule.getStartTimestamp(), tz));
        }

        return apiDTO;
    }

    /**
     * Associate a reserved instance with an action given RI coverage data, and a set of
     * {@link ReservedInstanceApiDTO} in the given context.
     *
     * @param coverage data representing the RI coverage of an action target entity
     * @param contextRis reserved instances in a given context
     * @return the reserved instance to be associated with a given {@link ActionApiDTO}
     */
    private ReservedInstanceApiDTO getActionAsscociatedRi(
            @Nonnull final Cost.EntityReservedInstanceCoverage coverage,
            @Nonnull final Set<ReservedInstanceApiDTO> contextRis) {
        final Map<String, ReservedInstanceApiDTO> riUuidToInstance = contextRis.stream()
                .collect(Collectors.toMap(
                        ReservedInstanceApiDTO::getUuid,
                        Function.identity()));
        final Map<Long, Double> buyRiToCouponsCovered = coverage.getCouponsCoveredByBuyRiMap();
        final Map<Long, Double> riToCouponsCovered = coverage.getCouponsCoveredByRiMap();
        ReservedInstanceApiDTO actionReservedInstanceApiDTO = null;
        // Try to associate a buyRi first...
        if (!buyRiToCouponsCovered.isEmpty()) {
            actionReservedInstanceApiDTO = getRiFromCoverageMap(
                    buyRiToCouponsCovered.entrySet().iterator(),
                    riUuidToInstance);
        }
        // Try to match an existing RI if no buyRis matched...
        if (Objects.isNull(actionReservedInstanceApiDTO) && !riToCouponsCovered.isEmpty()) {
            actionReservedInstanceApiDTO = getRiFromCoverageMap(
                    riToCouponsCovered.entrySet().iterator(),
                    riUuidToInstance);
        }
        return actionReservedInstanceApiDTO;
    }

    /**
     * Get the first relevant {@link ReservedInstanceApiDTO} represented in the entries of an RI
     * coverage map given a map of UUID -> RI representing the current context.
     *
     * @param entries the entries of an RI coverage map - RI UUID to coupons covered
     * @param riUuidToInstance a map of UUID -> {@link ReservedInstanceApiDTO}
     * @return a {@link ReservedInstanceApiDTO} representing the first relevant RI in the coverage map
     */
    private ReservedInstanceApiDTO getRiFromCoverageMap(
            @Nonnull final Iterator<Map.Entry<Long, Double>> entries,
            @Nonnull final Map<String, ReservedInstanceApiDTO> riUuidToInstance) {
        ReservedInstanceApiDTO reservedInstanceApiDTO = null;
        do {
            final String coveringRiUuid = entries.next().getKey().toString();
            if (riUuidToInstance.containsKey(coveringRiUuid)) {
                reservedInstanceApiDTO = riUuidToInstance.get(coveringRiUuid);
            }
        } while (entries.hasNext() && Objects.isNull(reservedInstanceApiDTO));
        return reservedInstanceApiDTO;
    }

    /**
     * Update the given ActionApiDTO with more info for actions, such as aspects, template,
     * location, etc.
     *
     * @param actionApiDTO the ActionApiDTO to add more info to
     * @param context the ActionSpecMappingContext
     * @param action action info
     * @param coverageMap a map of RI ID -> RI coverage
     * @throws UnsupportedActionException if the action type of the {@link ActionSpec}
     * is not supported.
     */
    private void addMoreInfoToActionApiDTO(@Nonnull ActionApiDTO actionApiDTO,
            @Nonnull ActionSpecMappingContext context,
            @Nonnull ActionDTO.Action action,
            @Nonnull final Map<Long, Cost.EntityReservedInstanceCoverage> coverageMap)
                throws UnsupportedActionException {
        final ServiceEntityApiDTO targetEntity = actionApiDTO.getTarget();
        final ServiceEntityApiDTO newEntity = actionApiDTO.getNewEntity();
        final String targetEntityUuid = targetEntity.getUuid();
        final Long targetEntityId = Long.valueOf(targetEntityUuid);

        // add aspects to targetEntity
        final Map<AspectName, EntityAspect> aspects = new HashMap<>();
        context.getCloudAspect(targetEntityId).map(cloudAspect -> aspects.put(
            AspectName.CLOUD, cloudAspect));
        context.getVMAspect(targetEntityId).map(vmAspect -> aspects.put(
            AspectName.VIRTUAL_MACHINE, vmAspect));
        context.getDBAspect(targetEntityId).map(dbAspect -> aspects.put(
            AspectName.DATABASE, dbAspect));
        context.getContainerPlatformContext(targetEntityId).map(cnpAspect -> aspects.put(
                AspectName.CONTAINER_PLATFORM_CONTEXT, cnpAspect));
        targetEntity.setAspectsByName(aspects);

        if (newEntity == null && actionApiDTO.getActionType().equals(ActionType.DELETE)) {
            final String className = actionApiDTO.getTarget().getClassName();
            // add volume aspects if delete volume action
            if (ApiEntityType.VIRTUAL_VOLUME.apiStr().equals(className)) {
                final Map<AspectName, EntityAspect> aspectMap = new HashMap<>();
                List<VirtualDiskApiDTO> volumeAspectsList = context.getVolumeAspects(targetEntityId);
                if (!volumeAspectsList.isEmpty()) {
                    VirtualDisksAspectApiDTO virtualDisksAspectApiDTO = new VirtualDisksAspectApiDTO();
                    virtualDisksAspectApiDTO.setVirtualDisks(volumeAspectsList);
                    aspectMap.put(AspectName.VIRTUAL_VOLUME, virtualDisksAspectApiDTO);
                }
                // add cloud aspect for delete volume action
                context.getCloudAspect(targetEntityId).ifPresent(
                        cloudAspect -> aspectMap.put(AspectName.CLOUD, cloudAspect));
                if (!aspectMap.isEmpty()) {
                    actionApiDTO.getTarget().setAspectsByName(aspectMap);
                }
            } else if (ApiEntityType.VIRTUAL_MACHINE_SPEC.apiStr().equals(className)) {
                // add template with current compute tier
                final Optional<ServiceEntityApiDTO> actionEntity = context.getEntity(ActionDTOUtil.getPrimaryEntityId(action));

                if (actionEntity.isPresent()) {
                    final ServiceEntityApiDTO entity = actionEntity.get();
                    final List<BaseApiDTO> providers = entity.getProviders();

                    final BaseApiDTO computeTier = providers.stream().filter(provider -> COMPUTE_TIER.equals(provider.getClassName())).findAny().orElse(null);

                    if (computeTier != null) {
                        TemplateApiDTO templateApiDTO = new TemplateApiDTO();
                        templateApiDTO.setUuid(computeTier.getUuid());
                        templateApiDTO.setDisplayName(computeTier.getDisplayName());
                        templateApiDTO.setClassName(computeTier.getClassName());
                        actionApiDTO.setTemplate(templateApiDTO);
                    }
                }
            }
        }

        // add more info for cloud actions
        if (newEntity != null && CLOUD_ACTIONS_TIER_VALUES.contains(newEntity.getClassName())) {
            // set template for cloud actions, which is the new tier the entity is using
            TemplateApiDTO templateApiDTO = new TemplateApiDTO();
            templateApiDTO.setUuid(newEntity.getUuid());
            templateApiDTO.setDisplayName(newEntity.getDisplayName());
            templateApiDTO.setClassName(newEntity.getClassName());
            actionApiDTO.setTemplate(templateApiDTO);

            // VM aspects need to be set for new entity (cloud tier for cloud migration case).
            if (context.hasMigrationActions()) {
                final Map<AspectName, EntityAspect> newAspects = new HashMap<>();
                context.getVMProjectedAspect(targetEntityId)
                        .map(vmAspect -> newAspects.put(AspectName.VIRTUAL_MACHINE, vmAspect));
                // We show SLES in initial plan configuration in UI, so change SUSE to SLES
                // in VM mapping output table to be consistent.
                newAspects.values()
                        .stream()
                        .filter(VMEntityAspectApiDTO.class::isInstance)
                        .map(VMEntityAspectApiDTO.class::cast)
                        .forEach(vmAspect -> {
                            if (OSType.SUSE.name().equals(vmAspect.getOs())) {
                                vmAspect.setOs(OperatingSystem.SLES.name());
                            }
                        });
                newEntity.setAspectsByName(newAspects);

                // If RI for plan is available, set it.
                final Cost.EntityReservedInstanceCoverage coverage = coverageMap.get(targetEntityId);
                final Set<ReservedInstanceApiDTO> contextRis = context.getReservedInstances();
                if (Objects.nonNull(coverage) && !contextRis.isEmpty()) {
                    final ReservedInstanceApiDTO actionReservedInstanceApiDTO =
                            getActionAsscociatedRi(coverage, contextRis);
                    if (Objects.nonNull(actionReservedInstanceApiDTO)) {
                        actionApiDTO.setReservedInstance(actionReservedInstanceApiDTO);
                    }
                }
            }
        }

        // TODO (Cloud PaaS): ASP "legacy" APPLICATION_COMPONENT support, OM-83212
        //  can remove APPLICATION_COMPONENT check when legacy support not needed
        if (ApiEntityType.VIRTUAL_MACHINE_SPEC.apiStr().equals(targetEntity.getClassName())
                || ApiEntityType.APPLICATION_COMPONENT.apiStr().equals(targetEntity.getClassName())) {
            final Long actionTargetId = ActionDTOUtil.getPrimaryEntity(action).getId();
            setCurrentAndNewLocation(actionTargetId, context, actionApiDTO);
        }

        // For cloud changing tier actions and virtual volume SCALE/MOVE/DELETE actions,
        // set region and virtualDisks.
        // (Volume action is SCALE in real time and optimized plan, and is MOVE in migration plan.)
        if (newEntity != null && CLOUD_ACTIONS_TIER_VALUES.contains(newEntity.getClassName())
                || targetEntity.getClassName().equals(ApiEntityType.VIRTUAL_VOLUME.apiStr())) {
            /*
             * Move volume actions in migration plan have virtual volume as target entity
             * after converting to the ActionApiDTO. So we need to get VM ID from action info.
             */
            final Long actionTargetId = ActionDTOUtil.getPrimaryEntity(action, false).getId();
            // set location, which is the region
            setCurrentAndNewLocation(actionTargetId, context, actionApiDTO);
            final ActionTypeCase actionTypeCase = action.getInfo().getActionTypeCase();
            // Filter virtual disks if it is MOVE virtual volume action in migration plan.
            final Predicate<VirtualDiskApiDTO> filter = actionTypeCase == ActionTypeCase.MOVE
                && ApiEntityType.VIRTUAL_VOLUME.apiStr().equals(targetEntity.getClassName())
                    ? vd -> targetEntityUuid.equals(vd.getUuid())
                    : vd -> true;

            final List<VirtualDiskApiDTO> virtualDisks = context.getVolumeAspects(actionTargetId).stream()
                    .filter(filter)
                    .collect(Collectors.toList());
            actionApiDTO.setVirtualDisks(virtualDisks);

            // update target display name from projection if moving multiple volumes into one (migrate to cloud)
            if (ApiEntityType.VIRTUAL_VOLUME.apiStr().equals(targetEntity.getClassName())
                            && virtualDisks.size() == 1 && actionTypeCase == ActionTypeCase.MOVE) {
                actionApiDTO.getTarget().setDisplayName(virtualDisks.get(0).getDisplayName());
            }
        }
    }

    /**
     * Creates the stats for the given actionSpec.
     *
     * @param source the actionSpec for which stats are to be created
     * @return a list of stats to be added to the ActionApiDto
     */
    private List<StatApiDTO> createStats(final ActionSpec source) {
        List<StatApiDTO> stats = Lists.newArrayList();
        if (source.hasRecommendation() && source.getRecommendation().hasSavingsPerHour()) {
            createSavingsStat(source.getRecommendation().getSavingsPerHour()).ifPresent(stats::add);
        }
        return stats;
    }

    /**
     * Creates the savings stats.
     *
     * @param savingsPerHour the savings per hour
     * @return the savings stats
     */
    private Optional<StatApiDTO> createSavingsStat(CurrencyAmount savingsPerHour) {
        if (savingsPerHour.getAmount() != 0) {
            // Get the currency
            final String currencyUnit = CostProtoUtil.getCurrencyUnit(savingsPerHour);
            // Get the amount rounded to 7 decimal places. We round to 7 decimal places because we
            // convert this to a monthly savings number, and if we round to less than 7 decimal
            // places, then we might lose a few tens of dollars in savings
            Optional<Float> savingsAmount = roundToFloat(savingsPerHour.getAmount(), 7);
            if (savingsAmount.isPresent()) {
                StatApiDTO dto = new StatApiDTO();
                dto.setName(StringConstants.COST_PRICE);
                dto.setValue(savingsAmount.get());
                // The savings
                dto.setUnits(currencyUnit);
                // Classic has 2 types of savings - savings and super savings. XL currently just has
                // one type of savings - savings
                dto.addFilter(StringConstants.SAVINGS_TYPE, StringConstants.SAVINGS);
                return Optional.of(dto);
            }
        }
        return Optional.empty();
    }

    /**
     * This method rounds the given number a specified number of decimal places and converts
     * to float.
     *
     * @param d the double to be rounded
     * @param precision the number of digits to the right of decimal point desired
     * @return the rounded number in float
     */
    private Optional<Float> roundToFloat(double d, int precision) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            return Optional.empty();
        }
        BigDecimal bd = new BigDecimal(Double.toString(d));
        bd = bd.setScale(precision, RoundingMode.HALF_UP);
        return Optional.of(Float.valueOf(bd.floatValue()));
    }

    /**
     * Update action mode based on decision user id.
     * Rule: if the decision user id is "SYSTEM", set the action mode to "automatic"; otherwise
     * set it to "MANUAL".
     *
     * @param actionApiDTO action API DTO
     * @param decisionUserUUid decision user id
     */
    private void updateActionMode(@Nonnull final ActionApiDTO actionApiDTO,
                                  @Nonnull final String decisionUserUUid) {
        if (AuditLogUtils.SYSTEM.equals(decisionUserUUid)) {
            actionApiDTO.setActionMode(ActionMode.AUTOMATIC);
        } else {
            actionApiDTO.setActionMode(ActionMode.MANUAL);
        }
    }

    /**
     * Get the username from "decisionUserUuid" to be showed in UI.
     * @param decisionUserUuid id could be either "SYSTEM" or "user" & UUID (e.g. administrator(22222222222))
     * @return username, e.g. either "SYSTEM" or "administrator"
     */
    @VisibleForTesting
    String getUserName(@Nonnull final String decisionUserUuid) {
        if (AuditLogUtils.SYSTEM.equals(decisionUserUuid)) {
            return decisionUserUuid;
        } else if (!decisionUserUuid.contains("(")) {
            return decisionUserUuid;
        } else {
            return decisionUserUuid.substring(0, decisionUserUuid.indexOf("("));
        }
    }

    private void setRelatedDatacenter(long oid,
                                      @Nonnull ActionApiDTO actionApiDTO,
                                      @Nonnull ActionSpecMappingContext context,
                                      boolean newLocation) {
        context.getDatacenterFromOid(oid)
            .ifPresent(apiPartialEntity ->
                context.getDiscoveredEntity(apiPartialEntity.getOid()).ifPresent(
                    discoveredEntityDTO -> {
                        if (newLocation) {
                            actionApiDTO.setNewLocation(discoveredEntityDTO);
                        } else {
                            actionApiDTO.setCurrentLocation(discoveredEntityDTO);
                        }
                    })
    );
    }

    @Nonnull
    private String createRiskDescription(@Nonnull final ActionSpec actionSpec,
              @Nonnull final ActionSpecMappingContext context) throws UnsupportedActionException {
        return RiskUtil.createRiskDescription(actionSpec,
            context::getPolicyDisplayName,
            oid -> context.getEntity(oid).map(BaseApiDTO::getDisplayName).orElse(null));
    }

    /**
     * Sets the currentValue and newValue of {@param wrapperDto} to the {@param primaryProvider} ID,
     * and sets currentEntity and newEntity to a newly created {@link ServiceEntityApiDTO}
     * representing {@param primaryProvider}.
     *
     * @param primaryProvider The primary provider of an action target
     * @param wrapperDto The API DTO to be delivered
     * @param context From which to fetch an entity reference
     */
    private void setCurrentAndNewToPrimaryProvider(
            @Nonnull ActionDTO.ActionEntity primaryProvider,
            @Nonnull final ActionApiDTO wrapperDto,
            @Nonnull final ActionSpecMappingContext context) {
        Long primaryProviderId = primaryProvider.getId();
        String primaryProviderIdString = Long.toString(primaryProviderId);
        wrapperDto.setCurrentValue(primaryProviderIdString);
        wrapperDto.setNewValue(primaryProviderIdString);

        ServiceEntityApiDTO primaryProviderSeApiDTO = getServiceEntityDTO(context, primaryProvider);
        wrapperDto.setCurrentEntity(primaryProviderSeApiDTO);
        wrapperDto.setNewEntity(primaryProviderSeApiDTO);

        setRelatedDatacenter(primaryProviderId, wrapperDto, context, false);
    }

    /**
     * Populate various fields of the {@link ActionApiDTO} representing a (compound) move.
     *
     * @param wrapperDto the DTO that represents the move recommendation and
     * wraps other {@link ActionApiDTO}s
     * @param action Action object
     * @param context mapping from {@link ActionSpec} to {@link ActionApiDTO}
     * @param actionType {@link ActionType} that will be assigned to wrapperDto param
     * @throws UnsupportedActionException if the action is an unsupported type.
     */
    private void addMoveInfo(@Nonnull final ActionApiDTO wrapperDto,
                             @Nonnull final ActionDTO.Action action,
                             @Nonnull final ActionSpecMappingContext context,
                             @Nonnull final ActionType actionType)
            throws UnsupportedActionException {

        // If any part of the compound move is an initial placement, the whole move is an initial
        // placement.
        //
        // Under normal circumstances, all sub-moves of an initial placement would be initial
        // placements (e.g. a new VM needs to start on a storage + host). However,
        // in a hardware refresh plan where we remove (or replace) entities we may get an initial
        // placement mixed with a regular move. For example, suppose we have a topology with 1 host,
        // 2 storages, and 1 VM. We replace the host with a different host. The original host
        // no longer exists in the topology. From the market's perspective the VM is not placed,
        // and it will generate an initial placement for the host. However, suppose it also generates
        // a storage move. Since the original storage was not removed from the topology, the storage
        // move would be a regular move.
        final List<ChangeProviderExplanation> changeProviderExplanationList = ActionDTOUtil
            .getChangeProviderExplanationList(action.getExplanation());
        final boolean initialPlacement = changeProviderExplanationList.stream()
                .anyMatch(ChangeProviderExplanation::hasInitialPlacement);

        wrapperDto.setActionType(actionType);
        // Set entity DTO fields for target, source (if needed) and destination entities
        final ActionEntity target = ActionDTOUtil.getPrimaryEntity(action, true);
        final ServiceEntityApiDTO targetEntity = getServiceEntityDTO(context, target);

        wrapperDto.setTarget(targetEntity);

        final ChangeProvider primaryChange = ActionDTOUtil.getPrimaryChangeProvider(action).orElse(null);
        final boolean hasPrimarySource = !initialPlacement
                && (primaryChange != null) && primaryChange.getSource().hasId();
        if (hasPrimarySource) {
            long primarySourceId = primaryChange.getSource().getId();
            wrapperDto.setCurrentValue(Long.toString(primarySourceId));
            wrapperDto.setCurrentEntity(getServiceEntityDTO(context, primaryChange.getSource()));
            setRelatedDatacenter(primarySourceId, wrapperDto, context, false);
        } else {
            final Optional<ActionDTO.ActionEntity> primaryProviderOptional = ActionDTOUtil.getPrimaryProvider(action);
            if (primaryProviderOptional.isPresent()) {
                setCurrentAndNewToPrimaryProvider(primaryProviderOptional.get(), wrapperDto, context);
            } else {
                // For less brittle UI integration, we set the current entity to an empty object.
                // The UI sometimes checks the validity of the "currentEntity.uuid" field,
                // which throws an error if current entity is unset.
                wrapperDto.setCurrentEntity(new ServiceEntityApiDTO());
            }
        }
        if (primaryChange != null) {
            long primaryDestinationId = primaryChange.getDestination().getId();
            wrapperDto.setNewValue(Long.toString(primaryDestinationId));
            wrapperDto.setNewEntity(getServiceEntityDTO(context, primaryChange.getDestination()));
            setRelatedDatacenter(primaryDestinationId, wrapperDto, context, true);
        }

        List<ActionApiDTO> actions = Lists.newArrayList();
        final Collection<BaseApiDTO> resources = new HashSet<>();
        for (ChangeProvider change : ActionDTOUtil.getChangeProviderList(action)) {
            actions.add(singleMove(actionType, wrapperDto, target, change, context));
            change.getResourceList().forEach(resource -> resources.add(getServiceEntityDTO(context, resource)));
        }
        targetEntity.getConnectedEntities().addAll(resources);
        wrapperDto.addCompoundActions(actions);

        setReasonCommodities(wrapperDto.getRisk(), ActionDTOUtil.getReasonCommodities(action));

        // set current location, new location and cloud aspects for cloud resize actions
        if (target.getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.CLOUD) {
            // set location, which is the region
            setCurrentAndNewLocation(target.getId(), context, wrapperDto);

            // set cloud aspects to target entity
            final Map<AspectName, EntityAspect> aspects = new HashMap<>();
            context.getCloudAspect(target.getId()).map(cloudAspect -> aspects.put(
                AspectName.CLOUD, cloudAspect));
            wrapperDto.getTarget().setAspectsByName(aspects);
        }
    }

    /**
     * Set setting policy for ActionApiDTO if reason settings are associated with action.
     *
     * @param action the action
     * @param wrapperDto the actionApiDTO
     * @param context ActionSpecMappingContext
     */
    protected void populateSettingsPolicyForActionApiDto(@Nonnull final ActionDTO.Action action,
                                                         @Nonnull final ActionApiDTO wrapperDto,
                                                         @Nonnull final ActionSpecMappingContext context) {
        final Map<Long, BaseApiDTO> settingsPolicies = context.getSettingPolicyIdToBaseApiDto();
        List<BaseApiDTO> policies = new ArrayList<>();
        if (!settingsPolicies.isEmpty()) {
            RiskUtil.extractPolicyIds(action)
                    .forEach(p -> {
                        final BaseApiDTO policy = settingsPolicies.get(p);
                        if (policy != null) {
                            policies.add(policy);
                        }
                    });
            wrapperDto.setRelatedSettingsPolicies(policies);
        }
    }

    /**
     * Set policy for ActionApiDTO if the reason commodity associates with a segmentation policy.
     *
     * @param action the action
     * @param wrapperDto the actionApiDTO
     * @param context ActionSpecMappingContext
     */
    protected void populatePolicyForActionApiDto(@Nonnull final ActionDTO.Action action,
                                                 @Nonnull final ActionApiDTO wrapperDto,
                                                 @Nonnull final ActionSpecMappingContext context) {
        final Map<Long, PolicyApiDTO> policyApiDtoMap = context.getPolicyIdToApiDtoMap();
        if (policyApiDtoMap.isEmpty()) {
            return;
        }
        RiskUtil.extractPolicyIds(action)
                .stream()
                .map(policyApiDtoMap::get)
                .filter(Objects::nonNull)
                .findAny()
                .ifPresent(wrapperDto::setPolicy);
    }

    private ActionApiDTO singleMove(ActionType actionType, ActionApiDTO compoundDto,
                    final ActionEntity targetActionEntity,
                    @Nonnull final ChangeProvider change,
                    @Nonnull final ActionSpecMappingContext context) {
        ActionApiDTO actionApiDTO = new ActionApiDTO();
        actionApiDTO.setTarget(new ServiceEntityApiDTO());
        actionApiDTO.setCurrentEntity(new ServiceEntityApiDTO());
        actionApiDTO.setNewEntity(new ServiceEntityApiDTO());

        actionApiDTO.setActionMode(compoundDto.getActionMode());
        actionApiDTO.setActionState(compoundDto.getActionState());
        actionApiDTO.setDisplayName(compoundDto.getActionMode().name());

        final long destinationId = change.getDestination().getId();

        actionApiDTO.setActionType(actionType);
        // Set entity DTO fields for target, source (if needed) and destination entities
        actionApiDTO.setTarget(getServiceEntityDTO(context, targetActionEntity));

        final boolean hasSource = change.getSource().hasId();
        if (hasSource) {
            final long sourceId = change.getSource().getId();
            actionApiDTO.setCurrentValue(Long.toString(sourceId));
            actionApiDTO.setCurrentEntity(getServiceEntityDTO(context, change.getSource()));
        }
        actionApiDTO.setNewValue(Long.toString(destinationId));
        actionApiDTO.setNewEntity(getServiceEntityDTO(context, change.getDestination()));

        // Set action details
        actionApiDTO.setDetails(
                actionDetails(hasSource, actionApiDTO, targetActionEntity.getId(), change,
                        context));
        return actionApiDTO;
    }

    // This method should only be used for actions inside a compound move.
    // Other actions get their descriptions directly from the action orchestrator!
    // TODO (roman, Jan 30 2019) OM-54978: Remove this logic, and have AO be the source of truth.
    private String actionDetails(boolean hasSource, ActionApiDTO actionApiDTO, long targetId,
                                 ChangeProvider change, ActionSpecMappingContext context) {
        // If there is no source,
        // "Start Consumer type x on Supplier type y".
        // When there is a source, there are a few cases:
        // Cloud:
        // If a VM/DB is moving from one primary tier to another, then we say "Scale Virtual Machine VMName from x to y".
        // If a volume of a VM is moving from one storage type to another, then we say "Move Virtual Volume x of Virtual Machine VMName from y to z".
        // If a VM/DB is moving from one AZ to another, then we say "Move Virtual Machine VMName from AZ1 to AZ2".
        // On prem:
        // "Move Consumer Type x from y to z".
        if (!hasSource) {
            return MessageFormat.format("Start {0} on {1}",
                    readableEntityTypeAndName(actionApiDTO.getTarget()),
                    readableEntityTypeAndName(actionApiDTO.getNewEntity()));
        } else {
            long destinationId = change.getDestination().getId();
            long sourceId = change.getSource().getId();
            final Optional<ServiceEntityApiDTO> destination = context.getEntity(destinationId);
            final Optional<ServiceEntityApiDTO> source = context.getEntity(sourceId);
            final String verb =
                SCALE_TIER_VALUES.contains(destination.map(BaseApiDTO::getClassName).orElse(""))
                    && SCALE_TIER_VALUES.contains(source.map(BaseApiDTO::getClassName).orElse(""))
                    ? "Scale" : "Move";
            String resource = "";
            if (change.getResourceCount() > 0) {
                // only applies to compound moves
                List<String> resources = change.getResourceList().stream()
                                .map(ActionEntity::getId)
                                .filter(id -> id != targetId)
                                .map(id -> context.getEntity(id))
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .map(ServiceEntityApiDTO::getDisplayName)
                                .collect(Collectors.toList());
                if (!resources.isEmpty()) {
                    resource = resources.stream().sorted().collect(Collectors.joining(", "));
                }
            }
            return MessageFormat.format("{0} {1}{2} from {3} to {4}", verb, resource,
                    readableEntityTypeAndName(actionApiDTO.getTarget()),
                                    actionApiDTO.getCurrentEntity().getDisplayName(),
                                    actionApiDTO.getNewEntity().getDisplayName());

        }
    }

    private void addAllocateInfo(
            @Nonnull final ActionApiDTO actionApiDTO,
            @Nonnull final ActionSpec allocateActionSpec,
            @Nonnull final ActionSpecMappingContext context) {
        // Set action type
        actionApiDTO.setActionType(ActionType.ALLOCATE);

        // Set action target
        final ActionDTO.ActionInfo actionInfo = allocateActionSpec.getRecommendation().getInfo();
        final ActionEntity targetActionEntity = actionInfo.getAllocate().getTarget();
        actionApiDTO.setTarget(getServiceEntityDTO(context, targetActionEntity));

        // Set template family in current entity
        final String templateFamily =
                allocateActionSpec.getRecommendation().getExplanation().getAllocate().getInstanceSizeFamily();
        ServiceEntityApiDTO serviceEntityApiDTO = new ServiceEntityApiDTO();
        serviceEntityApiDTO.setDisplayName(templateFamily);
        serviceEntityApiDTO.setClassName(ApiEntityType.COMPUTE_TIER.apiStr());
        actionApiDTO.setCurrentEntity(serviceEntityApiDTO);

        // Set the template for the current entity
        final ActionEntity workloadTierActionEntity = actionInfo.getAllocate().getWorkloadTier();
        final ServiceEntityApiDTO workloadTier = getServiceEntityDTO(context, workloadTierActionEntity);
        TemplateApiDTO templateApiDTO = new TemplateApiDTO();
        templateApiDTO.setUuid(workloadTier.getUuid());
        templateApiDTO.setDisplayName(workloadTier.getDisplayName());
        templateApiDTO.setClassName(workloadTier.getClassName());
        actionApiDTO.setTemplate(templateApiDTO);

        // Set action current and new locations (should be the same for Allocate)
        setCurrentAndNewLocation(targetActionEntity.getId(), context, actionApiDTO);

        // Set Cloud aspect
        context.getCloudAspect(targetActionEntity.getId()).ifPresent(cloudAspect -> {
            final Map<AspectName, EntityAspect> aspects = new HashMap<>();
            aspects.put(AspectName.CLOUD, cloudAspect);
            actionApiDTO.getTarget().setAspectsByName(aspects);
        });
    }

    private void addReconfigureInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                    @Nonnull final ActionDTO.Action action,
                                    @Nonnull final Reconfigure reconfigure,
                                    @Nonnull final ActionSpecMappingContext context) {
        actionApiDTO.setActionType(ActionType.RECONFIGURE);
        final ActionEntity targetEntity = reconfigure.getTarget();

        actionApiDTO.setTarget(getServiceEntityDTO(context, targetEntity));
        // Since we may or may not have a current entity, we store the DC for the target as the
        // new location.  This way, the UI will always be able to find a DC in one of the two
        // places.
        setRelatedDatacenter(reconfigure.getTarget().getId(), actionApiDTO, context, true);
        if (reconfigure.hasSource()) {
            actionApiDTO.setCurrentEntity(getServiceEntityDTO(context, reconfigure.getSource()));
            setRelatedDatacenter(reconfigure.getSource().getId(), actionApiDTO, context, false);
        } else {
            // For less brittle UI integration, we set the current entity to an empty object.
            // The UI sometimes checks the validity of the "currentEntity.uuid" field,
            // which throws an error if current entity is unset.
            actionApiDTO.setCurrentEntity(new ServiceEntityApiDTO());
        }

        setReasonCommodities(actionApiDTO.getRisk(), ActionDTOUtil.getReasonCommodities(action));
        actionApiDTO.setCurrentValue(Long.toString(reconfigure.getSource().getId()));
    }

    private void addProvisionInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                  @Nonnull final Provision provision,
                                  @Nonnull final ActionDTO.Action action,
                                  @Nonnull final ActionSpecMappingContext context) {
        final ActionEntity currentEntity = provision.getEntityToClone();
        long currentEntityId = currentEntity.getId();
        final long provisionedSellerId = provision.getProvisionedSeller();

        actionApiDTO.setActionType(ActionType.PROVISION);

        actionApiDTO.setCurrentValue(Long.toString(currentEntityId));
        actionApiDTO.setCurrentEntity(getServiceEntityDTO(context, currentEntity));
        setRelatedDatacenter(currentEntityId, actionApiDTO, context, false);
        setRelatedDatacenter(currentEntityId, actionApiDTO, context, true);

        actionApiDTO.setTarget(getServiceEntityDTO(context, currentEntity));

        // Set reason commodities
        setReasonCommodities(actionApiDTO.getRisk(), ActionDTOUtil.getReasonCommodities(action));

        if (context.isPlan()) {
            // In plan actions we want to provide a reference to the provisioned entities, because
            // we will show other actions (e.g. moves/starts) that involve the provisioned entities.
            //
            // The "new" entity is the provisioned seller.
            final Optional<ServiceEntityApiDTO> provisionedEntity =
                    context.getEntity(provisionedSellerId);
            final ServiceEntityApiDTO newEntity;
            if (provisionedEntity.isPresent()) {
                newEntity = ServiceEntityMapper.copyServiceEntityAPIDTO(provisionedEntity.get());
            } else {
                logger.error("There is no provisioned entity {} in projected topology. Populate "
                        + "new entity using information from current entity.", provisionedSellerId);
                newEntity = new ServiceEntityApiDTO();
                newEntity.setUuid(String.valueOf(provisionedSellerId));
                newEntity.setClassName(ApiEntityType.fromType(currentEntity.getType()).apiStr());
            }
            actionApiDTO.setNewEntity(newEntity);
            actionApiDTO.setNewValue(newEntity.getUuid());
        } else {
            Optional<BaseApiDTO> vsanCause = context.getEntity(currentEntityId)
                    .map(currentDTO -> getVSANCause(action.getExplanation(), currentDTO)).orElse(Optional.empty());
            addVSANCausingProvisionInfo(actionApiDTO, vsanCause);
        }
    }

    /**
     * Add info about vSAN storage whose needs have triggered host provision to actionApiDTO.
     * @param actionApiDTO the resulting action description for API
     * @param vsan the vSAN that causes the action
     */
    private void addVSANCausingProvisionInfo(@Nonnull final ActionApiDTO actionApiDTO, @Nonnull final Optional<BaseApiDTO> vsan) {
        final ServiceEntityApiDTO newEntity = new ServiceEntityApiDTO();
        actionApiDTO.setNewEntity(newEntity);
        //If vsan is empty, that means it didn't pass the vsan action check, this is not a vsan caused action.
        //Don't add the vsan aspect information then.
        if (!vsan.isPresent()) {
            return;
        }
        newEntity.setUuid(vsan.get().getUuid());
        newEntity.setClassName(StringConstants.STORAGE);
        newEntity.setDisplayName(vsan.get().getDisplayName());
        newEntity.setEnvironmentType(EnvironmentTypeMapper.fromXLToApi(
                                    EnvironmentTypeEnum.EnvironmentType.ON_PREM));
        actionApiDTO.setNewEntity(newEntity);
        actionApiDTO.setNewValue(newEntity.getUuid());
    }

    /**
     * Find the VSAN that causes the action, if exists.
     * @param actionExplanation action explanation
     * @param currentEntity the current se of the action
     * @return Not null if we can find a consumer of STORAGE type
     */
    private Optional<BaseApiDTO> getVSANCause(@Nonnull final Explanation actionExplanation,
            final @Nonnull ServiceEntityApiDTO currentEntity) {
        if (!(actionExplanation.hasProvision()
                && actionExplanation.getProvision().hasProvisionBySupplyExplanation()
                && actionExplanation.getProvision().getProvisionBySupplyExplanation()
                .hasMostExpensiveCommodityInfo())) {
            return Optional.empty();
        }

        Explanation.ReasonCommodity reasonCommodity = actionExplanation.getProvision()
                .getProvisionBySupplyExplanation().getMostExpensiveCommodityInfo();
        if (!(reasonCommodity.hasCommodityType() && reasonCommodity.getCommodityType().hasType()
                && HCIUtils.isVSANRelatedCommodity(reasonCommodity.getCommodityType().getType()))) {
            return Optional.empty();
        }

        for (BaseApiDTO consumer : currentEntity.getConsumers())  {
            if (StringConstants.STORAGE.equals(consumer.getClassName())) {
                return Optional.of(consumer);
            }
        }
        return Optional.empty();

    }

    // Top level merged resize action
    private void addAtomicResizeInfo(@Nonnull final ActionApiDTO actionApiDTO,
                               @Nonnull final AtomicResize resize,
                               @Nonnull final ActionSpecMappingContext context) {

        actionApiDTO.setActionType(ActionType.RESIZE);
        logger.debug("Handling merged action spec {}", actionApiDTO.getActionID());

        // Target entity for the action
        final ActionEntity targetEntity = resize.getExecutionTarget();
        actionApiDTO.setTarget(getServiceEntityDTO(context, targetEntity));
        actionApiDTO.setCurrentEntity(getServiceEntityDTO(context, targetEntity));
        actionApiDTO.setNewEntity(getServiceEntityDTO(context, targetEntity));

        setRelatedDatacenter(targetEntity.getId(), actionApiDTO, context, false);
        setRelatedDatacenter(targetEntity.getId(), actionApiDTO, context, true);

        //list of actions based of the resize action info list
        List<ActionApiDTO> actions = Lists.newArrayList();
        for (ResizeInfo resizeInfo : resize.getResizesList()) {
            actions.add(singleResize(actionApiDTO, resizeInfo, context));
        }
        actionApiDTO.addCompoundActions(actions);
    }

    // Resize details for each resize action that was merged
    // that will be added to the compound action DTO
    private ActionApiDTO singleResize(ActionApiDTO compoundDto,
                                    @Nonnull final ResizeInfo resizeInfo,
                                    @Nonnull final ActionSpecMappingContext context) {
        ActionApiDTO actionApiDTO = new ActionApiDTO();
        actionApiDTO.setTarget(new ServiceEntityApiDTO());

        actionApiDTO.setActionMode(compoundDto.getActionMode());
        actionApiDTO.setActionState(compoundDto.getActionState());
        actionApiDTO.setDisplayName(compoundDto.getActionMode().name());

        actionApiDTO.setActionType(ActionType.RESIZE);

        final ActionEntity originalEntity = resizeInfo.getTarget();
        final ActionEntity targetEntity = resizeInfo.getTarget();

        actionApiDTO.setTarget(getServiceEntityDTO(context, targetEntity));
        setRelatedDatacenter(originalEntity.getId(), actionApiDTO, context, false);
        setRelatedDatacenter(originalEntity.getId(), actionApiDTO, context, true);

        final CommodityDTO.CommodityType commodityType = CommodityDTO.CommodityType.forNumber(
                resizeInfo.getCommodityType().getType());
        Objects.requireNonNull(commodityType, "Commodity for number "
                + resizeInfo.getCommodityType().getType());

        final CommodityAttribute commodityAttribute =
                        CommodityTypeMapping.transformEnum(resizeInfo,
                                        ResizeInfo::hasCommodityAttribute,
                                        ResizeInfo::getCommodityAttribute,
                                        CommodityAttribute.CAPACITY,
                                        actionApiDTO::setResizeAttribute);
        actionApiDTO.setCurrentValue(formatActionValues(commodityType.getNumber(), resizeInfo.getOldCapacity()));
        actionApiDTO.setNewValue(formatActionValues(commodityType.getNumber(), resizeInfo.getNewCapacity()));
        // set units if available

        CommodityTypeMapping.getCommodityUnitsForActions(resizeInfo.getCommodityType().getType(),
                                        resizeInfo.getTarget().getType(), commodityAttribute.getNumber())
                        .ifPresent(actionApiDTO::setValueUnits);

        // set current location, new location and cloud aspects for cloud resize actions
        if (resizeInfo.getTarget().getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.CLOUD) {
            // set location, which is the region
            setCurrentAndNewLocation(resizeInfo.getTarget().getId(), context, actionApiDTO);
            // set cloud aspects to target entity
            final Map<AspectName, EntityAspect> aspects = new HashMap<>();
            context.getCloudAspect(resizeInfo.getTarget().getId()).map(cloudAspect -> aspects.put(
                    AspectName.CLOUD, cloudAspect));
            actionApiDTO.getTarget().setAspectsByName(aspects);
        }

        // Set reason commodity
        // map the recommendation info
        LogEntryApiDTO risk = new LogEntryApiDTO();
        risk.setImportance((float)0.0);
        risk.addReasonCommodity(UICommodityType.fromType(resizeInfo.getCommodityType()).apiStr());
        actionApiDTO.setRisk(risk);

        // Set action details
        actionApiDTO.setDetails(resizeDetails(actionApiDTO, resizeInfo));
        return actionApiDTO;
    }

    private String resizeDetails(ActionApiDTO actionApiDTO, ResizeInfo resizeInfo) {

        final String commType = UICommodityType.fromType(resizeInfo.getCommodityType()).displayName()
                + (resizeInfo.getCommodityAttribute() == CommodityAttribute.RESERVED ? " reservation" : "");

        StringBuilder actionDetails = new StringBuilder();
        final boolean isResizeDown = resizeInfo.getOldCapacity() > resizeInfo.getNewCapacity();
        if (isResizeDown) {
            actionDetails.append("Underutilized " + commType);
        } else {
            actionDetails.append(commType + " Congestion");
        }

        String message = MessageFormat.format("{0} in {1}", actionDetails.toString(),
                    actionApiDTO.getTarget().getDisplayName());

        return message;
    }

    private void addResizeInfo(@Nonnull final ActionApiDTO actionApiDTO,
            @Nonnull final Action action, @Nonnull final Resize resize,
            @Nonnull final ActionSpecMappingContext context) {
        actionApiDTO.setActionType(ActionType.RESIZE);

        final ActionEntity originalEntity = resize.getTarget();
        actionApiDTO.setTarget(getServiceEntityDTO(context, originalEntity));
        setRelatedDatacenter(originalEntity.getId(), actionApiDTO, context, false);
        setRelatedDatacenter(originalEntity.getId(), actionApiDTO, context, true);

        setReasonCommodities(actionApiDTO.getRisk(), ActionDTOUtil.getReasonCommodities(action));

        final CommodityAttribute commodityAttribute =
                        CommodityTypeMapping.transformEnum(resize,
                                        Resize::hasCommodityAttribute,
                                        Resize::getCommodityAttribute, CommodityAttribute.CAPACITY,
                                        actionApiDTO::setResizeAttribute);
        int commodityType = resize.getCommodityType().getType();
        actionApiDTO.setCurrentValue(formatActionValues(commodityType, resize.getOldCapacity()));
        actionApiDTO.setNewValue(formatActionValues(commodityType, resize.getNewCapacity()));
        // set units if available
        CommodityTypeMapping.getCommodityUnitsForActions(commodityType, null,
                        commodityAttribute.getNumber()).ifPresent(actionApiDTO::setValueUnits);

        // set current location, new location and cloud aspects for cloud resize actions
        if (resize.getTarget().getEnvironmentType() == EnvironmentTypeEnum.EnvironmentType.CLOUD) {
            // set location, which is the region
            setCurrentAndNewLocation(resize.getTarget().getId(), context, actionApiDTO);
            // set cloud aspects to target entity
            final Map<AspectName, EntityAspect> aspects = new HashMap<>();
            context.getCloudAspect(resize.getTarget().getId()).map(cloudAspect -> aspects.put(
                AspectName.CLOUD, cloudAspect));
            actionApiDTO.getTarget().setAspectsByName(aspects);
        }
    }

    /**
     * Set current and new location of target entity in cloud scale actions.
     * @param targetUuid - uuid of action target
     * @param context - ActionSpecMappingContext for given action
     * @param actionApiDTO - result actionApiDTO
     */
    private void setCurrentAndNewLocation(long targetUuid, ActionSpecMappingContext context, ActionApiDTO actionApiDTO) {
        ApiPartialEntity region = context.getRegion(targetUuid);
        if (region != null) {
            context.getDiscoveredEntity(region.getOid()).ifPresent(regionDTO -> {
                actionApiDTO.setCurrentLocation(regionDTO);
                actionApiDTO.setNewLocation(regionDTO);
            });
        }

        // If there is a new region in compound action, set newLocation based on that.
        final List<ActionApiDTO> compoundActions = actionApiDTO.getCompoundActions();
        if (CollectionUtils.isEmpty(compoundActions)) {
            return;
        }
        Optional<ServiceEntityApiDTO> newLocation = compoundActions
                .stream()
                .map(ActionApiDTO::getNewEntity)
                .filter(seApiDto -> seApiDto != null && StringConstants.REGION.equals(
                        seApiDto.getClassName()))
                .findFirst();
        newLocation.ifPresent(actionApiDTO::setNewLocation);
    }

    /**
     * Adds information to a RI Buy Action.
     * @param actionApiDTO Action API DTO.
     * @param buyRI Buy RI DTO.
     * @param context ActionSpecMappingContext.
     */
    private void addBuyRIInfo(@Nonnull final ActionApiDTO actionApiDTO,
                              @Nonnull final BuyRI buyRI,
                              @Nonnull final ActionSpecMappingContext context) {
        actionApiDTO.setActionType(ActionType.BUY_RI);

        final Pair<ReservedInstanceBought, ReservedInstanceSpec> pair = context
                .getRIBoughtandRISpec(buyRI.getBuyRiId());

        final ReservedInstanceBought ri = pair.first;
        final ReservedInstanceSpec riSpec = pair.second;

        try {
            ReservedInstanceApiDTO riApiDTO = reservedInstanceMapper.mapToReservedInstanceApiDTO(ri,
                    riSpec, context.getServiceEntityApiDTOs(),
                    null, null, null);
            actionApiDTO.setReservedInstance(riApiDTO);
            actionApiDTO.setTarget(getServiceEntityDTO(context, buyRI.getMasterAccount()));
            actionApiDTO.setCurrentLocation(getServiceEntityDTO(context, buyRI.getRegion()));
            // set current/new location as DiscoveredEntityDTO
            ApiPartialEntity region = context.getRegion(buyRI.getRegion().getId());
            if (region != null) {
                context.getDiscoveredEntity(region.getOid()).ifPresent(actionApiDTO::setCurrentLocation);
            }
            // For less brittle UI integration, we set the current entity to an empty object.
            // The UI sometimes checks the validity of the "currentEntity.uuid" field,
            // which throws an error if current entity is unset.
            actionApiDTO.setCurrentEntity(new ServiceEntityApiDTO());
            //We need to add a newEntity field for RI buy to complete pending_action csv file.
            ServiceEntityApiDTO newEntity = new ServiceEntityApiDTO();
            newEntity.setUuid(riApiDTO.getTemplate().getUuid());
            newEntity.setDisplayName(riApiDTO.getTemplate().getDisplayName());
            newEntity.setClassName(riApiDTO.getClassName());
            actionApiDTO.setNewEntity(newEntity);
            actionApiDTO.setNewValue(formatBuyRINewValue(riApiDTO));
        } catch (NotFoundMatchPaymentOptionException e) {
            logger.error("Payment Option not found for RI : {}", buyRI.getBuyRiId(),  e);
        } catch (NotFoundMatchTenancyException e) {
            logger.error("Tenancy not found for RI : {}", buyRI.getBuyRiId(), e);
        } catch (NotFoundMatchOfferingClassException e) {
            logger.error("Offering Class not found for RI : {}", buyRI.getBuyRiId(), e);
        } catch (NotFoundCloudTypeException e) {
            logger.error("Cannot identify Cloud Type for RI : {}", buyRI.getBuyRiId(), e);
        }
    }

    /**
     * Format the RIBuy information into a String to show in pending action table.
     * @param ri ReservedInstanceApiDTO to format
     * @return a String contains all the necessary information to buy an RI
     */
    private String formatBuyRINewValue(ReservedInstanceApiDTO ri) {
        String platform = ri.getPlatform().name();
        String payment = ri.getPayment().name();
        String type = ri.getType().name();
        String termUnit = ri.getTerm().getUnits();
        int term = (int)Math.floor(ri.getTerm().getValue());
        int instanceCount = ri.getInstanceCount();
        String templateName = ri.getTemplate().getDisplayName();
        return String.format("%d %s(%s, %d%s, %s, %s)", instanceCount, templateName, platform, term, termUnit, payment, type);
    }

    private void addActivateInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                 @Nonnull final ActionDTO.Action action,
                                 @Nonnull final ActionSpecMappingContext context) {
        actionApiDTO.setActionType(ActionType.START);
        final ActionEntity targetEntity = action.getInfo().getActivate().getTarget();
        actionApiDTO.setTarget(getServiceEntityDTO(context, targetEntity));
        actionApiDTO.setCurrentEntity(getServiceEntityDTO(context, targetEntity));
        setRelatedDatacenter(targetEntity.getId(), actionApiDTO, context, false);

        setReasonCommodities(actionApiDTO.getRisk(), ActionDTOUtil.getReasonCommodities(action));
    }

    private void setReasonCommodities(LogEntryApiDTO risk, Stream<ReasonCommodity> reasonCommodities) {
        // Convert a collection of reason commodities to a set of string.
        final Set<String> reasonCommodityNames = reasonCommodities
                .map(ReasonCommodity::getCommodityType)
                .map(UICommodityType::fromType)
                .map(UICommodityType::apiStr)
                .collect(Collectors.toSet());
        if (!reasonCommodityNames.isEmpty()) {
            risk.addAllReasonCommodities(reasonCommodityNames);
        }
    }

    private void addDeactivateInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                   @Nonnull final ActionDTO.Action action,
                                   @Nonnull final ActionSpecMappingContext context) {
        final ActionEntity targetEntity = action.getInfo().getDeactivate().getTarget();
        actionApiDTO.setTarget(getServiceEntityDTO(context, targetEntity));
        actionApiDTO.setCurrentEntity(getServiceEntityDTO(context, targetEntity));
        setRelatedDatacenter(targetEntity.getId(), actionApiDTO, context, false);

        actionApiDTO.setActionType(ActionType.SUSPEND);

        setReasonCommodities(actionApiDTO.getRisk(), ActionDTOUtil.getReasonCommodities(action));
    }

    /**
     * Add information related to a Delete action to the actionApiDTO.  Note that this currently only
     * handles on prem wasted file delete actions.
     *
     * @param actionApiDTO the {@link ActionApiDTO} we are populating
     * @param delete the {@link Delete} action info object that contains the basic delete action parameters
     * @param deleteExplanation the {@link DeleteExplanation} that contains the details of the action
     * @param context the {@link ActionSpecMappingContext}
     */
    private void addDeleteInfo(@Nonnull final ActionApiDTO actionApiDTO,
                               @Nonnull final Delete delete,
                               @Nonnull final DeleteExplanation deleteExplanation,
                               @Nonnull final ActionSpecMappingContext context) {
        final ActionEntity targetEntity = delete.getTarget();
        actionApiDTO.setTarget(getServiceEntityDTO(context, targetEntity));
        actionApiDTO.setCurrentEntity(getServiceEntityDTO(context, targetEntity));
        actionApiDTO.setActionType(ActionType.DELETE);
        final long deletedSizeinKB = deleteExplanation.getSizeKb();
        if (deletedSizeinKB > 0) {
            final double deletedSizeInMB = deletedSizeinKB / (double)Units.NUM_OF_KB_IN_MB;
            actionApiDTO.setCurrentValue(String.format(FORMAT_FOR_ACTION_VALUES, deletedSizeInMB));
            actionApiDTO.setValueUnits("MB");
        }
        // set the virtualDisks field on ActionApiDTO, only one VirtualDiskApiDTO should be set,
        // since there is only one file (on-prem) or volume (cloud) associated with DELETE action
        if (delete.hasFilePath()) {
            final VirtualDiskApiDTO virtualDiskApiDTO = new VirtualDiskApiDTO();
            virtualDiskApiDTO.setDisplayName(delete.getFilePath());
            virtualDiskApiDTO.setLastModified(deleteExplanation.getModificationTimeMs());
            actionApiDTO.setVirtualDisks(Collections.singletonList(virtualDiskApiDTO));
        }
    }

    /**
     * Return a nicely formatted string like:
     *
     * <p><code>Virtual Machine vm-test 01 for now</code></p>
     *
     * <p>in which the entity type is expanded from camel case to words, and the displayName()
     * is surrounded with single quotes.</p>
     *
     * <p>The regex uses zero-length pattern matching with lookbehind and lookforward, and is
     * taken from - http://stackoverflow.com/questions/2559759.</p>
     *
     * <p>It converts camel case (e.g. PhysicalMachine) into strings with the same
     * capitalization plus blank spaces (e.g. "Physical Machine"). It also splits numbers,
     * e.g. "May5" -> "May 5" and respects upper case runs, e.g. (PDFLoader -> "PDF Loader").</p>
     *
     * @param entityDTO the entity for which the readable name is to be created
     * @return a string with the entity type, with blanks inserted, plus displayName with
     * single quotes
     */
    private String readableEntityTypeAndName(BaseApiDTO entityDTO) {
        final String fullType = entityDTO.getClassName();
        final String shortenedIfNecessary = SHORTENED_ENTITY_TYPES.getOrDefault(fullType, fullType);
        final String entityType = StringUtil.getSpaceSeparatedWordsFromCamelCaseString(shortenedIfNecessary);
        return entityType + " " + entityDTO.getDisplayName();
    }

    /**
     * Creates an {@link ActionQueryFilter} instance based on a given {@link ActionApiInputDTO},
     * an oid collection of involved entities and selected scope.
     *
     * @param inputDto The {@link ActionApiInputDTO} instance, where only action states are used.
     * @param involvedEntities The oid collection of involved entities.
     * @param scopeId {@code ApiId} for scoped entity.
     * @return The {@link ActionQueryFilter} instance.
     */
    public ActionQueryFilter createActionFilter(@Nullable final ActionApiInputDTO inputDto,
                                                @Nonnull final Optional<Set<Long>> involvedEntities,
                                                @Nullable final ApiId scopeId) {
        ActionQueryFilter.Builder queryBuilder = ActionQueryFilter.newBuilder()
            .setVisible(true);

        if (inputDto != null) {
            populateDateInput(inputDto, queryBuilder);

            if (inputDto.getActionStateList() != null) {
                inputDto.getActionStateList().stream()
                    .map(ActionSpecMapper::mapApiStateToXl)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(queryBuilder::addStates);
            } else {
                // TODO: (DavidBlinn, 3/15/2018): The UI request for "Pending Actions" does not
                // include any action states in its filter even though it wants to exclude executed
                // actions. Request only operational action states.
                OPERATIONAL_ACTION_STATES.forEach(queryBuilder::addStates);
            }

            if (inputDto.getRiskSeverityList() != null) {
                inputDto.getRiskSeverityList().stream()
                    .map(ActionSpecMapper::mapApiSeverityToXl)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(queryBuilder::addSeverities);
            }

            // Map UI's ActionMode to ActionDTO.ActionMode and add them to filter
            if (inputDto.getActionModeList() != null) {
                inputDto.getActionModeList().stream()
                    .map(ActionSpecMapper::mapApiModeToXl)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(queryBuilder::addModes);
            }

            queryBuilder.addAllTypes(buyRiScopeHandler.extractActionTypes(inputDto, scopeId));

            if (CollectionUtils.isNotEmpty(inputDto.getRiskSubCategoryList())) {
                inputDto.getRiskSubCategoryList().forEach(apiCategory -> {
                    Optional<ActionDTO.ActionCategory> xlCategory = mapApiActionCategoryToXl(apiCategory);
                    if (xlCategory.isPresent()) {
                        queryBuilder.addCategories(xlCategory.get());
                    } else {
                        logger.warn("Unable to map action category {} to XL category.", apiCategory);
                    }
                });
            }

            if (inputDto.getEnvironmentType() != null) {
                queryBuilder.setEnvironmentType(EnvironmentTypeMapper.fromApiToXL(inputDto.getEnvironmentType()));
            }

            if (CollectionUtils.isNotEmpty(inputDto.getRelatedEntityTypes())) {
                inputDto.getRelatedEntityTypes().stream()
                    .map(ApiEntityType::fromString)
                    .map(ApiEntityType::typeNumber)
                    .forEach(queryBuilder::addEntityType);
            }

            if (inputDto.getCostType() != null) {
                queryBuilder.setCostType(ActionSpecMapper.mapApiCostTypeToXL(inputDto.getCostType()));
            }

            // TODO either toRegexQuery needs to be reimplemented to more portable,
            // or we need to stop delegating regex's to databases
            if (inputDto.getDescriptionQuery() != null && Strings.isNotEmpty(inputDto.getDescriptionQuery().getQuery())) {
                queryBuilder.setDescriptionQuery(inputDto.getDescriptionQuery().toRegexQuery());
            }

            if (inputDto.getRiskQuery() != null && Strings.isNotEmpty(inputDto.getRiskQuery().getQuery())) {
                queryBuilder.setRiskQuery(inputDto.getRiskQuery().toRegexQuery());
            }

            if (inputDto.getExecutionCharacteristics() != null) {
                if (inputDto.getExecutionCharacteristics().getDisruptiveness() != null) {
                    queryBuilder.setDisruptiveness(ActionSpecMapper.mapApiDisruptivenessToXL(inputDto.getExecutionCharacteristics().getDisruptiveness()));
                }

                if (inputDto.getExecutionCharacteristics().getReversibility() != null) {
                    queryBuilder.setReversibility(ActionSpecMapper.mapApiReversibilityToXL(inputDto.getExecutionCharacteristics().getReversibility()));
                }
            }

            if (inputDto.getSavingsAmountRange() != null && (inputDto.getSavingsAmountRange().getMinValue() != null || inputDto.getSavingsAmountRange().getMaxValue() != null)) {
                ActionSavingsAmountRangeFilter.Builder savingsAmountRangeFilter = ActionSavingsAmountRangeFilter.newBuilder();
                if (inputDto.getSavingsAmountRange().getMinValue() != null) {
                    savingsAmountRangeFilter.setMinValue(inputDto.getSavingsAmountRange().getMinValue().doubleValue());
                }

                if (inputDto.getSavingsAmountRange().getMaxValue() != null) {
                    savingsAmountRangeFilter.setMaxValue(inputDto.getSavingsAmountRange().getMaxValue().doubleValue());
                }
                queryBuilder.setSavingsAmountRange(savingsAmountRangeFilter);
            }

            if (inputDto.getHasSchedule() != null) {
                queryBuilder.setHasSchedule(inputDto.getHasSchedule());
            }

            if (inputDto.getHasPrerequisites() != null) {
                queryBuilder.setHasPrerequisites(inputDto.getHasPrerequisites());
            }

            if (CollectionUtils.isNotEmpty(inputDto.getActionRelationTypeFilter())) {
                inputDto.getActionRelationTypeFilter().forEach(apiRelatedAction -> {
                    Optional<ActionDTO.ActionRelationType> xlRelatedActionType
                            = RelatedActionMapper.mapApiActionRelationTypeEnumToXl(apiRelatedAction);
                    if (xlRelatedActionType.isPresent()) {
                        queryBuilder.addRelationTypes(xlRelatedActionType.get());
                    } else {
                        logger.warn("Unable to map RelatedActionType {} to XL RelatedActionType.", xlRelatedActionType);
                    }
                });
            }

            final List<String> relatedCloudServiceProviderIds = inputDto.getRelatedCloudServiceProviderIds();
            if (relatedCloudServiceProviderIds != null && !relatedCloudServiceProviderIds.isEmpty()) {
                relatedCloudServiceProviderIds.stream()
                    .filter(StringUtils::isNumeric)
                    .map(Long::parseLong)
                    .forEach(queryBuilder::addRelatedCloudServiceProviderIds);
            }
        } else {
            // When "inputDto" is null, we should automatically insert the operational action states.
            OPERATIONAL_ACTION_STATES.forEach(queryBuilder::addStates);
        }

        // Set involved entities from user input and Buy RI scope
        final Set<Long> allInvolvedEntities = new HashSet<>(
                buyRiScopeHandler.extractBuyRiEntities(scopeId));
        involvedEntities.ifPresent(allInvolvedEntities::addAll);
        if (!allInvolvedEntities.isEmpty()) {
            queryBuilder.setInvolvedEntities(InvolvedEntities.newBuilder()
                    .addAllOids(allInvolvedEntities));
        }

        // Set organization scope filter
        setScopeOrganizationQueryFilters(scopeId, queryBuilder);

        return queryBuilder.build();
    }

    /**
     * Set organizational related scope filters in ActionQueryFilter.
     *
     * <p>At present UI supports one scope views, however, this method can be reworked to support
     * multiple scopes, as fetching from multiple scopes is supported in the db.
     * @param scopeId the api scope id.
     * @param queryBuilder The action query filter builder.
     * @return true if any filters were set, false otherwise;
     */
    private boolean setScopeOrganizationQueryFilters(@Nullable final ApiId scopeId,
                                                      @Nonnull final ActionQueryFilter.Builder queryBuilder) {
        // Set Account Filter if scope is a supported scope or group.
        if (scopeId != null && !scopeId.isRealtimeMarket()
            && scopeId.getScopeTypes() != null
            && scopeId.getScopeTypes().isPresent()) {
            // Add blocks for other supported scopes here.
            Set<ApiEntityType> scopeType = scopeId.getScopeTypes().get();
            if (scopeType.contains(ApiEntityType.BUSINESS_ACCOUNT)) {
                if (scopeId.isGroup()) { // Group of Business Accounts
                    queryBuilder.setAccountFilter(AccountFilter.newBuilder()
                            .addAllAccountId(groupExpander.expandOids(Collections
                                    .singleton(scopeId))));
                } else { // Single Business Account
                    queryBuilder.setAccountFilter(AccountFilter.newBuilder()
                            .addAllAccountId(Collections.singleton(scopeId.oid())));
                }
                return true;
            } else if (scopeId.isResourceGroupOrGroupOfResourceGroups()) {
                if (scopeId.getGroupType().isPresent()) {
                    switch (scopeId.getGroupType().get()) {
                        case RESOURCE: // Single Resouce Group
                            // Single Resource Group
                            queryBuilder.setResourceGroupFilter(ResourceGroupFilter.newBuilder()
                                    .addAllResourceGroupOid(Collections
                                            .singleton(scopeId.oid())));
                            break;
                        case REGULAR:
                            // Group of Resource Groups
                            Optional<GroupAndMembers> groupAndMembers =
                                    groupExpander.getGroupWithImmediateMembersOnly(Long.toString(scopeId.oid()));
                            groupAndMembers.ifPresent(g -> queryBuilder.setResourceGroupFilter(ResourceGroupFilter
                                                                    .newBuilder()
                                                        .addAllResourceGroupOid(g.members())));
                            break;
                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Handles time in the translation of an {@link ActionApiInputDTO}
     * object to an {@link ActionQueryFilter} object.
     * If the start time is not empty and the end time is empty,
     * the end time is set to "now".
     *
     * <p>Some cases should not appear:
     * <ul>
     *     <li>If startTime is in the future.</li>
     *     <li>If endTime precedes startTime.</li>
     *     <li>If endTime only was passed.</li>
     * </ul>
     * In these cases an {@link IllegalArgumentException} is thrown.
     * </p>
     *
     * @param input the {@link ActionApiDTO} object.
     * @param queryBuilder the {@link ActionQueryFilter.Builder} object
     *                     whose time entries must be populated
     */
    private static void populateDateInput(@Nonnull ActionApiInputDTO input,
                                          @Nonnull final ActionQueryFilter.Builder queryBuilder) {
        final String startTimeString = input.getStartTime();
        final String endTimeString = input.getEndTime();
        final String nowString = DateTimeUtil.getNow();

        if (!StringUtils.isEmpty(startTimeString)) {
            final long startTime = DateTimeUtil.parseTime(startTimeString);
            final long now = DateTimeUtil.parseTime(nowString);

            if (startTime > now) {
                // start time is in the future.
                throw new IllegalArgumentException("startTime " + startTimeString
                        + " can't be in the future");
            }
            queryBuilder.setStartDate(startTime);

            if (!StringUtils.isEmpty(endTimeString)) {
                final long endTime = DateTimeUtil.parseTime(endTimeString);
                if (endTime < startTime) {
                    // end time is before start time
                    throw new IllegalArgumentException("startTime " + startTimeString
                            + " must precede endTime " + endTimeString);
                }
                queryBuilder.setEndDate(endTime);
            } else {
                // start time is set, but end time is null
                // end time should be set to now
                queryBuilder.setEndDate(now);
            }
        } else if (!StringUtils.isEmpty(endTimeString)) {
            // start time is set, but end time is not
            throw new IllegalArgumentException("startTime is required along with endTime");
        }
    }

    @Nonnull
    private static Optional<Severity> mapApiSeverityToXl(@Nonnull String apiSeverity) {
        try {
            return Optional.of(Severity.valueOf(apiSeverity.toUpperCase()));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    private static String formatActionValues(int type, float value) {
        return String.format(
                COMMODITY_TYPE_TO_FORMAT_FOR_ACTION_VALUES
                        .getOrDefault(type, FORMAT_FOR_ACTION_VALUES), value);
    }

    /**
     * Translates a {@link Severity} into a human-readable string
     * to be returned by the API.
     *
     * @param severity the severity in internal XL format
     * @return the API corresponding string
     */
    @Nonnull
    public static String mapSeverityToApi(@Nonnull Severity severity) {
        return severity.name();
    }

    /**
     * Converts the api action state into xl action state.
     *
     * @param apiActionState the api's version of the action state.
     * @return the api action state represented in xl action state.
     */
    @Nonnull
    public static Optional<ActionDTO.ActionState> mapApiStateToXl(final ActionState apiActionState) {
        switch (apiActionState) {
            case READY:
                return Optional.of(ActionDTO.ActionState.READY);
            case ACCEPTED:
                return Optional.of(ActionDTO.ActionState.ACCEPTED);
            case QUEUED:
                return Optional.of(ActionDTO.ActionState.QUEUED);
            case SUCCEEDED:
                return Optional.of(ActionDTO.ActionState.SUCCEEDED);
            case IN_PROGRESS:
                return Optional.of(ActionDTO.ActionState.IN_PROGRESS);
            case FAILED:
                return Optional.of(ActionDTO.ActionState.FAILED);
            case CLEARED:
                return Optional.of(ActionDTO.ActionState.CLEARED);
            default:
                logger.error("Unknown action state {}", apiActionState);
                throw new IllegalArgumentException("Unsupported action state " + apiActionState);
        }
    }


    /**
     * Given an actionSpec fetches the corresponding action details.
     * @param action - ActionOrchestratorAction for the user sent action
     * @param topologyContextId - the topology context that the action corresponds to
     * @return actionDetailsApiDTO which contains extra information about a given action
     */
    @Nullable
    public ActionDetailsApiDTO createActionDetailsApiDTO(
            final ActionDTO.ActionOrchestratorAction action, Long topologyContextId) {

        Map<String, ActionDetailsApiDTO> dtoMap = createActionDetailsApiDTO(Collections.singleton(action), topologyContextId);
        return dtoMap.get(Long.toString(getActionId(action.getActionId(),
                action.getActionSpec().getRecommendationId(), topologyContextId)));
    }


    /**
     * Given a list of actionSpecs fetches the corresponding action details.
     * @param actions - collection of ActionOrchestratorAction objects for the user sent actions
     * @param topologyContextId - the topology context that the action corresponds to
     * @return actionDetailsApiDTO which contains extra information about a given action
     */
    @Nullable
    public Map<String, ActionDetailsApiDTO> createActionDetailsApiDTO(
            final Collection<ActionOrchestratorAction> actions, Long topologyContextId) {
        Map<String, Long> resizeCloudVMActionToVMUuidMap = new HashMap<>();
        Map<String, Long> scaleCloudVolumeActionToVolumeUuidMap = new HashMap<>();

        Map<String, ActionDetailsApiDTO> response = new HashMap<>();
        Map<String, Long> provisionCloudVMActionToVMUuidMap = new HashMap<>();
        Map<String, Long> suspendCloudVMActionToVMUuidMap = new HashMap<>();

        for (ActionOrchestratorAction action: actions) {
            @Nonnull final ActionSpec actionSpec = action.getActionSpec();
            final String actionIdString = Long.toString(getActionId(action.getActionId(),
                    action.getActionSpec().getRecommendationId(), topologyContextId));
            if (!actionSpec.hasRecommendation()) {
                response.put(actionIdString, new NoDetailsApiDTO());
                continue;
            }
            // Get the recommended action
            @Nonnull final ActionDTO.Action recommendation = actionSpec.getRecommendation();
            // Get primary entity of the action
            ActionEntity entity;
            try {
                entity = ActionDTOUtil.getPrimaryEntity(recommendation);
            } catch (UnsupportedActionException e) {
                logger.warn("Cannot create action details due to unsupported action type", e);
                continue;
            }

            final long entityUuid = entity.getId();
            final int entityType = entity.getType();
            @Nonnull final ActionDTO.ActionType actionType = ActionDTOUtil.getActionInfoActionType(recommendation);
            // Buy RI action - set est. on-demand cost and coverage values + historical demand data
            if (recommendation.hasExplanation() && actionType.equals(ActionDTO.ActionType.BUY_RI)) {
                RIBuyActionDetailsApiDTO detailsDto = new RIBuyActionDetailsApiDTO();
                // set est RI Coverage
                ActionDTO.Explanation.BuyRIExplanation buyRIExplanation = recommendation.getExplanation().getBuyRI();
                final double covered = buyRIExplanation.getCoveredAverageDemand();
                final double capacity = buyRIExplanation.getTotalAverageDemand();
                detailsDto.setEstimatedRICoverage((float)(covered / capacity) * 100);
                // set est. on-demand cost
                detailsDto.setEstimatedOnDemandCost((float)buyRIExplanation.getEstimatedOnDemandCost());
                // set demand data
                Cost.riBuyDemandStats snapshots = riStub
                        .getRIBuyContextData(Cost.GetRIBuyContextRequest.newBuilder()
                                .setActionId(Long.toString(recommendation.getId())).build());
                List<StatSnapshotApiDTO> demandList = createRiHistoricalContextStatSnapshotDTO(
                        snapshots.getStatSnapshotsList());
                detailsDto.setHistoricalDemandData(demandList);
                response.put(actionIdString, detailsDto);
                continue;
            } else if (entity.getEnvironmentType() == EnvironmentType.ON_PREM
                            && recommendation.hasInfo()) {
                //For on-prem vcpu resize, we need to populate action details to show it in UX.
                ActionInfo actionInfo = recommendation.getInfo();
                if (actionType == ActionDTO.ActionType.RESIZE && actionInfo.hasResize()) {
                    Resize resize = actionInfo.getResize();
                    if (resize.getCommodityType().getType() == CommodityType.VCPU_VALUE && resize.getCommodityAttribute() == CommodityAttribute.CAPACITY) {
                        response.put(actionIdString, createOnPremResizeActionDetails(resize));
                    }
                    continue;
                } else if (actionType == ActionDTO.ActionType.RECONFIGURE
                                && actionInfo.hasReconfigure() && actionInfo
                                                .getReconfigure().getSettingChangeCount() > 1) {
                    response.put(actionIdString, createOnPremReconfigureActionDetails(
                                    actionInfo.getReconfigure()));
                    continue;
                }
            }

            // Skip if the entity is not a cloud entity
            if (entity.getEnvironmentType() != EnvironmentTypeEnum.EnvironmentType.CLOUD) {
                continue;
            }
            // We show action details for
            // - Cloud VM Provision
            // - Cloud VM Suspend/Deactivate
            // - Cloud Migration Move across region
            // - Cloud Volume Scale
            // - Cloud Resize
            final ActionInfo info = recommendation.getInfo();
            switch (actionType) {
                case PROVISION:
                    if (entityType == EntityType.VIRTUAL_MACHINE_VALUE) {
                        provisionCloudVMActionToVMUuidMap.put(actionIdString, entityUuid);
                    }
                    break;
                case DEACTIVATE:
                case SUSPEND:
                    if (entityType == EntityType.VIRTUAL_MACHINE_VALUE) {
                        suspendCloudVMActionToVMUuidMap.put(actionIdString, entityUuid);
                    }
                    break;
                case MOVE:
                    // If we are moving across regions, as in cloud migration for example, then
                    // we need to show details. Otherwise, skip.
                    final ActionDTO.Move move = info.getMove();
                    if (!TopologyDTOUtil.isMigrationAction(recommendation)) {
                        break;
                    } else {
                        if (entityType == EntityType.VIRTUAL_VOLUME_VALUE) {
                            // Scaling cloud volume action
                            scaleCloudVolumeActionToVolumeUuidMap.put(actionIdString, entityUuid);
                        } else {
                            // Scaling cloud VM/DB/DBS action
                            resizeCloudVMActionToVMUuidMap.put(actionIdString, entityUuid);
                        }
                        if (scaleCloudVolumeActionToVolumeUuidMap.size() > 0 || resizeCloudVMActionToVMUuidMap.size() > 0) {
                            Map<Long, CloudResizeActionDetailsApiDTO> cloudResizeActionDetailMap =
                                    createCloudResizeActionDetailsDTO(resizeCloudVMActionToVMUuidMap.values(),
                                            scaleCloudVolumeActionToVolumeUuidMap.values(), topologyContextId);
                            resizeCloudVMActionToVMUuidMap.forEach((actionId, entityId) -> {
                                CloudResizeActionDetailsApiDTO cloudResizeActionDetail = cloudResizeActionDetailMap.get(entityId);
                                if (move.hasCloudSavingsDetails()) {
                                    cloudResizeActionDetail.setEntityUptime(cloudSavingsDetailsDtoConverter.convert(move.getCloudSavingsDetails()).getEntityUptime());
                                }
                                response.put(actionId, cloudResizeActionDetail);
                            });
                            scaleCloudVolumeActionToVolumeUuidMap.forEach((actionId, volumeId) -> {
                                response.put(actionId, cloudResizeActionDetailMap.get(volumeId));
                            });
                        }
                    }
                    break;
                case RESIZE:
                case SCALE:
                    final Scale scale = info.getScale();
                    if (scale.hasCloudSavingsDetails()) {
                        response.put(actionIdString, cloudSavingsDetailsDtoConverter.convert(
                                scale.getCloudSavingsDetails()));
                    } else {
                        logger.error("Missing cloud saving details for {}", scale.toString());
                    }
                    break;
                case ALLOCATE:
                    final Allocate allocate = info.getAllocate();
                    if (allocate.hasCloudSavingsDetails()) {
                        response.put(actionIdString, cloudSavingsDetailsDtoConverter.convert(
                                allocate.getCloudSavingsDetails()));
                    } else {
                        logger.error("Missing cloud saving details for {}", allocate.toString());
                    }
                    break;
                default:
                    break;
            }
        }

        Map<Long, ActionDetailsApiDTO> cloudProvisionActionDetailMap = new HashMap<>();
        Map<Long, ActionDetailsApiDTO> cloudSuspendActionDetailMap = new HashMap<>();
        createCloudProvisionSuspendActionDetailsDTOs(
                provisionCloudVMActionToVMUuidMap.values(),
                suspendCloudVMActionToVMUuidMap.values(),
                topologyContextId,
                cloudProvisionActionDetailMap,
                cloudSuspendActionDetailMap);
        provisionCloudVMActionToVMUuidMap.forEach((actionId, vmId) ->
                response.put(actionId, cloudProvisionActionDetailMap.get(vmId)));
        suspendCloudVMActionToVMUuidMap.forEach((actionId, vmId) ->
                response.put(actionId, cloudSuspendActionDetailMap.get(vmId)));

        return response;
    }

    /**
     * Create OnPremResizeActionDetailsApiDTO from Resize information.
     *
     * @param resize Resize info
     * @return created OnPremResizeActionDetails API object
     */
    private static ActionDetailsApiDTO createOnPremResizeActionDetails(Resize resize) {
        int vcpuBefore = (int)resize.getOldCapacity();
        int vcpuAfter = (int)resize.getNewCapacity();
        int cpsrBefore = resize.getOldCpsr();
        int cpsrAfter = resize.getNewCpsr();
        int socketsBefore = vcpuBefore / cpsrBefore;
        int socketsAfter = vcpuAfter / cpsrAfter;
        return createVcpuChangeActionDetails(new OnPremResizeActionDetailsApiDTO(), vcpuBefore,
                        vcpuAfter, cpsrBefore, cpsrAfter, socketsBefore, socketsAfter);
    }

    private static ActionDetailsApiDTO createOnPremReconfigureActionDetails(Reconfigure reconfigure) {
        Map<EntityAttribute, SettingChange> attr2change = reconfigure.getSettingChangeList()
                        .stream()
                        .collect(Collectors.toMap(SettingChange::getEntityAttribute, sc -> sc, (a1, a2) -> a1));
        SettingChange cpsr = attr2change.get(EntityAttribute.CORES_PER_SOCKET);
        SettingChange sockets = attr2change.get(EntityAttribute.SOCKET);
        if (cpsr == null || sockets == null) {
            logger.warn("Reconfigure for compliance action for {} has vCPU data missing",
                            reconfigure.getTarget().getId());
        }
        int cpsrBefore = cpsr == null ? 1 : (int)cpsr.getCurrentValue();
        int cpsrAfter = cpsr == null ? 1 : (int)cpsr.getNewValue();
        int socketsBefore = sockets == null ? 1 : (int)sockets.getCurrentValue();
        int socketsAfter = sockets == null ? 1 : (int)sockets.getNewValue();
        // TODO API dto has parameters annotated as 'optional' but they are not nullable, dto is not designed for missing data
        return createVcpuChangeActionDetails(new ReconfigureActionDetailsApiDTO(),
                        cpsrBefore * socketsBefore, cpsrAfter * socketsAfter,
                        cpsrBefore, cpsrAfter,
                        socketsBefore, socketsAfter);
    }

    private static <T extends CpuChangeDetailsApiDTO> T
                    createVcpuChangeActionDetails(T dto, int vcpuBefore, int vcpuAfter,
                                    int cpsrBefore, int cpsrAfter, int socketsBefore,
                                    int socketsAfter) {
        dto.setVcpuBefore(vcpuBefore);
        dto.setVcpuAfter(vcpuAfter);
        dto.setCoresPerSocketBefore(cpsrBefore);
        dto.setCoresPerSocketAfter(cpsrAfter);
        dto.setSocketsBefore(socketsBefore);
        dto.setSocketsAfter(socketsAfter);
        return dto;
    }

    /**
     * Create Cloud Resize Action Details DTOs for RI Buy use.
     * @param entityUuids - list of uuid of move action target entities.
     * @param topologyContextId - the topology context that the action corresponds to.
     * @return dtoMap - A map that contains additional details about the actions.
     */
    @Nonnull
    public Map<Long, CloudResizeActionDetailsApiDTO> createMoveActionRIBuyDetailsDTO(Long entityUuids, Long topologyContextId) {
        Map<Long, CloudResizeActionDetailsApiDTO> dtoMap = new HashMap<>();
        dtoMap.put(entityUuids, new CloudResizeActionDetailsApiDTO());
        // get RI coverage before/after
        setRiCoverage(topologyContextId, dtoMap);

        return dtoMap;
    }

    /**
     * Create Cloud Resize Action Details DTOs for a list of entity ids.
     * @param entityUuids - list of uuid of the action target entity
     * @param cloudVolumesToBeScaledUuids - list of uuid of action target entities which are cloud volumes.
     * @param topologyContextId - the topology context that the action corresponds to
     * @return dtoMap - A map that contains additional details about the actions
     * like on-demand rates, costs and RI coverage before/after the resize, indexed by entity id
     */
    @Nonnull
    public Map<Long, CloudResizeActionDetailsApiDTO> createCloudResizeActionDetailsDTO(Collection<Long> entityUuids,
                                                                                       Collection<Long> cloudVolumesToBeScaledUuids,
                                                                                       Long topologyContextId) {
        Set<Long> entityUuidSet = new HashSet<>(entityUuids);
        Map<Long, CloudResizeActionDetailsApiDTO> dtoMap = entityUuidSet.stream().collect(Collectors.toMap(e -> e, e -> new CloudResizeActionDetailsApiDTO()));

        // get on-demand costs
        setOnDemandCosts(topologyContextId, dtoMap);

        // get on-demand rates
        setOnDemandRates(topologyContextId, dtoMap);

        // get RI coverage before/after
        setRiCoverage(topologyContextId, dtoMap);

        if (!cloudVolumesToBeScaledUuids.isEmpty()) {
            Map<Long, CloudResizeActionDetailsApiDTO> volumeDTOMap
                    = cloudVolumesToBeScaledUuids.stream().collect(Collectors.toMap(e -> e, e -> new CloudResizeActionDetailsApiDTO()));
            setVolumeCosts(topologyContextId, volumeDTOMap);
            dtoMap.putAll(volumeDTOMap);
        }

        return dtoMap;
    }

    /**
     * Set on-demand costs for target entity which factors in RI usage.
     *
     * @param topologyContextId - context Id
     * @param dtoMap - cloud resize action details DTO, key is action target entity id
     */
    private void setOnDemandCosts(Long topologyContextId, Map<Long, CloudResizeActionDetailsApiDTO> dtoMap) {
        final Map<Long, List<StatRecord>> recordsByTime =
                getOnDemandCosts(dtoMap.keySet(), topologyContextId, true);
        // We expect to receive only current and future times, unless it is an on-prem to cloud
        // migration, in which case there are only projected costs.
        Set<Long> timeSet = recordsByTime.keySet();
        if (timeSet.size() >= 1) {
            boolean hasCurrentCosts = timeSet.size() >= 2;
            Long currentTime = hasCurrentCosts ? Collections.min(timeSet) : null; // current
            Long projectedTime = Collections.max(timeSet); // projected
            List<StatRecord> currentRecords = hasCurrentCosts ? recordsByTime.get(currentTime) : null;
            List<StatRecord> projectedRecords = recordsByTime.get(projectedTime);

            dtoMap.forEach((id, dto) -> {
                Double onDemandCostBefore = 0d;
                if (hasCurrentCosts) {
                    // get real-time
                    onDemandCostBefore = currentRecords.stream()
                            .filter(rec -> rec.getAssociatedEntityId() == id)
                            .map(StatRecord::getValues)
                            .mapToDouble(StatValue::getTotal)
                            .sum();
                }
                // get projected
                Double onDemandCostAfter = projectedRecords
                        .stream()
                        .filter(rec -> rec.getAssociatedEntityId() == id)
                        .map(StatRecord::getValues)
                        .mapToDouble(StatValue::getTotal)
                        .sum();
                dto.setOnDemandCostBefore(onDemandCostBefore.floatValue());
                dto.setOnDemandCostAfter(onDemandCostAfter.floatValue());
            });
        } else {
            logger.debug("Unable to provide on-demand costs before or after action for entities {}",
                    dtoMap);
        }
    }

    /**
     * Set before/after on-demand cost and on-demand rate for volumes within cloud scaling volume actions.
     *
     * @param topologyContextId topology context Id
     * @param dtoMap cloud resize action details DTO map, key is volume id
     */
    private void setVolumeCosts(Long topologyContextId, Map<Long, CloudResizeActionDetailsApiDTO> dtoMap) {
        EntityFilter entityFilter = EntityFilter.newBuilder().addAllEntityId(dtoMap.keySet()).build();
        CloudCostStatsQuery.Builder cloudCostStatsQueryBuilder = CloudCostStatsQuery.newBuilder()
                .setRequestProjected(true)
                .setEntityFilter(entityFilter)
                .setCostCategoryFilter(CostCategoryFilter.newBuilder()
                        .setExclusionFilter(false)
                        .addCostCategory(CostCategory.STORAGE)
                        .build());
        if (Objects.nonNull(topologyContextId)) {
            cloudCostStatsQueryBuilder.setTopologyContextId(topologyContextId);
        }
        GetCloudCostStatsRequest cloudCostStatsRequest = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(cloudCostStatsQueryBuilder.build())
                .build();
        final Iterator<GetCloudCostStatsResponse> response =
                costServiceBlockingStub.getCloudCostStats(cloudCostStatsRequest);
        Map<Long, List<StatRecord>> recordsByTime = new HashMap<>();
        while (response.hasNext()) {
            for (CloudCostStatRecord rec: response.next().getCloudStatRecordList()) {
                recordsByTime.computeIfAbsent(rec.getSnapshotDate(), x -> new ArrayList<>()).addAll(rec.getStatRecordsList());
            }
        }

        // We expect to receive only current and future times
        Set<Long> timeSet = recordsByTime.keySet();
        if (timeSet.size() == 2) {
            Long currentTime = Collections.min(timeSet); // current
            Long projectedTime = Collections.max(timeSet); // projected
            List<StatRecord> currentRecords = recordsByTime.get(currentTime);
            List<StatRecord> projectedRecords = recordsByTime.get(projectedTime);

            dtoMap.forEach((id, dto) -> {
                // get real-time
                Double onDemandCostBefore = currentRecords
                        .stream()
                        .filter(rec -> rec.getAssociatedEntityId() == id)
                        .map(StatRecord::getValues)
                        .mapToDouble(StatValue::getTotal)
                        .sum();
                // get projected
                Double onDemandCostAfter = projectedRecords
                        .stream()
                        .filter(rec -> rec.getAssociatedEntityId() == id)
                        .map(StatRecord::getValues)
                        .mapToDouble(StatValue::getTotal)
                        .sum();
                dto.setOnDemandCostBefore(onDemandCostBefore.floatValue());
                dto.setOnDemandRateBefore(onDemandCostBefore.floatValue());
                dto.setOnDemandCostAfter(onDemandCostAfter.floatValue());
                dto.setOnDemandRateAfter(onDemandCostAfter.floatValue());
            });
        } else {
            logger.debug("Unable to provide on-demand costs before and after action for volumes {}",
                    dtoMap);
        }
    }

    /**
     * Set on-demand template rates for a list of target entities.
     *
     * @param topologyContextId - topology context ID
     * @param dtoMap - map of cloud resize action details DTO, key is action target entity id
     */
    private void setOnDemandRates(@Nullable Long topologyContextId,
                                  Map<Long, CloudResizeActionDetailsApiDTO> dtoMap) {
        Set<Long> entityUuids = dtoMap.keySet();

        // Get the On Demand compute costs
        GetTierPriceForEntitiesResponse onDemandComputeCostsResponse =
                getOnDemandRates(entityUuids, CostCategory.ON_DEMAND_COMPUTE, topologyContextId);
        Map<Long, CurrencyAmount> beforeOnDemandComputeCostByEntityOidMap = onDemandComputeCostsResponse
                .getBeforeTierPriceByEntityOidMap();
        Map<Long, CurrencyAmount> afterComputeCostByEntityOidMap = onDemandComputeCostsResponse
                .getAfterTierPriceByEntityOidMap();

        // Get the On Demand License costs
        GetTierPriceForEntitiesResponse onDemandLicenseCostsResponse =
                getOnDemandRates(entityUuids, CostCategory.ON_DEMAND_LICENSE, topologyContextId);
        Map<Long, CurrencyAmount> beforeLicenseComputeCosts = onDemandLicenseCostsResponse
                .getBeforeTierPriceByEntityOidMap();
        Map<Long, CurrencyAmount> afterLicenseComputeCosts = onDemandLicenseCostsResponse
                .getAfterTierPriceByEntityOidMap();

        dtoMap.forEach((entityUuid, cloudResizeActionDetailsApiDTO) -> {
            double totalCurrentOnDemandRate = 0;
            if (beforeOnDemandComputeCostByEntityOidMap != null && beforeOnDemandComputeCostByEntityOidMap.get(entityUuid) != null) {
                double amount = beforeOnDemandComputeCostByEntityOidMap.get(entityUuid).getAmount();
                totalCurrentOnDemandRate += amount;
            }
            if (beforeLicenseComputeCosts != null && beforeLicenseComputeCosts.get(entityUuid) != null) {
                double amount = beforeLicenseComputeCosts.get(entityUuid).getAmount();
                totalCurrentOnDemandRate += amount;
            }
            if (totalCurrentOnDemandRate == 0) {
                logger.error("Current On Demand rate for entity with oid {}, not found", entityUuid);
            }
            cloudResizeActionDetailsApiDTO.setOnDemandRateBefore((float)totalCurrentOnDemandRate);

            double totalProjectedOnDemandRate = 0;
            if (afterComputeCostByEntityOidMap != null && afterComputeCostByEntityOidMap.get(entityUuid) != null) {
                double amount = afterComputeCostByEntityOidMap.get(entityUuid).getAmount();
                totalProjectedOnDemandRate += amount;
            }

            if (afterLicenseComputeCosts != null && afterLicenseComputeCosts.get(entityUuid) != null) {
                double amount = afterLicenseComputeCosts.get(entityUuid).getAmount();
                totalProjectedOnDemandRate += amount;
            }

            if (totalProjectedOnDemandRate == 0) {
                logger.error("Projected On Demand rate for entity with oid {}, not found", entityUuid);
            }
            cloudResizeActionDetailsApiDTO.setOnDemandRateAfter((float)totalProjectedOnDemandRate);
        });
    }

    /**
     * Set RI Coverage before/after for a list of target entities.
     * @param topologyContextId - the topology context for which RI coverage is being set
     * @param dtoMap - map of cloud resize action details DTO, key is action target entity id
     */
    private void setRiCoverage(Long topologyContextId, Map<Long, CloudResizeActionDetailsApiDTO> dtoMap) {
        final EntityFilter entityFilter = EntityFilter.newBuilder().addAllEntityId(dtoMap.keySet()).build();

        // get latest RI coverage for target entity
        Cost.GetEntityReservedInstanceCoverageRequest reservedInstanceCoverageRequest =
                Cost.GetEntityReservedInstanceCoverageRequest
                        .newBuilder()
                        .setEntityFilter(entityFilter)
                        .build();
        Cost.GetEntityReservedInstanceCoverageResponse reservedInstanceCoverageResponse =
                reservedInstanceUtilizationCoverageServiceBlockingStub
                        .getEntityReservedInstanceCoverage(reservedInstanceCoverageRequest);

        Map<Long, Cost.EntityReservedInstanceCoverage> coverageMap = reservedInstanceCoverageResponse.getCoverageByEntityIdMap();
        dtoMap.forEach((entityUuid, cloudResizeActionDetailsApiDTO) -> {
            if (coverageMap.containsKey(entityUuid)) {
                Cost.EntityReservedInstanceCoverage latestCoverage = coverageMap.get(entityUuid);
                StatValueApiDTO latestCoverageCapacityDTO = new StatValueApiDTO();
                latestCoverageCapacityDTO.setAvg((float)latestCoverage.getEntityCouponCapacity());

                StatApiDTO latestCoverageStatDTO = new StatApiDTO();
                // set coupon capacity
                latestCoverageStatDTO.setCapacity(latestCoverageCapacityDTO);
                // set coupon usage
                latestCoverageStatDTO.setValue((float)latestCoverage.getCouponsCoveredByRiMap().values()
                        .stream().mapToDouble(Double::doubleValue).sum());
                cloudResizeActionDetailsApiDTO.setRiCoverageBefore(latestCoverageStatDTO);
            } else {
                logger.debug("Failed to retrieve current RI coverage for entity with ID: {}", entityUuid);
            }
        });

        Map<Long, Cost.EntityReservedInstanceCoverage> projectedCoverageMap = getEntityRiCoverageMap(topologyContextId, entityFilter);

        dtoMap.forEach((entityUuid, cloudResizeActionDetailsApiDTO) -> {
            if (projectedCoverageMap.containsKey(entityUuid)) {
                // set projected RI coverage
                Cost.EntityReservedInstanceCoverage projectedRiCoverage = projectedCoverageMap.get(entityUuid);
                StatValueApiDTO projectedCoverageCapacityDTO = new StatValueApiDTO();
                projectedCoverageCapacityDTO.setAvg((float)projectedRiCoverage.getEntityCouponCapacity());

                StatApiDTO projectedCoverageStatDTO = new StatApiDTO();
                // set coupon capacity
                projectedCoverageStatDTO.setCapacity(projectedCoverageCapacityDTO);
                // set coupon usage
                projectedCoverageStatDTO.setValue((float)projectedRiCoverage.getCouponsCoveredByRiMap()
                        .values().stream().mapToDouble(Double::doubleValue).sum());
                cloudResizeActionDetailsApiDTO.setRiCoverageAfter(projectedCoverageStatDTO);
            } else {
                logger.debug("Failed to retrieve projected RI coverage for entity with ID: {}", entityUuid);
            }
        });
    }


    /**
     * Get the UUID of the primary entity involved in a given action.
     *
     * @param actionSpec the {@link ActionSpec} corresponding to a given action
     * @return the UUID of the primary entity involved in a given action
     */
    private static Long getCloudEntityUuidFromActionSpec(final ActionSpec actionSpec) {
        long entityUuid;
        ActionEntity entity;
        try {
            entity = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation());
            entityUuid = entity.getId();
        } catch (UnsupportedActionException e) {
            logger.warn("Cannot create action details due to unsupported action type", e);
            return null;
        }
        if (entity.getEnvironmentType() != EnvironmentTypeEnum.EnvironmentType.CLOUD) {
            return null;
        }
        return entityUuid;
    }

    /**
     * Create Cloud Provision or Suspend Action Details DTOs for a list of entity ids.
     *
     * @param provisionUuids list of cloud VMs to be provisioned
     * @param suspendUuids list of cloud VMs to be suspended
     * @param topologyContextId the topology context of the actions
     * @param provisionActionDetails the map of provision action details
     * @param suspendActionDetails the map suspend action details
     */
    public void createCloudProvisionSuspendActionDetailsDTOs(
            @Nonnull final Collection<Long> provisionUuids,
            @Nonnull final Collection<Long> suspendUuids,
            @Nullable final Long topologyContextId,
            @Nonnull Map<Long, ActionDetailsApiDTO> provisionActionDetails,
            @Nonnull Map<Long, ActionDetailsApiDTO> suspendActionDetails) {
        Collection<Long> entityUuids = new HashSet<>(provisionUuids);
        entityUuids.addAll(suspendUuids);
        // Get current on-demand costs. For provision and suspend action, we only need to obtain the
        // cost from the real entity store. There is no need to obtain cost from projected entity
        // store because for suspend action, the entity is deactivated so there is no associated
        // cost, and for provision action, the cost will be the same.
        final Map<Long, Double> onDemandCostsByEntity =
                getOnDemandCosts(entityUuids, topologyContextId, false)
                        .values()
                        .stream()
                        // Get the records from the first time snapshot
                        .findFirst()
                        // Group the records by entityId
                        .map(currentRecords -> currentRecords.stream()
                                .collect(Collectors.groupingBy(StatRecord::getAssociatedEntityId,
                                        Collectors.summingDouble(record -> record.getValues().getTotal()))))
                        .orElse(new HashMap<>());
        // Get current on-demand compute rates
        final Map<Long, CurrencyAmount> onDemandComputeRatesByEntity =
                getOnDemandRates(entityUuids, CostCategory.ON_DEMAND_COMPUTE, topologyContextId)
                        .getBeforeTierPriceByEntityOidMap();
        // Get current on-demand license rates
        final Map<Long, CurrencyAmount> onDemandLicenseRatesByEntity =
                getOnDemandRates(entityUuids, CostCategory.ON_DEMAND_LICENSE, topologyContextId)
                        .getBeforeTierPriceByEntityOidMap();
        // Sum the on-demand license and compute rates
        final Map<Long, Double> onDemandRatesByEntity =
                Stream.of(onDemandComputeRatesByEntity, onDemandLicenseRatesByEntity)
                        .flatMap(map -> map.entrySet().stream())
                        .collect(Collectors.groupingBy(Map.Entry::getKey,
                                Collectors.summingDouble(amount -> amount.getValue().getAmount())));
        // Fill action details for provisioned entities
        provisionUuids.forEach(id -> provisionActionDetails
                .putIfAbsent(id, createCloudProvisionSuspendActionDetailsDTO(
                        onDemandCostsByEntity.getOrDefault(id, 0d),
                        onDemandRatesByEntity.getOrDefault(id, 0d),
                        true)));
        // Fill action details for suspended entities
        suspendUuids.forEach(id -> suspendActionDetails
                .putIfAbsent(id, createCloudProvisionSuspendActionDetailsDTO(
                        onDemandCostsByEntity.getOrDefault(id, 0d),
                        onDemandRatesByEntity.getOrDefault(id, 0d),
                        false)));
    }

    /**
     * Create Cloud Provision or Suspend Action Details DTO for an entity.
     *
     * @param onDemandCost the on-demand cost of the entity
     * @param onDemandRate the on-demant rate of the entity
     * @param isProvision if the action is provision
     * @return the action details DTO
     */
    @Nonnull
    private ActionDetailsApiDTO createCloudProvisionSuspendActionDetailsDTO(
            @Nonnull final Double onDemandCost,
            @Nonnull final Double onDemandRate,
            final boolean isProvision) {
        if (isProvision) {
            CloudProvisionActionDetailsApiDTO provisionActionDetailsApiDTO =
                    new CloudProvisionActionDetailsApiDTO();
            provisionActionDetailsApiDTO.setOnDemandCost(onDemandCost.floatValue());
            provisionActionDetailsApiDTO.setOnDemandRate(onDemandRate.floatValue());
            return provisionActionDetailsApiDTO;
        }
        CloudSuspendActionDetailsApiDTO suspendActionDetailsApiDTO =
                new CloudSuspendActionDetailsApiDTO();
        suspendActionDetailsApiDTO.setOnDemandCost(onDemandCost.floatValue());
        suspendActionDetailsApiDTO.setOnDemandRate(onDemandRate.floatValue());
        return suspendActionDetailsApiDTO;
    }

    /**
     * Get the on-demand compute and license cost for a collection of entities.
     *
     * @param entityUuids the given entities
     * @param topologyContextId the context Id
     * @param requestProjected whether or not to request projected bottom-up costs.
     * @return the on demand costs of the given entities
     */
    @Nonnull
    private Map<Long, List<StatRecord>> getOnDemandCosts(@Nonnull final Collection<Long> entityUuids,
                                                         @Nullable final Long topologyContextId,
                                                         boolean requestProjected) {
        EntityFilter entityFilter = EntityFilter.newBuilder()
                .addAllEntityId(entityUuids)
                .build();
        CloudCostStatsQuery.Builder cloudCostStatsQueryBuilder = CloudCostStatsQuery.newBuilder()
                .setRequestProjected(requestProjected)
                .setEntityFilter(entityFilter)
                // For cloud scale actions, the action savings will reflect only the savings from
                // accepting the specific action, ignoring any potential discount from Buy RI actions.
                // Therefore, we want the projected on-demand cost in the actions details to only
                // reflect the cost from accepting this action. We filter out BUY_RI_DISCOUNT here
                // to be consistent with the action savings calculation and to avoid double counting
                // potential savings from Buy RI actions
                .setCostSourceFilter(CostSourceFilter.newBuilder()
                        .setExclusionFilter(true)
                        .addAllCostSources(SOURCES_TO_EXCLUDE_FOR_ON_DEMAND_COST))
                .setCostCategoryFilter(CostCategoryFilter.newBuilder()
                        .setExclusionFilter(false)
                        .addAllCostCategory(CATEGORIES_TO_INCLUDE_FOR_ON_DEMAND_COST)
                        .build());
        if (Objects.nonNull(topologyContextId)) {
            cloudCostStatsQueryBuilder.setTopologyContextId(topologyContextId);
        }
        GetCloudCostStatsRequest cloudCostStatsRequest = GetCloudCostStatsRequest.newBuilder()
                .addCloudCostStatsQuery(cloudCostStatsQueryBuilder.build())
                .build();
        final Iterator<GetCloudCostStatsResponse> response =
                costServiceBlockingStub.getCloudCostStats(cloudCostStatsRequest);
        Map<Long, List<StatRecord>> recordsByTime = new HashMap<>();
        while (response.hasNext()) {
            for (CloudCostStatRecord rec: response.next().getCloudStatRecordList()) {
                recordsByTime.computeIfAbsent(rec.getSnapshotDate(), x -> new ArrayList<>()).addAll(rec.getStatRecordsList());
            }
        }
        return recordsByTime;
    }

    /**
     * Get the on-demand rate for a list of entities.
     *
     * @param entityUuids the list of entity uuids
     * @param costCategory the cost category
     * @param topologyContextId the topology context ID
     * @return the on-demand rate for a list of entities
     */
    @Nonnull
    private GetTierPriceForEntitiesResponse getOnDemandRates(@Nonnull final Collection<Long> entityUuids,
                                                             @Nonnull final CostCategory costCategory,
                                                             @Nullable Long topologyContextId) {
        // Get the On Demand compute costs
        GetTierPriceForEntitiesRequest.Builder onDemandRatesRequest = GetTierPriceForEntitiesRequest.newBuilder()
                .addAllOids(entityUuids)
                .setCostCategory(costCategory);
        Optional.ofNullable(topologyContextId).ifPresent(onDemandRatesRequest::setTopologyContextId);
        return costServiceBlockingStub.getTierPriceForEntities(onDemandRatesRequest.build());
    }

    /**
     * Given a {@param topologyContextId} and an {@link EntityFilter}, retrieve the corresponding
     * map of RI coverage by entity ID.
     *
     * @param topologyContextId the context ID of a given topology
     * @param entityFilter the {@link EntityFilter} representing a given group of entities
     * @return a map of RI coverage by entity ID
     */
    private Map<Long, Cost.EntityReservedInstanceCoverage> fetchEntityRiCoverageMap(
            Long topologyContextId,
            EntityFilter entityFilter) {
        // get projected RI coverage for target entity
        Cost.GetProjectedEntityReservedInstanceCoverageRequest.Builder builder =
                Cost.GetProjectedEntityReservedInstanceCoverageRequest
                        .newBuilder()
                        .setEntityFilter(entityFilter);
        if (!Objects.isNull(topologyContextId)) {
            builder.setTopologyContextId(topologyContextId);
        }

        Cost.GetProjectedEntityReservedInstanceCoverageRequest projectedEntityReservedInstanceCoverageRequest = builder.build();
        Cost.GetProjectedEntityReservedInstanceCoverageResponse projectedEntityReservedInstanceCoverageResponse =
                reservedInstanceUtilizationCoverageServiceBlockingStub
                        .getProjectedEntityReservedInstanceCoverageStats(projectedEntityReservedInstanceCoverageRequest);

        return projectedEntityReservedInstanceCoverageResponse
                .getCoverageByEntityIdMap();
    }

    /**
     * Retrieve the RI coverage map corresponding to a given {@param topologyContextId} and
     * {@param entityFilter} from cache if present, otherwise fetch it via gRPC request.
     *
     * @param topologyContextId the context ID of a given topology
     * @param entityFilter the {@link EntityFilter} representing a set of entities
     * @return the corresponding RI coverage map
     */
    private Map<Long, Cost.EntityReservedInstanceCoverage> getEntityRiCoverageMap(
            Long topologyContextId,
            EntityFilter entityFilter) {
        if (topologyContextId == null) {
            logger.error("Topology Context ID is null for entityFilter " + entityFilter);
            return Collections.emptyMap();
        }

        //Check if the topology is real time.
        boolean isRealTimeTopology = topologyContextId == realtimeTopologyContextId ? true : false;

        // If real time, fetch projected RI coverage from the cost component.
        if (isRealTimeTopology) {
            return fetchEntityRiCoverageMap(topologyContextId, entityFilter);
        }

        // If not real time, rely on the cache.
        boolean containsTopologyContextEntry = topologyContextIdToEntityFilterToEntityRiCoverage.containsKey(topologyContextId);
        if (containsTopologyContextEntry
                && topologyContextIdToEntityFilterToEntityRiCoverage.get(topologyContextId).containsKey(entityFilter)) {
            return topologyContextIdToEntityFilterToEntityRiCoverage.get(topologyContextId).get(entityFilter);
        } else {
            Map<Long, Cost.EntityReservedInstanceCoverage> coverageMap = fetchEntityRiCoverageMap(topologyContextId, entityFilter);
            // Update the cache...
            if (!containsTopologyContextEntry) {
                // Bust the cache when it gets large
                if (topologyContextIdToEntityFilterToEntityRiCoverage.size() >= 20) {
                    topologyContextIdToEntityFilterToEntityRiCoverage.clear();
                }
                final Map<EntityFilter, Map<Long, Cost.EntityReservedInstanceCoverage>> entityFilterToRiCoverage = Maps.newHashMap();
                entityFilterToRiCoverage.put(entityFilter, coverageMap);
                topologyContextIdToEntityFilterToEntityRiCoverage.put(topologyContextId, entityFilterToRiCoverage);
            } else {
                topologyContextIdToEntityFilterToEntityRiCoverage.get(topologyContextId).put(entityFilter, coverageMap);
            }
            return coverageMap;
        }
    }

    /**
     * Create RI historical Context Stat Snapshot DTOs.
     *
     * @param snapshots - template demand snapshots
     * @return list of StatSnapshotApiDTO each translated from the provided Stats.StatSnapshot snapshots.
     */
    @Nonnull
    private List<StatSnapshotApiDTO> createRiHistoricalContextStatSnapshotDTO(final List<Stats.StatSnapshot> snapshots) {

        final List<StatSnapshotApiDTO> statSnapshotApiDTOList = new ArrayList<>();

        for (Stats.StatSnapshot snapshot : snapshots) {
            // Create 168 snapshots in hourly intervals
            for (Stats.StatSnapshot.StatRecord record : snapshot.getStatRecordsList()) {

                final List<StatApiDTO> statApiDTOList = new ArrayList<>();

                final StatApiDTO statApiDTO = new StatApiDTO();
                statApiDTO.setValue((record.getValues().getAvg()));
                statApiDTO.setUnits(record.getUnits());
                statApiDTOList.add(statApiDTO);

                final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                statSnapshotApiDTO.setStatistics(statApiDTOList);
                statSnapshotApiDTO.setDisplayName(record.getStatKey());
                statSnapshotApiDTO.setDate(Long.toString(snapshot.getSnapshotDate()));

                statSnapshotApiDTOList.add(statSnapshotApiDTO);
            }
        }
        return statSnapshotApiDTOList;
    }

    /**
     * Create an ActionExecutionAuditApiDTO for when the Action State is in EXECUTION_ACTION_STATES,
     * otherwise return null.
     *
     * @param actionSpec                    Action specifications
     * @return ActionExecutionAuditApiDTO   contains details about the execution of the Action,
     *                                      null if the Action is not executed yet.
     */
    @Nullable
    private ActionExecutionAuditApiDTO createActionExecutionAuditApiDTO(@Nonnull final ActionSpec actionSpec) {
        ActionState executionState = mapXlActionStateToExecutionApi(actionSpec.getActionState());
        // If the Action was not executed, return null
        if (executionState == null || actionSpec.getExecutionStep() == null) {
            return null;
        }

        final ActionDTO.ExecutionStep executionStep = actionSpec.getExecutionStep();
        ActionExecutionAuditApiDTO executionDTO = new ActionExecutionAuditApiDTO();
        executionDTO.setState(executionState);
        if (IN_PROGRESS_PREDICATE.test(executionDTO.getState())) {
            executionDTO.setProgress(executionStep.getProgressPercentage());
        }
        if (CollectionUtils.isNotEmpty(executionStep.getErrorsList())) {
            // Show the last most updated message
            executionDTO.setMessage(Iterables.getLast(executionStep.getErrorsList()));
        }
        if (executionStep.hasStartTime()) {
            final String startTime = DateTimeUtil.toString(executionStep.getStartTime());
            executionDTO.setExecutionTime(startTime);
        }
        if (executionStep.hasCompletionTime()) {
            final String completionTime = DateTimeUtil.toString(executionStep.getCompletionTime());
            executionDTO.setCompletionTime(completionTime);
        }

        return executionDTO;
    }

    /**
     * Get entity ({@link ServiceEntityApiDTO} involved in action. If certain entity is absent in
     * context (e.g. entity was removed from topology because of deleting target) we get all
     * possible information from {@link ActionEntity}.
     *
     * @param context contains different information related to action and helps for
     * mapping {@link ActionSpec} to {@link ActionApiDTO}.
     * @param actionEntity entity involved in action
     * @return {@link ServiceEntityApiDTO} entity involved in action
     */
    @Nonnull
    private ServiceEntityApiDTO getServiceEntityDTO(@Nonnull ActionSpecMappingContext context,
            @Nonnull ActionEntity actionEntity) {
        final Optional<ServiceEntityApiDTO> targetEntity = context.getEntity(actionEntity.getId());
        final ServiceEntityApiDTO serviceEntityApiDTO;
        if (targetEntity.isPresent()) {
            serviceEntityApiDTO = ServiceEntityMapper.copyServiceEntityAPIDTO(targetEntity.get());
        } else {
            serviceEntityApiDTO = getMinimalServiceEntityApiDTO(actionEntity);
        }

        // set the cluster if the entity has a cluster
        context.getCluster(actionEntity.getId())
            .map(Collections::singletonList)
            .ifPresent(serviceEntityApiDTO::setConnectedEntities);

        return serviceEntityApiDTO;
    }

    /**
     * Get as much information about entity involved in action as possible from
     * {@link ActionEntity}.
     *
     * @param actionEntity entity involved in action
     * @return {@link ServiceEntityApiDTO} contains all possible information from {@link ActionEntity}
     */
    @Nonnull
    private ServiceEntityApiDTO getMinimalServiceEntityApiDTO(@Nonnull ActionEntity actionEntity) {
        final ServiceEntityApiDTO serviceEntity = new ServiceEntityApiDTO();
        serviceEntity.setUuid(String.valueOf(actionEntity.getId()));
        serviceEntity.setClassName(ApiEntityType.fromType(actionEntity.getType()).apiStr());
        if (actionEntity.hasEnvironmentType()) {
            serviceEntity.setEnvironmentType(EnvironmentTypeMapper.fromXLToApi(actionEntity.getEnvironmentType()));
        }
        return serviceEntity;
    }

    /**
     * Map UI's ActionMode to ActionDTO.ActionMode.
     *
     * @param actionMode UI's ActionMode
     * @return ActionDTO.ActionMode
     */
    @Nonnull
    public static Optional<ActionDTO.ActionMode> mapApiModeToXl(final ActionMode actionMode) {
        switch (actionMode) {
            case DISABLED:
                return Optional.of(ActionDTO.ActionMode.DISABLED);
            case RECOMMEND:
                return Optional.of(ActionDTO.ActionMode.RECOMMEND);
            case MANUAL:
                return Optional.of(ActionDTO.ActionMode.MANUAL);
            case AUTOMATIC:
                return Optional.of(ActionDTO.ActionMode.AUTOMATIC);
            case EXTERNAL_APPROVAL:
                return Optional.of(ActionDTO.ActionMode.EXTERNAL_APPROVAL);
            default:
                logger.error("Unknown action mode {}", actionMode);
                return Optional.empty();
        }
    }

    /**
     * Map UI's ActionCostType to ActionDTO.ActionCostType.
     *
     * @param actionCostType UI's ActionCostType
     * @return ActionDTO.ActionCostType
     */
    public static ActionDTO.ActionCostType mapApiCostTypeToXL(final ActionCostType actionCostType) {
        switch (actionCostType) {
            case SAVING:
                return ActionDTO.ActionCostType.SAVINGS;
            case INVESTMENT:
                return ActionDTO.ActionCostType.INVESTMENT;
            case ACTION_COST_TYPE_NONE:
                return ActionDTO.ActionCostType.ACTION_COST_TYPE_NONE;
            default:
                throw new IllegalArgumentException("Unknown action cost type" + actionCostType);
        }
    }

    /**
     * Map UI's ActionDisruptiveness to ActionDTO.ActionDisruptiveness.
     *
     * @param actionDisruptiveness UI's ActionDisruptiveness
     * @return ActionDTO.ActionDisruptiveness
     */
    public static ActionDTO.ActionDisruptiveness mapApiDisruptivenessToXL(final ActionDisruptiveness actionDisruptiveness) {
        switch (actionDisruptiveness) {
            case DISRUPTIVE:
                return ActionDTO.ActionDisruptiveness.DISRUPTIVE;
            case NON_DISRUPTIVE:
                return ActionDTO.ActionDisruptiveness.NON_DISRUPTIVE;
            default:
                throw new IllegalArgumentException("Unknown action disruptiveness" + actionDisruptiveness);
        }
    }

    /**
     * Map UI's ActionReversibility to ActionDTO.ActionReversibility.
     *
     * @param actionReversibility UI's ActionReversibility
     * @return ActionDTO.ActionReversibility
     */
    public static ActionDTO.ActionReversibility mapApiReversibilityToXL(final ActionReversibility actionReversibility) {
        switch (actionReversibility) {
            case REVERSIBLE:
                return ActionDTO.ActionReversibility.REVERSIBLE;
            case IRREVERSIBLE:
                return ActionDTO.ActionReversibility.IRREVERSIBLE;
            default:
                throw new IllegalArgumentException("Unknown action reversibility" + actionReversibility);
        }
    }

    /**
     * Get minimal entityApiDTO from action entity. Note that this returns {@link DiscoveredEntityApiDTO}
     * instead of {@link ServiceEntityApiDTO} to avoid redundant fields in entityApiDTO in an ActionApiDTO.
     *
     * @param actionEntity Given {@link ActionEntity} to be converted to {@link DiscoveredEntityApiDTO}.
     * @return {@link DiscoveredEntityApiDTO} converted from {@link ActionEntity}.
     */
    public static DiscoveredEntityApiDTO getDiscoveredEntityApiDTOFromActionEntity(ActionEntity actionEntity) {
        DiscoveredEntityApiDTO discoveredEntityApiDTO = new DiscoveredEntityApiDTO();
        discoveredEntityApiDTO.setUuid(String.valueOf(actionEntity.getId()));
        discoveredEntityApiDTO.setEnvironmentType(EnvironmentTypeMapper.fromXLToApi(actionEntity.getEnvironmentType()));
        discoveredEntityApiDTO.setClassName(ApiEntityType.fromType(actionEntity.getType()).apiStr());
        return discoveredEntityApiDTO;
    }
}
