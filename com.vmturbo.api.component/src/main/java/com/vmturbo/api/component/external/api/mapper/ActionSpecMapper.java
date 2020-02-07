package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.action.ActionDTO.ActionType.ALLOCATE;
import static com.vmturbo.common.protobuf.action.ActionDTO.ActionType.BUY_RI;
import static com.vmturbo.common.protobuf.action.ActionDTO.ActionType.RESIZE;
import static com.vmturbo.common.protobuf.action.ActionDTO.ActionType.SCALE;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.TRANSLATION_PATTERN;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.TRANSLATION_PREFIX;

import java.beans.PropertyDescriptor;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMappingContextFactory.ActionSpecMappingContext;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper.NotFoundCloudTypeException;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper.NotFoundMatchOfferingClassException;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper.NotFoundMatchPaymentOptionException;
import com.vmturbo.api.component.external.api.mapper.ReservedInstanceMapper.NotFoundMatchTenancyException;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.BuyRiScopeHandler;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.action.ActionDetailsApiDTO;
import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.action.RIBuyActionDetailsApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.entityaspect.VirtualDiskApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.reservedinstance.ReservedInstanceApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.enums.AspectName;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.Pair;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.StringUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter.InvolvedEntities;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReasonCommodity;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord.StatRecord;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery;
import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.CostSourceFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetTierPriceForEntitiesResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIBuyContextFetchServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.stats.Stats;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.commons.Units;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;

/**
 * Map an ActionSpec returned from the ActionOrchestrator into an {@link ActionApiDTO} to be
 * returned from the API.
 */
public class ActionSpecMapper {
    // Currencies by numeric code map will is a map of numeric code to the Currency.
    // TODO: But the numeric code is not unique. As of writing this, there are a few currencies
    // which share numeric code. They are:
    // Currency code = 946 -> Romanian Leu (RON), Romanian Leu (1952-2006) (ROL)
    // Currency code = 891 -> Serbian Dinar (2002-2006) (CSD), Yugoslavian New Dinar (1994-2002) (YUM)
    // Currency code = 0   -> French UIC-Franc (XFU), French Gold Franc (XFO)
    private static final Map<Integer, Currency> CURRENCIES_BY_NUMERIC_CODE =
            Collections.unmodifiableMap(
                    Currency.getAvailableCurrencies().stream()
                        .collect(Collectors.toMap(Currency::getNumericCode, Function.identity(),
                                (c1, c2) -> c1)));

    // START - Strings representing action categories in the API.
    // These should be synchronized with the strings in stringUtils.js
    private static final String API_CATEGORY_PERFORMANCE_ASSURANCE = "Performance Assurance";
    private static final String API_CATEGORY_EFFICIENCY_IMPROVEMENT = "Efficiency Improvement";
    private static final String API_CATEGORY_PREVENTION = "Prevention";
    private static final String API_CATEGORY_COMPLIANCE = "Compliance";
    private static final String API_CATEGORY_UNKNOWN = "Unknown";
    // END - Strings representing action categories in the API.

    private static final Set<String> SCALE_TIER_VALUES = ImmutableSet.of(
            UIEntityType.COMPUTE_TIER.apiStr(), UIEntityType.DATABASE_SERVER_TIER.apiStr(),
            UIEntityType.DATABASE_TIER.apiStr());

    private static final Set<String> CLOUD_ACTIONS_TIER_VALUES = new ImmutableSet.Builder<String>()
                    .addAll(SCALE_TIER_VALUES)
                    .add(UIEntityType.STORAGE_TIER.apiStr())
                    .build();

    /**
     * Map of entity types to shortened versions for action descriptions.
     */
    private static final Map<String, String> SHORTENED_ENTITY_TYPES = ImmutableMap.of(
        UIEntityType.VIRTUAL_VOLUME.apiStr(), "Volume"
    );

    private final ActionSpecMappingContextFactory actionSpecMappingContextFactory;

    private final ServiceEntityMapper serviceEntityMapper;

    private final long realtimeTopologyContextId;

    private static final Logger logger = LogManager.getLogger();

    private final ReservedInstanceMapper reservedInstanceMapper;

    private final RIBuyContextFetchServiceGrpc.RIBuyContextFetchServiceBlockingStub riStub;

    private final ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub
            reservedInstanceUtilizationCoverageServiceBlockingStub;

    private final CostServiceBlockingStub costServiceBlockingStub;

    private final BuyRiScopeHandler buyRiScopeHandler;

    /**
     * The set of action states for operational actions (ie actions that have not
     * completed execution).
     */
    public static final ActionDTO.ActionState[] OPERATIONAL_ACTION_STATES = {
        ActionDTO.ActionState.READY,
        ActionDTO.ActionState.QUEUED,
        ActionDTO.ActionState.IN_PROGRESS
    };

    public ActionSpecMapper(@Nonnull ActionSpecMappingContextFactory actionSpecMappingContextFactory,
                            @Nonnull final ServiceEntityMapper serviceEntityMapper,
                            @Nonnull final ReservedInstanceMapper reservedInstanceMapper,
                            @Nullable final RIBuyContextFetchServiceGrpc.RIBuyContextFetchServiceBlockingStub riStub,
                            @Nonnull final CostServiceBlockingStub costServiceBlockingStub,
                            @Nonnull final ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub reservedInstanceUtilizationCoverageServiceBlockingStub,
                            @Nonnull final BuyRiScopeHandler buyRiScopeHandler,
                            final long realtimeTopologyContextId) {
        this.actionSpecMappingContextFactory = Objects.requireNonNull(actionSpecMappingContextFactory);
        this.serviceEntityMapper = Objects.requireNonNull(serviceEntityMapper);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.reservedInstanceMapper = Objects.requireNonNull(reservedInstanceMapper);
        this.costServiceBlockingStub = Objects.requireNonNull(costServiceBlockingStub);
        this.riStub = riStub;
        this.reservedInstanceUtilizationCoverageServiceBlockingStub = reservedInstanceUtilizationCoverageServiceBlockingStub;
        this.buyRiScopeHandler = buyRiScopeHandler;
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
     */
    @Nonnull
    public List<ActionApiDTO> mapActionSpecsToActionApiDTOs(
            @Nonnull final Collection<ActionSpec> actionSpecs,
            final long topologyContextId)
                    throws UnsupportedActionException, ExecutionException, InterruptedException {
        if (actionSpecs.isEmpty()) {
            return Collections.emptyList();
        }
        final List<ActionDTO.Action> recommendations = actionSpecs.stream()
            .map(ActionSpec::getRecommendation)
            .collect(Collectors.toList());
        final ActionSpecMappingContext context =
            actionSpecMappingContextFactory.createActionSpecMappingContext(recommendations, topologyContextId);
        final ImmutableList.Builder<ActionApiDTO> actionApiDTOS = ImmutableList.builder();
        int unresolvedEntities = 0;
        for (ActionSpec spec : actionSpecs) {
            try {
                final ActionApiDTO actionApiDTO = mapActionSpecToActionApiDTOInternal(spec, context, topologyContextId);
                actionApiDTOS.add(actionApiDTO);
            } catch (UnknownObjectException e) {
                unresolvedEntities += 1;
                logger.debug("Couldn't resolve entity from spec {} {}", spec, e);
            }
        }
        if (unresolvedEntities > 0) {
            logger.error("Couldn't resolve {}", (unresolvedEntities > 1 ? "entities" : "entity"));
        }
        return actionApiDTOS.build();
    }

    /**
     * Map an ActionSpec returned from the ActionOrchestratorComponent into an {@link ActionApiDTO}
     * to be returned from the API.
     *
     * When required, a displayName value for a given Service Entity ID is gathered from the
     * Repository service.
     *
     * Some fields are returned as a constant:
     * Some fields are ignored:
     *
     * @param actionSpec The {@link ActionSpec} object to be mapped into an {@link ActionApiDTO}.
     * @param topologyContextId The topology context within which the {@link ActionSpec} was
     *                          produced. We need this to get the right information froGm related
     *                          entities.
     * @return an {@link ActionApiDTO} object populated from the given ActionSpec
     * @throws UnknownObjectException If any entities involved in the action are not found in
     * the repository.
     * @throws UnsupportedActionException If the action type of the {@link ActionSpec} is not
     * supported.
     */
    @Nonnull
    public ActionApiDTO mapActionSpecToActionApiDTO(@Nonnull final ActionSpec actionSpec,
                                                    final long topologyContextId)
                    throws UnknownObjectException, UnsupportedActionException, ExecutionException,
                    InterruptedException {
        final ActionSpecMappingContext context =
            actionSpecMappingContextFactory.createActionSpecMappingContext(
                Lists.newArrayList(actionSpec.getRecommendation()), topologyContextId);
        return mapActionSpecToActionApiDTOInternal(actionSpec, context, topologyContextId);
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

    @Nonnull
    public ActionState mapXlActionStateToApi(@Nonnull final ActionDTO.ActionState actionState) {
        switch (actionState) {
            case PRE_IN_PROGRESS:
            case POST_IN_PROGRESS:
                return ActionState.IN_PROGRESS;
            default:
                return ActionState.valueOf(actionState.name());
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
    public Optional<ActionDTO.ActionCategory> mapApiActionCategoryToXl(@Nonnull final String category) {
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
            final long topologyContextId)
                    throws UnsupportedActionException, UnknownObjectException, ExecutionException, InterruptedException {
        // Construct a response ActionApiDTO to return
        final ActionApiDTO actionApiDTO = new ActionApiDTO();
        // actionID and uuid are the same
        actionApiDTO.setUuid(Long.toString(actionSpec.getRecommendation().getId()));
        actionApiDTO.setActionID(actionSpec.getRecommendation().getId());
        // actionMode is direct translation
        final ActionDTO.ActionMode actionMode = actionSpec.getActionMode();
        actionApiDTO.setActionMode(ActionMode.valueOf(actionMode.name()));

        // For plan action, set the state to successes, so it will not be selectable
        // TODO (Gary, Jan 17 2019): handle case when realtimeTopologyContextId is changed (if needed)
        if (topologyContextId == realtimeTopologyContextId) {
            actionApiDTO.setActionState(mapXlActionStateToApi(actionSpec.getActionState()));
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
                .map(description -> translateExplanation(description, context))
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
        risk.setSeverity(ActionDTOUtil.getSeverityName(
            ActionDTOUtil.mapActionCategoryToSeverity(actionSpec.getCategory())));
        risk.setReasonCommodity("");
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
                addReconfigureInfo(actionApiDTO, info.getReconfigure(), context);
                break;
            case PROVISION:
                addProvisionInfo(actionApiDTO, info.getProvision(), context);
                break;
            case RESIZE:
                addResizeInfo(actionApiDTO, info.getResize(), context);
                break;
            case ACTIVATE:
                // if the ACTIVATE action was originally a MOVE, we need to set the action details
                // as if it was a MOVE, otherwise we call the ACTIVATE method.
                if (info.getActionTypeCase() == ActionTypeCase.MOVE) {
                    addMoveInfo(actionApiDTO, recommendation, context, ActionType.START);
                } else {
                    addActivateInfo(actionApiDTO, info.getActivate(), context);
                }
                break;
            case DEACTIVATE:
                addDeactivateInfo(actionApiDTO, info.getDeactivate(), context);
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

        // update actionApiDTO with more info for plan actions
        if (topologyContextId != realtimeTopologyContextId) {
            addMoreInfoToActionApiDTOForPlan(actionApiDTO, context, recommendation);
        }

        return actionApiDTO;
    }

    /**
     * Update the given ActionApiDTO with more info for plan actions, such as aspects, template,
     * location, etc.
     *
     * @param actionApiDTO the plan ActionApiDTO to add more info to
     * @param context the ActionSpecMappingContext
     * @param action action info
     * @throws UnknownObjectException if target entity is not found
     * @throws UnsupportedActionException if the action type of the {@link ActionSpec}
     * is not supported.
     */
    private void addMoreInfoToActionApiDTOForPlan(@Nonnull ActionApiDTO actionApiDTO,
                                                  @Nonnull ActionSpecMappingContext context,
                                                  @Nonnull ActionDTO.Action action)
                throws UnknownObjectException, UnsupportedActionException {
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
        targetEntity.setAspectsByName(aspects);

        // add more info for cloud actions
        if (newEntity != null && CLOUD_ACTIONS_TIER_VALUES.contains(newEntity.getClassName())) {
            // set template for cloud actions, which is the new tier the entity is using
            TemplateApiDTO templateApiDTO = new TemplateApiDTO();
            templateApiDTO.setUuid(newEntity.getUuid());
            templateApiDTO.setDisplayName(newEntity.getDisplayName());
            templateApiDTO.setClassName(newEntity.getClassName());
            actionApiDTO.setTemplate(templateApiDTO);

            /*
             * Set virtualDisks on ActionApiDTO. Scale virtual volume actions have virtual volume as
             * target entity after converting to the ActionApiDTO. SO we need get VM ID from action info.
             */
            final boolean isVirtualVolumeTarget = targetEntity.getClassName()
                            .equals(UIEntityType.VIRTUAL_VOLUME.apiStr());
            final Long vmId = isVirtualVolumeTarget
                            ? ActionDTOUtil.getPrimaryEntity(action, false).getId()
                            : targetEntityId;
            // set location, which is the region
            final ApiPartialEntity region = context.getRegion(vmId);
            if (region != null) {
                final BaseApiDTO regionDTO = serviceEntityMapper.toServiceEntityApiDTO(region);
                // todo: set current and new location to be different if region could be changed
                actionApiDTO.setCurrentLocation(regionDTO);
                actionApiDTO.setNewLocation(regionDTO);
            }

            // Filter virtual disks if it is scale virtual volume action.
            final Predicate<VirtualDiskApiDTO> filter = isVirtualVolumeTarget
                            ? vd -> targetEntityUuid.equals(vd.getUuid())
                            : vd -> true;

            final List<VirtualDiskApiDTO> virtualDisks = context.getVolumeAspects(vmId).stream()
                            .filter(filter)
                            .collect(Collectors.toList());
            actionApiDTO.setVirtualDisks(virtualDisks);
        }
    }

    /**
     * Creates the stats for the given actionSpec
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
     * Creates the savings stats
     *
     * @param savingsPerHour the savings per hour
     * @return the savings stats
     */
    private Optional<StatApiDTO> createSavingsStat(CloudCostDTO.CurrencyAmount savingsPerHour) {
        if (savingsPerHour.getAmount() != 0) {
            // Get the currency
            Currency currency = CURRENCIES_BY_NUMERIC_CODE.get(savingsPerHour.getCurrency());
            if (currency == null) {
                currency = Currency.getInstance("USD");
                logger.warn("Cannot find currency code {}. Defaulting to {}.",
                        savingsPerHour.getCurrency(), currency.getDisplayName());
            }
            // Get the amount rounded to 7 decimal places. We round to 7 decimal places because we
            // convert this to a monthly savings number, and if we round to less than 7 decimal
            // places, then we might lose a few tens of dollars in savings
            Optional<Float> savingsAmount = roundToFloat(savingsPerHour.getAmount(), 7);
            if (savingsAmount.isPresent()) {
                StatApiDTO dto = new StatApiDTO();
                dto.setName(StringConstants.COST_PRICE);
                dto.setValue(savingsAmount.get());
                // The savings
                dto.setUnits(currency.getSymbol() + "/h");
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
        } else {
            return decisionUserUuid.substring(0, decisionUserUuid.indexOf("("));
        }
    }

    private void setRelatedDatacenter(long oid,
                                      @Nonnull ActionApiDTO actionApiDTO,
                                      @Nonnull ActionSpecMappingContext context,
                                      boolean newLocation) {
        context.getDatacenterFromOid(oid)
            .ifPresent(apiPartialEntity -> {
                final BaseApiDTO baseApiDTO = serviceEntityMapper.toServiceEntityApiDTO(apiPartialEntity);
                if (newLocation) {
                    actionApiDTO.setNewLocation(baseApiDTO);
                } else {
                    actionApiDTO.setCurrentLocation(baseApiDTO);
                }
            });
    }


    @Nonnull
    private String createRiskDescription(@Nonnull final ActionSpec actionSpec,
                    @Nonnull final ActionSpecMappingContext context) throws UnsupportedActionException {
        final Optional<String> policyId = tryExtractPlacementPolicyId(actionSpec.getRecommendation());
        if (policyId.isPresent()) {
            final long entityOid = ActionDTOUtil.getPrimaryEntityId(actionSpec.getRecommendation());
            final long policyOid = Long.parseLong(policyId.get());

            try {
                final Optional<PolicyDTO.Policy> policy =
                                Optional.ofNullable(context.getPolicy(policyOid));
                if (!policy.isPresent()) {
                    return actionSpec.getExplanation();
                }
                if (actionSpec.getRecommendation().getExplanation().hasProvision()) {
                    return String.format("%s violation",
                            policy.get().getPolicyInfo().getName());
                } else {
                    // constructing risk with policyName for move and reconfigure
                    Optional<String> commNames = nonSegmentationCommoditiesToString(actionSpec.getRecommendation());
                    return String.format("%s doesn't comply to %s%s",
                            context.getEntity(entityOid).getDisplayName(),
                            policy.get().getPolicyInfo().getName(),
                            commNames.isPresent() ?
                                    ", " + commNames.get() : "");
                }
            } catch (UnknownObjectException ex) {
                logger.error(String.format("Cannot resolve VM with oid %s from context", entityOid), ex);
            }
        }
        return translateExplanation(actionSpec.getExplanation(), context);
    }

    /**
     * Return comma seperated list of commodities to be reconfigured on the consumer
     *
     * @param recommendation contains the entityId for the action
     * @return String
     */
    private Optional<String> nonSegmentationCommoditiesToString(@Nonnull ActionDTO.Action recommendation) {
        if (!recommendation.getInfo().hasReconfigure()) {
            return Optional.empty();
        }
        // if its a reconfigure due to SEGMENTATION commodity (among other commodities),
        // we override the explanation generated by market eg., "Enable supplier to offer requested resource(s) Segmentation, Network networkABC"
        // with "vmName doesn't comply with policyName, networkABC"
        if (recommendation.getExplanation().getReconfigure().getReconfigureCommodityCount() < 1) {
            return Optional.empty();
        }
        String commNames = ActionDTOUtil.getReasonCommodities(recommendation)
                .filter(comm -> comm.getCommodityType().getType()
                        != CommodityDTO.CommodityType.SEGMENTATION_VALUE)
                .map(ReasonCommodity::getCommodityType)
                .map(commType -> commodityDisplayName(commType, false))
                .collect(Collectors.joining(", "));
        if (commNames.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(commNames);
    }

    @Nonnull
    private String commodityDisplayName(@Nonnull final TopologyDTO.CommodityType commType, final boolean keepItShort) {
        if (keepItShort) {
            return UICommodityType.fromType(commType).apiStr();
        } else {
            return ActionDTOUtil.getCommodityDisplayName(commType);
        }
    }

    /**
     * Translates placeholders in the input string with values from the {@link ActionSpecMappingContext}.
     *
     * If the string doesn't start with the TRANSLATION_PREFIX, return the original string.
     *
     * Otherwise, translate any translation fragments into text. Translation fragments have the
     * syntax:
     *     {entity:(oid):(field-name):(default-value)}
     *
     * Where "entity" is a static prefix, (oid) is the entity oid to look up, and (field-name) is the
     * name of the entity property to fetch. The entity property value will be substituted into the
     * string. If the entity is not found, or there is an error getting the property value, the
     * (default-value) will be used instead.
     *
     * @param input
     * @param context
     * @return
     */
    @VisibleForTesting
    public static String translateExplanation(String input, @Nonnull final ActionSpecMappingContext context) {
        if (! input.startsWith(TRANSLATION_PREFIX)) {
            // most of the time, we probably won't need to translate anything.
            return input;
        }

        StringBuilder sb = new StringBuilder();
        int lastOffset = TRANSLATION_PREFIX.length(); // offset to start appending from
        // we do need some minor translation. fill in the blanks here.
        Matcher matcher = TRANSLATION_PATTERN.matcher(input);
        while (matcher.find()) {
            // append the part of the string between regions
            sb.append(input, lastOffset, matcher.start());
            lastOffset = matcher.end();
            // replace the pattern
            try {
                long oid = Long.valueOf(matcher.group(1));
                ServiceEntityApiDTO entity = context.getEntity(oid);
                // invoke the getter via reflection
                Object fieldValue = new PropertyDescriptor(matcher.group(2), ServiceEntityApiDTO.class).getReadMethod().invoke(entity);
                sb.append(fieldValue);
            } catch (Exception e) {
                logger.warn("Couldn't translate entity {}:{} -- using default value {}",
                        matcher.group(1), matcher.group(2), matcher.group(3), e);
                // use the substitute/fallback value
                sb.append(matcher.group(3));
            }
        }
        // add the remainder of the input string
        sb.append(input, lastOffset, input.length());
        return sb.toString();
    }

    private Optional<String> tryExtractPlacementPolicyId(@Nonnull ActionDTO.Action recommendation) {
        if (!recommendation.hasExplanation()) {
            return Optional.empty();
        }
        if (recommendation.getExplanation().hasMove()) {

            if (recommendation.getExplanation().getMove().getChangeProviderExplanationCount() < 1) {
                return Optional.empty();
            }

            List<ChangeProviderExplanation> explanations = recommendation.getExplanation()
                    .getMove().getChangeProviderExplanationList();

            // We always go with the primary explanation if available
            Optional<ChangeProviderExplanation> primaryExp = explanations.stream()
                    .filter(ChangeProviderExplanation::getIsPrimaryChangeProviderExplanation).findFirst();
            final ChangeProviderExplanation explanation = primaryExp.orElse(explanations.get(0));

            if (!explanation.hasCompliance()) {
                return Optional.empty();
            }
            if (explanation.getCompliance().getMissingCommoditiesCount() < 1) {
                return Optional.empty();
            }
            if (explanation.getCompliance().getMissingCommodities(0).getCommodityType()
                    .getType() != CommodityDTO.CommodityType.SEGMENTATION_VALUE) {
                return Optional.empty();
            }
            return Optional.of(explanation.getCompliance().getMissingCommodities(0).getCommodityType()
                    .getKey());
        } else if (recommendation.getExplanation().hasReconfigure()) {
            if (recommendation.getExplanation().getReconfigure().getReconfigureCommodityCount() < 1) {
                return Optional.empty();
            }
            Optional<ReasonCommodity> reasonCommodity = recommendation.getExplanation().getReconfigure().getReconfigureCommodityList().stream()
                    .filter(comm -> comm.getCommodityType().getType()
                            == CommodityDTO.CommodityType.SEGMENTATION_VALUE).findFirst();
            if (!reasonCommodity.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(reasonCommodity.get().getCommodityType().getKey());
        } else if (recommendation.getExplanation().hasProvision()) {
            if (!recommendation.getExplanation().getProvision().hasProvisionBySupplyExplanation()) {
                return Optional.empty();
            }
            ReasonCommodity reasonCommodity = recommendation.getExplanation().getProvision().getProvisionBySupplyExplanation().getMostExpensiveCommodityInfo();
            if (reasonCommodity.getCommodityType().getType() != CommodityDTO.CommodityType.SEGMENTATION_VALUE) {
                return Optional.empty();
            }
            return Optional.of(reasonCommodity.getCommodityType().getKey());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Populate various fields of the {@link ActionApiDTO} representing a (compound) move.
     *
     * @param wrapperDto the DTO that represents the move recommendation and
     * wraps other {@link ActionApiDTO}s
     * @param action Action object
     * @param context mapping from {@link ActionSpec} to {@link ActionApiDTO}
     * @param actionType {@link ActionType} that will be assigned to wrapperDto param
     * @throws UnknownObjectException when the actions involve an unrecognized (out of
     * context) oid
     * @throws UnsupportedActionException if the action is an unsupported type.
     */
    private void addMoveInfo(@Nonnull final ActionApiDTO wrapperDto,
                             @Nonnull final ActionDTO.Action action,
                             @Nonnull final ActionSpecMappingContext context,
                             @Nonnull final ActionType actionType)
            throws UnknownObjectException, UnsupportedActionException {

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
        wrapperDto.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(target.getId())));

        final ChangeProvider primaryChange = ActionDTOUtil.getPrimaryChangeProvider(action);
        final boolean hasPrimarySource = !initialPlacement && primaryChange.getSource().hasId();
        if (hasPrimarySource) {
            long primarySourceId = primaryChange.getSource().getId();
            wrapperDto.setCurrentValue(Long.toString(primarySourceId));
            wrapperDto.setCurrentEntity(
                ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(primarySourceId)));
            setRelatedDatacenter(primarySourceId, wrapperDto, context, false);
        } else {
            // For less brittle UI integration, we set the current entity to an empty object.
            // The UI sometimes checks the validity of the "currentEntity.uuid" field,
            // which throws an error if current entity is unset.
            wrapperDto.setCurrentEntity(new ServiceEntityApiDTO());
        }
        long primaryDestinationId = primaryChange.getDestination().getId();
        wrapperDto.setNewValue(Long.toString(primaryDestinationId));
        wrapperDto.setNewEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(primaryDestinationId)));
        setRelatedDatacenter(primaryDestinationId, wrapperDto, context, true);

        List<ActionApiDTO> actions = Lists.newArrayList();
        for (ChangeProvider change : ActionDTOUtil.getChangeProviderList(action)) {
            actions.add(singleMove(actionType, wrapperDto, target.getId(), change, context));
        }
        wrapperDto.addCompoundActions(actions);

        wrapperDto.getRisk().setReasonCommodity(getReasonCommodities(changeProviderExplanationList));

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

    private String getReasonCommodities(List<ChangeProviderExplanation> changeProviderExplanations) {
        // Using set to avoid duplicates
        Set<ReasonCommodity> reasonCommodities = new HashSet<>();
        for (ChangeProviderExplanation changeProviderExplanation : changeProviderExplanations) {
            switch (changeProviderExplanation.getChangeProviderExplanationTypeCase()) {
                case COMPLIANCE:
                    reasonCommodities.addAll(changeProviderExplanation.getCompliance().getMissingCommoditiesList());
                    break;
                case CONGESTION:
                    reasonCommodities.addAll(changeProviderExplanation.getCongestion().getCongestedCommoditiesList());
                    break;
            }
        }

        return reasonCommodities.stream()
                .map(ReasonCommodity::getCommodityType)
                .map(commodityType -> CommodityDTO.CommodityType.forNumber(commodityType.getType()).name())
                .collect(Collectors.joining(", "));
    }

    private ActionApiDTO singleMove(ActionType actionType, ActionApiDTO compoundDto,
                    final long targetId,
                    @Nonnull final ChangeProvider change,
                    @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException {
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
        actionApiDTO.setTarget(ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(targetId)));

        final boolean hasSource = change.getSource().hasId();
        if (hasSource) {
            final long sourceId = change.getSource().getId();
            actionApiDTO.setCurrentValue(Long.toString(sourceId));
            actionApiDTO.setCurrentEntity(
                ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(sourceId)));
        }
        actionApiDTO.setNewValue(Long.toString(destinationId));
        actionApiDTO.setNewEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(destinationId)));

        // Set action details
        actionApiDTO.setDetails(actionDetails(hasSource, actionApiDTO, targetId, change, context));
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
            Optional<ServiceEntityApiDTO> destination = context.getOptionalEntity(destinationId);
            Optional<ServiceEntityApiDTO> source = context.getOptionalEntity(sourceId);
            final String verb =
                SCALE_TIER_VALUES.contains(destination.map(BaseApiDTO::getClassName).orElse("")) &&
                    SCALE_TIER_VALUES.contains(source.map(BaseApiDTO::getClassName).orElse("")) ?
                "Scale" : "Move";
            String resource = "";
            if (change.hasResource()) {
                final long resourceId = change.getResource().getId();
                final Optional<ServiceEntityApiDTO> resourceEntity =
                    context.getOptionalEntity(resourceId);
                if (resourceEntity.isPresent() && resourceId != targetId) {
                    resource = readableEntityTypeAndName(resourceEntity.get()) + " of ";
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
            @Nonnull final ActionSpecMappingContext context)
            throws UnknownObjectException {
        // Set action type
        actionApiDTO.setActionType(ActionType.ALLOCATE);

        // Set action target
        ActionDTO.ActionInfo actionInfo = allocateActionSpec.getRecommendation().getInfo();
        final long targetId = actionInfo.getAllocate().getTarget().getId();
        final ServiceEntityApiDTO target = context.getEntity(targetId);
        actionApiDTO.setTarget(ServiceEntityMapper.copyServiceEntityAPIDTO(target));

        // Set template family in current entity
        final String templateFamily =
                allocateActionSpec.getRecommendation().getExplanation().getAllocate().getInstanceSizeFamily();
        ServiceEntityApiDTO serviceEntityApiDTO = new ServiceEntityApiDTO();
        serviceEntityApiDTO.setDisplayName(templateFamily);
        serviceEntityApiDTO.setClassName(UIEntityType.COMPUTE_TIER.apiStr());
        actionApiDTO.setCurrentEntity(serviceEntityApiDTO);

        // Set action current and new locations (should be the same for Allocate)
        setCurrentAndNewLocation(targetId, context, actionApiDTO);

        // Set Cloud aspect
        context.getCloudAspect(targetId).ifPresent(cloudAspect -> {
            final Map<AspectName, EntityAspect> aspects = new HashMap<>();
            aspects.put(AspectName.CLOUD, cloudAspect);
            actionApiDTO.getTarget().setAspectsByName(aspects);
        });
    }

    private void addReconfigureInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                    @Nonnull final Reconfigure reconfigure,
                                    @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        actionApiDTO.setActionType(ActionType.RECONFIGURE);

        actionApiDTO.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(reconfigure.getTarget().getId())));
        // Since we may or may not have a current entity, we store the DC for the target as the
        // new location.  This way, the UI will always be able to find a DC in one of the two
        // places.
        setRelatedDatacenter(reconfigure.getTarget().getId(), actionApiDTO, context, true);
        if (reconfigure.hasSource()) {
            actionApiDTO.setCurrentEntity(
                ServiceEntityMapper.copyServiceEntityAPIDTO(
                    context.getEntity(reconfigure.getSource().getId())));
            setRelatedDatacenter(reconfigure.getSource().getId(), actionApiDTO, context, false);
        } else {
            // For less brittle UI integration, we set the current entity to an empty object.
            // The UI sometimes checks the validity of the "currentEntity.uuid" field,
            // which throws an error if current entity is unset.
            actionApiDTO.setCurrentEntity(new ServiceEntityApiDTO());
        }

        actionApiDTO.setCurrentValue(Long.toString(reconfigure.getSource().getId()));
    }

    private void addProvisionInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                  @Nonnull final Provision provision,
                                  @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        final long currentEntityId = provision.getEntityToClone().getId();
        final long provisionedSellerId = provision.getProvisionedSeller();

        actionApiDTO.setActionType(ActionType.PROVISION);

        actionApiDTO.setCurrentValue(Long.toString(currentEntityId));
        actionApiDTO.setCurrentEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(
                context.getEntity(currentEntityId)));
        setRelatedDatacenter(currentEntityId, actionApiDTO, context, false);
        setRelatedDatacenter(currentEntityId, actionApiDTO, context, true);

        actionApiDTO.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(actionApiDTO.getCurrentEntity()));

        if (context.isPlan()) {
            // In plan actions we want to provide a reference to the provisioned entities, because
            // we will show other actions (e.g. moves/starts) that involve the provisioned entities.
            //
            // The "new" entity is the provisioned seller.
            final ServiceEntityApiDTO newEntity = ServiceEntityMapper.copyServiceEntityAPIDTO(
                context.getEntity(provisionedSellerId));
            actionApiDTO.setNewEntity(newEntity);
            actionApiDTO.setNewValue(newEntity.getUuid());
        } else {
            // In realtime actions we don't provide a reference to the provisioned entities, because
            // they do not exist in the projected topology. This is because provisioning is not
            // something that can be realistically executed, and we don't show the impact of
            // provisions when constructing the projected topology.
            actionApiDTO.setNewEntity(new ServiceEntityApiDTO());
        }
    }

    private void addResizeInfo(@Nonnull final ActionApiDTO actionApiDTO,
                               @Nonnull final Resize resize,
                               @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        actionApiDTO.setActionType(ActionType.RESIZE);

        long originalEntityOid = resize.getTarget().getId();
        actionApiDTO.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(originalEntityOid)));
        actionApiDTO.setCurrentEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(originalEntityOid)));
        actionApiDTO.setNewEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(originalEntityOid)));
        setRelatedDatacenter(originalEntityOid, actionApiDTO, context, false);
        setRelatedDatacenter(originalEntityOid, actionApiDTO, context, true);

        final CommodityDTO.CommodityType commodityType = CommodityDTO.CommodityType.forNumber(
                resize.getCommodityType().getType());
        Objects.requireNonNull(commodityType, "Commodity for number "
                + resize.getCommodityType().getType());
        actionApiDTO.getRisk().setReasonCommodity(commodityType.name());
        if (resize.hasCommodityAttribute()) {
            actionApiDTO.setResizeAttribute(resize.getCommodityAttribute().name());
        }
        actionApiDTO.setCurrentValue(Float.toString(resize.getOldCapacity()));
        actionApiDTO.setResizeToValue(Float.toString(resize.getNewCapacity()));

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
     * @throws UnknownObjectException
     */
    private void setCurrentAndNewLocation(long targetUuid, ActionSpecMappingContext context, ActionApiDTO actionApiDTO)
        throws UnknownObjectException {
        ApiPartialEntity region = context.getRegion(targetUuid);
        if (region != null) {
            BaseApiDTO regionDTO = serviceEntityMapper.toServiceEntityApiDTO(region);
            regionDTO.setDisplayName(region.getDisplayName());
            actionApiDTO.setCurrentLocation(regionDTO);
            actionApiDTO.setNewLocation(regionDTO);
        }
    }

    /**
     * Adds information to a RI Buy Action.
     * @param actionApiDTO Action API DTO.
     * @param buyRI Buy RI DTO.
     * @param context ActionSpecMappingContext.
     * @throws UnknownObjectException
     */
    private void addBuyRIInfo(@Nonnull final ActionApiDTO actionApiDTO,
                              @Nonnull final BuyRI buyRI,
                              @Nonnull final ActionSpecMappingContext context)
                              throws UnknownObjectException {
        actionApiDTO.setActionType(ActionType.BUY_RI);

        final Pair<ReservedInstanceBought, ReservedInstanceSpec> pair = context
                .getRIBoughtandRISpec(buyRI.getBuyRiId());

        final ReservedInstanceBought ri = pair.first;
        final ReservedInstanceSpec riSpec = pair.second;

        try {
            ReservedInstanceApiDTO riApiDTO = reservedInstanceMapper
                    .mapToReservedInstanceApiDTO(ri, riSpec, context.getServiceEntityApiDTOs());
            actionApiDTO.setReservedInstance(riApiDTO);
            actionApiDTO.setTarget(
                    ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(buyRI.getRegion().getId())));
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
            actionApiDTO.setResizeToValue(formatBuyRIResizeToValue(riApiDTO));
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
    private final String formatBuyRIResizeToValue(ReservedInstanceApiDTO ri) {
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
                                 @Nonnull final Activate activate,
                                 @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException {
        actionApiDTO.setActionType(ActionType.START);
        final long targetEntityId = activate.getTarget().getId();
        actionApiDTO.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(targetEntityId)));
        actionApiDTO.setCurrentEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(targetEntityId)));
        setRelatedDatacenter(targetEntityId, actionApiDTO, context, false);

        final List<String> reasonCommodityNames =
            activate.getTriggeringCommoditiesList().stream()
                .map(commodityType -> CommodityDTO.CommodityType
                    .forNumber(commodityType.getType()))
                .map(CommodityDTO.CommodityType::name)
                .collect(Collectors.toList());

        actionApiDTO.getRisk()
            .setReasonCommodity(reasonCommodityNames.stream().collect(Collectors.joining(",")));
    }

    private void addDeactivateInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                   @Nonnull final Deactivate deactivate,
                                   @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        final long targetEntityId = deactivate.getTarget().getId();
        actionApiDTO.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(targetEntityId)));
        actionApiDTO.setCurrentEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(targetEntityId)));
        setRelatedDatacenter(targetEntityId, actionApiDTO, context, false);

        actionApiDTO.setActionType(ActionType.SUSPEND);

        final List<String> reasonCommodityNames =
                deactivate.getTriggeringCommoditiesList().stream()
                        .map(commodityType -> CommodityDTO.CommodityType
                                .forNumber(commodityType.getType()))
                        .map(CommodityDTO.CommodityType::name)
                        .collect(Collectors.toList());

        actionApiDTO.getRisk().setReasonCommodity(
            reasonCommodityNames.stream().collect(Collectors.joining(",")));
    }

    /**
     * Add information related to a Delete action to the actionApiDTO.  Note that this currently only
     * handles on prem wasted file delete actions.
     *
     * @param actionApiDTO the {@link ActionApiDTO} we are populating
     * @param delete the {@link Delete} action info object that contains the basic delete action parameters
     * @param deleteExplanation the {@link DeleteExplanation} that contains the details of the action
     * @param context the {@link ActionSpecMappingContext}
     * @throws UnknownObjectException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private void addDeleteInfo(@Nonnull final ActionApiDTO actionApiDTO,
                               @Nonnull final Delete delete,
                               @Nonnull final DeleteExplanation deleteExplanation,
                               @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        final long targetEntityId = delete.getTarget().getId();
        actionApiDTO.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(targetEntityId)));
        actionApiDTO.setCurrentEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(targetEntityId)));
        actionApiDTO.setActionType(ActionType.DELETE);
        long deletedSizeinKB = deleteExplanation.getSizeKb();
        if (deletedSizeinKB > 0) {
            actionApiDTO.setCurrentValue(Math.round(deletedSizeinKB / Units.NUM_OF_KB_IN_MB) + "MB");
        }
        // set the virtualDisks field on ActionApiDTO, only one VirtualDiskApiDTO should be set,
        // since there is only one file (on-prem) or volume (cloud) associated with DELETE action
        if (delete.hasFilePath()) {
            VirtualDiskApiDTO virtualDiskApiDTO = new VirtualDiskApiDTO();
            virtualDiskApiDTO.setDisplayName(delete.getFilePath());
            actionApiDTO.setVirtualDisks(Collections.singletonList(virtualDiskApiDTO));
        }
    }

    /**
     * Return a nicely formatted string like:
     *
     * <p><code>Virtual Machine vm-test 01 for now</code>
     *
     * <p>in which the entity type is expanded from camel case to words, and the displayName()
     * is surrounded with single quotes.
     *
     * The regex uses zero-length pattern matching with lookbehind and lookforward, and is
     * taken from - http://stackoverflow.com/questions/2559759.
     *
     * It converts camel case (e.g. PhysicalMachine) into strings with the same
     * capitalization plus blank spaces (e.g. "Physical Machine"). It also splits numbers,
     * e.g. "May5" -> "May 5" and respects upper case runs, e.g. (PDFLoader -> "PDF Loader").
     *
     * @param entityDTO the entity for which the readable name is to be created
     * @return a string with the entity type, with blanks inserted, plus displayName with
     * single quotes
     */
    private String readableEntityTypeAndName(BaseApiDTO entityDTO) {
        final String fullType = entityDTO.getClassName();
        final String shortenedIfNecessary = SHORTENED_ENTITY_TYPES.getOrDefault(fullType, fullType);
        return String.format("%s %s",
            StringUtil.getSpaceSeparatedWordsFromCamelCaseString(shortenedIfNecessary),
            entityDTO.getDisplayName()
        );
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
            // TODO (roman, Dec 28 2016): This is only implementing a small subset of
            // query options. Need to do another pass, including handling
            // action states that don't map to ActionDTO.ActionState neatly,
            // dealing with decisions/ActionModes, etc.
            if (inputDto.getActionStateList() != null) {
                inputDto.getActionStateList().stream()
                    .map(this::mapApiStateToXl)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(queryBuilder::addStates);
            } else {
                // TODO: (DavidBlinn, 3/15/2018): The UI request for "Pending Actions" does not
                // include any action states in its filter even though it wants to exclude executed
                // actions. Request only operational action states.
                Stream.of(OPERATIONAL_ACTION_STATES).forEach(queryBuilder::addStates);
            }

            // Map UI's ActionMode to ActionDTO.ActionMode and add them to filter
            if (inputDto.getActionModeList() != null) {
                inputDto.getActionModeList().stream()
                    .map(this::mapApiModeToXl)
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

            // pass in start and end time
            if (inputDto.getStartTime() != null && !inputDto.getStartTime().isEmpty()) {
                queryBuilder.setStartDate(DateTimeUtil.parseTime(inputDto.getStartTime()));
            }

            if (inputDto.getEndTime() != null && !inputDto.getEndTime().isEmpty()) {
                queryBuilder.setEndDate(DateTimeUtil.parseTime(inputDto.getEndTime()));
            }

            if (inputDto.getEnvironmentType() != null) {
                UIEnvironmentType.fromString(inputDto.getEnvironmentType().name()).toEnvType()
                    .ifPresent(queryBuilder::setEnvironmentType);
            }

            if (CollectionUtils.isNotEmpty(inputDto.getRelatedEntityTypes())) {
                inputDto.getRelatedEntityTypes().stream()
                    .map(UIEntityType::fromString)
                    .map(UIEntityType::typeNumber)
                    .forEach(queryBuilder::addEntityType);
            }
        } else {
            // When "inputDto" is null, we should automatically insert the operational action states.
            Stream.of(OPERATIONAL_ACTION_STATES).forEach(queryBuilder::addStates);
        }

        // Set involved entities from user input and Buy RI scope
        final Set<Long> allInvolvedEntities = new HashSet<>(
                buyRiScopeHandler.extractBuyRiEntities(scopeId));
        involvedEntities.ifPresent(allInvolvedEntities::addAll);
        if (!allInvolvedEntities.isEmpty()) {
            queryBuilder.setInvolvedEntities(InvolvedEntities.newBuilder()
                    .addAllOids(allInvolvedEntities));
        }

        return queryBuilder.build();
    }

    @Nonnull
    public Optional<ActionDTO.ActionState> mapApiStateToXl(final ActionState stateStr) {
        switch (stateStr) {
            case READY:
                return Optional.of(ActionDTO.ActionState.READY);
            case ACCEPTED: case QUEUED:
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
                logger.error("Unknown action state {}", stateStr);
                throw new IllegalArgumentException("Unsupported action state " + stateStr);
        }
    }


    /**
     * Given an actionSpec fetches the corresponding action details.
     * @param action - ActionOrchestratorAction for the user sent action
     * @return actionDetailsApiDTO which contains extra information about a given action
     */
    @Nullable
    public ActionDetailsApiDTO createActionDetailsApiDTO(final ActionDTO.ActionOrchestratorAction action) {
        final ActionSpec actionSpec = action.getActionSpec();
        if (actionSpec != null && actionSpec.hasRecommendation()) {
            ActionDTO.ActionType actionType = ActionDTOUtil.getActionInfoActionType(actionSpec.getRecommendation());
            // Buy RI action - set est. on-demand cost and coverage values + historical demand data
            if (actionSpec.getRecommendation().hasExplanation() && actionType.equals(BUY_RI)) {
                RIBuyActionDetailsApiDTO detailsDto = new RIBuyActionDetailsApiDTO();
                // set est RI Coverage
                ActionDTO.Explanation.BuyRIExplanation buyRIExplanation = actionSpec.getRecommendation()
                        .getExplanation().getBuyRI();
                float covered = buyRIExplanation.getCoveredAverageDemand();
                float capacity = buyRIExplanation.getTotalAverageDemand();
                detailsDto.setEstimatedRICoverage((covered / capacity) * 100);
                // set est. on-demand cost
                detailsDto.setEstimatedOnDemandCost(buyRIExplanation.getEstimatedOnDemandCost());
                // set demand data
                Cost.riBuyDemandStats snapshots = riStub
                        .getRIBuyContextData(Cost.GetRIBuyContextRequest.newBuilder()
                                .setActionId(Long.toString(actionSpec.getRecommendation().getId())).build());
                List<StatSnapshotApiDTO> demandList =
                        createRiHistoricalContextStatSnapshotDTO(snapshots.getStatSnapshotsList());
                detailsDto.setHistoricalDemandData(demandList);
                return detailsDto;
            }
            if (actionType == RESIZE || actionType == SCALE || actionType == ALLOCATE) {
                long entityUuid;
                ActionEntity entity;
                try {
                    entity = ActionDTOUtil.getPrimaryEntity(action.getActionSpec().getRecommendation());
                    entityUuid = entity.getId();
                } catch (UnsupportedActionException e) {
                    logger.warn("Cannot create action details due to unsupported action type", e);
                    return null;
                }
                if (entity.getEnvironmentType() != EnvironmentTypeEnum.EnvironmentType.CLOUD) {
                    logger.warn("Cannot create action details for on-prem actions");
                    return null;
                }
                return createCloudResizeActionDetailsDTO(entityUuid);
            }
            return null;
        }
        return null;
    }

    /**
     * Create Cloud Resize Action Details DTO.
     * @param entityUuid - uuid of the action target entity
     * @return cloudResizeActionDetailsDTO - this contains additional details about the action
     * like on-demand rates, costs and RI coverage before/after the resize
     */
    @Nonnull
    public CloudResizeActionDetailsApiDTO createCloudResizeActionDetailsDTO(long entityUuid) {
        CloudResizeActionDetailsApiDTO cloudResizeActionDetailsApiDTO = new CloudResizeActionDetailsApiDTO();
        // get on-demand costs
        setOnDemandCosts(entityUuid, cloudResizeActionDetailsApiDTO);
        // get on-demand rates
        setOnDemandRates(entityUuid, cloudResizeActionDetailsApiDTO);
        // get RI coverage before/after
        setRiCoverage(entityUuid, cloudResizeActionDetailsApiDTO);
        return cloudResizeActionDetailsApiDTO;
    }

    /**
     * Set on-demand costs for target entity which factors in RI usage.
     * @param entityUuid - uuid of target entity
     * @param cloudResizeActionDetailsApiDTO - cloud resize action details DTO
     */
    private void setOnDemandCosts(long entityUuid, CloudResizeActionDetailsApiDTO cloudResizeActionDetailsApiDTO) {
        EntityFilter entityFilter = EntityFilter.newBuilder().addEntityId(entityUuid).build();
        GetCloudCostStatsRequest cloudCostStatsRequest = GetCloudCostStatsRequest
                .newBuilder().addCloudCostStatsQuery(CloudCostStatsQuery.newBuilder()
                .setRequestProjected(true)
                .setEntityFilter(entityFilter)
                // For cloud scale actions, the action savings will reflect only the savings from
                // accepting the specific action, ignoring any potential discount from Buy RI actions.
                // Therefore, we want the projected on-demand cost in the actions details to only
                // reflect the cost from accepting this action. We filter out BUY_RI_DISCOUNT here
                // to be consistent with the action savings calculation and to avoid double counting
                // potential savings from Buy RI actions
                .setCostSourceFilter(CostSourceFilter.newBuilder()
                        .setExclusionFilter(true)
                        .addCostSources(CostSource.BUY_RI_DISCOUNT))
                .setCostCategoryFilter(CostCategoryFilter.newBuilder()
                        .setExclusionFilter(false)
                        .addCostCategory(CostCategory.ON_DEMAND_COMPUTE)
                        .addCostCategory(CostCategory.ON_DEMAND_LICENSE)
                        .addCostCategory(CostCategory.RESERVED_LICENSE)))
                .build();
        GetCloudCostStatsResponse response = costServiceBlockingStub.getCloudCostStats(cloudCostStatsRequest);
        int statRecordListSize = response.getCloudStatRecordList().size();
        if (statRecordListSize == 2) {
            // get real-time
            Double onDemandCostBefore = response.getCloudStatRecord(0).getStatRecordsList()
                    .stream()
                    .map(StatRecord::getValues)
                    .mapToDouble(StatRecord.StatValue::getTotal)
                    .sum();
            // get projected
            Double onDemandCostAfter = response.getCloudStatRecord(1).getStatRecordsList()
                    .stream()
                    .map(StatRecord::getValues)
                    .mapToDouble(StatRecord.StatValue::getTotal)
                    .sum();
            cloudResizeActionDetailsApiDTO.setOnDemandCostBefore(onDemandCostBefore.floatValue());
            cloudResizeActionDetailsApiDTO.setOnDemandCostAfter(onDemandCostAfter.floatValue());
        }
    }

    /**
     * Set on-demand template rates for the target entity.
     * @param entityUuid - uuid of target entity
     * @param cloudResizeActionDetailsApiDTO - cloud resize action details DTO
     */
    private void setOnDemandRates(long entityUuid, CloudResizeActionDetailsApiDTO cloudResizeActionDetailsApiDTO) {

        // Get the On Demand compute costs
        GetTierPriceForEntitiesRequest onDemandComputeCostsRequest = GetTierPriceForEntitiesRequest
                .newBuilder().setOid(entityUuid)
                .setCostCategory(CostCategory.ON_DEMAND_COMPUTE).build();
        GetTierPriceForEntitiesResponse onDemandComputeCostsResponse = costServiceBlockingStub
                .getTierPriceForEntities(onDemandComputeCostsRequest);
        Map<Long, CurrencyAmount> beforeOnDemandComputeCostByEntityOidMap = onDemandComputeCostsResponse
                .getBeforeTierPriceByEntityOidMap();
        Map<Long, CurrencyAmount> afterComputeCostByEntityOidMap = onDemandComputeCostsResponse
                .getAfterTierPriceByEntityOidMap();

        // Get the On Demand License costs
        GetTierPriceForEntitiesRequest onDemandLicenseCostsRequest = GetTierPriceForEntitiesRequest
                .newBuilder().setOid(entityUuid)
                .setCostCategory(CostCategory.ON_DEMAND_LICENSE).build();
        GetTierPriceForEntitiesResponse onDemandLicenseCostsResponse = costServiceBlockingStub
                .getTierPriceForEntities(onDemandLicenseCostsRequest);
        Map<Long, CurrencyAmount> beforeLicenseComputeCosts = onDemandLicenseCostsResponse
                .getBeforeTierPriceByEntityOidMap();
        Map<Long, CurrencyAmount> afterLicenseComputeCosts = onDemandLicenseCostsResponse
                .getAfterTierPriceByEntityOidMap();

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
    }

    /**
     * Set RI Coverage before/after for the target entity.
     * @param entityUuid - uuid of target entity
     * @param cloudResizeActionDetailsApiDTO - cloud resize action details DTO
     */
    private void setRiCoverage(long entityUuid, CloudResizeActionDetailsApiDTO cloudResizeActionDetailsApiDTO) {
        final EntityFilter entityFilter = EntityFilter.newBuilder().addEntityId(entityUuid).build();

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

        // get projected RI coverage for target entity
        Cost.GetProjectedEntityReservedInstanceCoverageRequest projectedEntityReservedInstanceCoverageRequest =
                Cost.GetProjectedEntityReservedInstanceCoverageRequest
                .newBuilder()
                .setEntityFilter(entityFilter)
                .build();

        Cost.GetProjectedEntityReservedInstanceCoverageResponse projectedEntityReservedInstanceCoverageResponse =
                reservedInstanceUtilizationCoverageServiceBlockingStub
                    .getProjectedEntityReservedInstanceCoverageStats(projectedEntityReservedInstanceCoverageRequest);

        Map<Long, Cost.EntityReservedInstanceCoverage> projectedCoverageMap = projectedEntityReservedInstanceCoverageResponse
                .getCoverageByEntityIdMap();

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
    }

    /**
     * Create RI historical Context Stat Snapshot DTOs
     * snapshots - template demand snapshots
     * @return
     */
    @Nonnull
    private List<StatSnapshotApiDTO> createRiHistoricalContextStatSnapshotDTO(final List<Stats.StatSnapshot> snapshots) {
        final List<StatSnapshotApiDTO> statSnapshotApiDTOList = new ArrayList<>();
        for (Stats.StatSnapshot snapshot : snapshots) {
            // The records we obtain start one week back in time from snapshot.getTimestamp()
            // So we subtract a week from that.
            // Each of the subsequent record has an 1 hour incremental timestamp from its previous record.
            final Long contextStartDate = snapshot.getSnapshotDate() - ((long)Units.WEEK_MS);
            int index = 0;
            // Create 168 snapshots in hourly intervals
            for (Stats.StatSnapshot.StatRecord record : snapshot.getStatRecordsList()) {
                final StatSnapshotApiDTO statSnapshotApiDTO = new StatSnapshotApiDTO();
                List<StatApiDTO> statApiDTOList = new ArrayList<>();
                StatApiDTO statApiDTO = new StatApiDTO();
                statApiDTO.setValue((record.getValues().getAvg()));
                statApiDTOList.add(statApiDTO);
                statSnapshotApiDTO.setStatistics(statApiDTOList);
                statSnapshotApiDTO.setDisplayName(record.getStatKey());
                statSnapshotApiDTO.setDate(Long.toString(contextStartDate + (index * (long)Units.HOUR_MS)));
                statSnapshotApiDTOList.add(statSnapshotApiDTO);
                index++;
            }
        }
        return statSnapshotApiDTOList;
    }

    /**
     * Map UI's ActionMode to ActionDTO.ActionMode
     *
     * @param actionMode UI's ActionMode
     * @return ActionDTO.ActionMode
     */
    @Nonnull
    public Optional<ActionDTO.ActionMode> mapApiModeToXl(final ActionMode actionMode) {
        switch (actionMode) {
            case DISABLED:
                return Optional.of(ActionDTO.ActionMode.DISABLED);
            case RECOMMEND:
                return Optional.of(ActionDTO.ActionMode.RECOMMEND);
            case MANUAL:
                return Optional.of(ActionDTO.ActionMode.MANUAL);
            case AUTOMATIC:
                return Optional.of(ActionDTO.ActionMode.AUTOMATIC);
            default:
                logger.error("Unknown action mode {}", actionMode);
                return Optional.empty();
        }
    }
}
