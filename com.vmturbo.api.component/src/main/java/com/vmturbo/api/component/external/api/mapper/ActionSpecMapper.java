package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.action.ActionDTOUtil.TRANSLATION_PATTERN;
import static com.vmturbo.common.protobuf.action.ActionDTOUtil.TRANSLATION_PREFIX;

import java.beans.PropertyDescriptor;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.MessageFormat;
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
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.vmturbo.api.component.external.api.mapper.ActionSpecMappingContextFactory.ActionSpecMappingContext;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.entityaspect.EntityAspect;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Delete;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.DeleteExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.Units;
import com.vmturbo.components.common.mapping.UIEnvironmentType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO;

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


    private static final String STORAGE_VALUE = UIEntityType.STORAGE.getValue();
    private static final String STORAGE_TIER_VALUE = UIEntityType.STORAGE_TIER.getValue();
    private static final String PHYSICAL_MACHINE_VALUE = UIEntityType.PHYSICAL_MACHINE.getValue();
    private static final String DISK_ARRAY_VALUE = UIEntityType.DISKARRAY.getValue();
    private static final Set<String> PRIMARY_TIER_VALUES = ImmutableSet.of(
            UIEntityType.COMPUTE_TIER.getValue(), UIEntityType.DATABASE_SERVER_TIER.getValue(),
            UIEntityType.DATABASE_TIER.getValue());
    private static final Set<String> TIER_VALUES = ImmutableSet.of(
            UIEntityType.COMPUTE_TIER.getValue(), UIEntityType.DATABASE_SERVER_TIER.getValue(),
            UIEntityType.DATABASE_TIER.getValue(), UIEntityType.STORAGE_TIER.getValue());
    private static final Set<String> STORAGE_VALUES = ImmutableSet.of(
            UIEntityType.STORAGE_TIER.getValue(), UIEntityType.STORAGE.getValue());

    private static final String UP = "up";
    private static final String DOWN = "down";

    // Commodities in actions mapped to their default units.
    // For example, vMem commodity has its default capacity unit as KB.
    private static final ImmutableMap<CommodityDTO.CommodityType, Long> commodityTypeToDefaultUnits =
                    new ImmutableMap.Builder<CommodityDTO.CommodityType, Long>()
                        .put(CommodityDTO.CommodityType.VMEM, Units.KBYTE)
                        .put(CommodityDTO.CommodityType.STORAGE_AMOUNT, Units.MBYTE)
                        .put(CommodityDTO.CommodityType.HEAP, Units.KBYTE)
                        .build();

    private final ActionSpecMappingContextFactory actionSpecMappingContextFactory;

    private final long realtimeTopologyContextId;

    private static final Logger logger = LogManager.getLogger();

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
                            final long realtimeTopologyContextId) {
        this.actionSpecMappingContextFactory = Objects.requireNonNull(actionSpecMappingContextFactory);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
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
                if (Objects.nonNull(actionApiDTO)) {
                    actionApiDTOS.add(actionApiDTO);
                }
            } catch (UnknownObjectException e) {
                unresolvedEntities+=1;
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
                    throws UnknownObjectException, ExecutionException, InterruptedException {
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

        // map the recommendation info
        final ActionDTO.Action recommendation = actionSpec.getRecommendation();
        LogEntryApiDTO risk = new LogEntryApiDTO();
        actionApiDTO.setImportance((float) recommendation.getImportance());
        risk.setImportance((float)recommendation.getImportance());
        // set the explanation string

        risk.setDescription(createRiskDescription(actionSpec, context));
        risk.setSubCategory(mapXlActionCategoryToApi(actionSpec.getCategory()));
        risk.setSeverity(
            ActionDTOUtil.getSeverityName(ActionDTOUtil.mapImportanceToSeverity(recommendation
                    .getImportance())));
        risk.setReasonCommodity("");
        actionApiDTO.setRisk(risk);

        // The target definition
        actionApiDTO.setStats(createStats(actionSpec));

        final ActionDTO.ActionInfo info = recommendation.getInfo();
        ActionDTO.ActionType actionType = ActionDTOUtil.getActionInfoActionType(recommendation);

        // handle different action types
        switch (actionType) {
            case MOVE:
                addMoveInfo(actionApiDTO, info.getMove(),
                    recommendation.getExplanation().getMove(), context, ActionType.MOVE);
                break;
            case RECONFIGURE:
                addReconfigureInfo(actionApiDTO, info.getReconfigure(),
                    recommendation.getExplanation().getReconfigure(), context);
                break;
            case PROVISION:
                addProvisionInfo(actionApiDTO, info.getProvision(), context);
                break;
            case RESIZE:
                // if the RESIZE action was originally a MOVE, we need to set the action details as
                // if it was a MOVE, otherwise we call the RESIZE method.
                if (info.getActionTypeCase() == ActionTypeCase.MOVE) {
                    addMoveInfo(actionApiDTO, info.getMove(),
                        recommendation.getExplanation().getMove(), context, ActionType.RESIZE);
                } else {
                    addResizeInfo(actionApiDTO, info.getResize(), context);
                }
                break;
            case ACTIVATE:
                // if the ACTIVATE action was originally a MOVE, we need to set the action details
                // as if it was a MOVE, otherwise we call the ACTIVATE method.
                if (info.getActionTypeCase() == ActionTypeCase.MOVE) {
                    addMoveInfo(actionApiDTO, info.getMove(),
                        recommendation.getExplanation().getMove(), context, ActionType.START);
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
            default:
                throw new IllegalArgumentException("Unsupported action type " + actionType);
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
                actionApiDTO.setUserName(decisionUserUUid);
                // update actionMode based on decision uer id
                // TODO: move it to Action Orchestrator (see OM-37935)
                updateActionMode(actionApiDTO, decisionUserUUid);
            }
        }

        // update actionApiDTO with more info for plan actions
        if (topologyContextId != realtimeTopologyContextId) {
            addMoreInfoToActionApiDTOForPlan(actionApiDTO, context);
        }

        return actionApiDTO;
    }

    /**
     * Update the given ActionApiDTO with more info for plan actions, such as aspects, template,
     * location, etc.
     *
     * @param actionApiDTO the plan ActionApiDTO to add more info to
     * @param context the ActionSpecMappingContext
     * @throws UnknownObjectException
     */
    private void addMoreInfoToActionApiDTOForPlan(@Nonnull ActionApiDTO actionApiDTO,
                                                  @Nonnull ActionSpecMappingContext context)
                throws UnknownObjectException {
        final ServiceEntityApiDTO targetEntity = actionApiDTO.getTarget();
        final ServiceEntityApiDTO newEntity = actionApiDTO.getNewEntity();
        final Long targetEntityId = Long.valueOf(targetEntity.getUuid());

        // add aspects to targetEntity
        final Map<String, EntityAspect> aspects = new HashMap<>();
        context.getCloudAspect(targetEntityId).map(cloudAspect -> aspects.put(
            StringConstants.CLOUD_ASPECT_NAME, cloudAspect));
        context.getVMAspect(targetEntityId).map(vmAspect -> aspects.put(
            StringConstants.VM_ASPECT_NAME, vmAspect));
        targetEntity.setAspects(aspects);

        // add more info for cloud actions
        if (newEntity != null && TIER_VALUES.contains(newEntity.getClassName())) {
            // set template for cloud actions, which is the new tier the entity is using
            TemplateApiDTO templateApiDTO = new TemplateApiDTO();
            templateApiDTO.setUuid(newEntity.getUuid());
            templateApiDTO.setDisplayName(newEntity.getDisplayName());
            templateApiDTO.setClassName(newEntity.getClassName());
            actionApiDTO.setTemplate(templateApiDTO);

            // set location, which is the region
            TopologyEntityDTO region = context.getRegionForVM(targetEntityId);
            if (region != null) {
                BaseApiDTO regionDTO = ServiceEntityMapper.toServiceEntityApiDTO(region, null);
                // todo: set current and new location to be different if region could be changed
                actionApiDTO.setCurrentLocation(regionDTO);
                actionApiDTO.setNewLocation(regionDTO);
            }

            // set virtualDisks on ActionApiDTO
            actionApiDTO.setVirtualDisks(context.getVolumeAspects(targetEntityId));
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
     * Rule: if the decision user id is "SYSTEM", set the action mode to "automatic".
     *
     * @param actionApiDTO action API DTO
     * @param decisionUserUUid decision user id
     */
    private void updateActionMode(@Nonnull final ActionApiDTO actionApiDTO,
                                  @Nullable final String decisionUserUUid) {
        if (AuditLogUtils.SYSTEM.equals(decisionUserUUid)) {
            actionApiDTO.setActionMode(ActionMode.AUTOMATIC);
        }
    }

    @Nonnull
    private String createRiskDescription(@Nonnull final ActionSpec actionSpec,
                    @Nonnull final ActionSpecMappingContext context) {
        final Optional<String> policyId = tryExtractPlacementPolicyId(actionSpec.getRecommendation());
        if (policyId.isPresent()) {
            final long entityOid = actionSpec.getRecommendation().getInfo().getMove().getTarget().getId();
            final long policyOid = Long.parseLong(policyId.get());

            try {
                final Optional<PolicyDTO.Policy> policy =
                                Optional.ofNullable(context.getPolicy(policyOid));
                if (!policy.isPresent()) {
                    return actionSpec.getExplanation();
                }
                return String.format("%s doesn't comply to %s",
                                context.getEntity(entityOid).getDisplayName(),
                        policy.get().getPolicyInfo().getName());
            } catch (UnknownObjectException ex) {
                logger.error(String.format("Cannot resolve VM with oid %s from context", entityOid), ex);
            }
        }
        return translateExplanation(actionSpec.getExplanation(), context);
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
        if (!recommendation.getExplanation().hasMove()) {
            return Optional.empty();
        }
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
        if (explanation.getCompliance().getMissingCommodities(0)
                        .getType() != CommodityDTO.CommodityType.SEGMENTATION_VALUE) {
            return Optional.empty();
        }
        return Optional.of(explanation.getCompliance().getMissingCommodities(0).getKey());
    }

    /**
     * Populate various fields of the {@link ActionApiDTO} representing a (compound) move.
     *
     * @param wrapperDto the DTO that represents the move recommendation and
     * wraps other {@link ActionApiDTO}s
     * @param move A Move recommendation with one or more provider changes
     * @param moveExplanation wraps the explanations for the provider changes
     * @param context mapping from {@link ActionSpec} to {@link ActionApiDTO}
     * @param actionType {@link ActionType} that will be assigned to wrapperDto param
     * @throws UnknownObjectException when the actions involve an unrecognized (out of
     * context) oid
     */
    private void addMoveInfo(@Nonnull final ActionApiDTO wrapperDto,
                             @Nonnull final Move move,
                             final MoveExplanation moveExplanation,
                             @Nonnull final ActionSpecMappingContext context,
                             @Nonnull final ActionType actionType)
                    throws UnknownObjectException, ExecutionException, InterruptedException {

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
        final boolean initialPlacement =
            moveExplanation.getChangeProviderExplanationList().stream()
                .anyMatch(ChangeProviderExplanation::hasInitialPlacement);

        wrapperDto.setActionType(actionType);
        // Set entity DTO fields for target, source (if needed) and destination entities
        wrapperDto.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(move.getTarget().getId())));

        ChangeProvider primaryChange = ActionDTOUtil.getPrimaryChangeProvider(move);
        final boolean hasPrimarySource = !initialPlacement && primaryChange.getSource().hasId();
        if (hasPrimarySource) {
            long primarySourceId = primaryChange.getSource().getId();
            wrapperDto.setCurrentValue(Long.toString(primarySourceId));
            wrapperDto.setCurrentEntity(
                ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(primarySourceId)));
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

        List<ActionApiDTO> actions = Lists.newArrayList();
        for (ChangeProvider change : move.getChangesList()) {
            actions.add(singleMove(actionType, wrapperDto, move.getTarget().getId(), change, context));
        }
        wrapperDto.addCompoundActions(actions);
        wrapperDto.setDetails(actionDetails(hasPrimarySource, wrapperDto, primaryChange, context));

        wrapperDto.getRisk().setReasonCommodity(getReasonCommodities(moveExplanation));
    }

    private String getReasonCommodities(MoveExplanation moveExplanation) {
        // Using set to avoid duplicates
        Set<CommodityType> reasonCommodities = new HashSet<>();
        List<ChangeProviderExplanation> changeProviderExplanations = moveExplanation.getChangeProviderExplanationList();
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
                .map(commodityType -> CommodityDTO.CommodityType.forNumber(commodityType.getType()).name())
                .collect(Collectors.joining(", "));
    }

    private ActionApiDTO singleMove(ActionType actionType, ActionApiDTO compoundDto,
                    final long targetId,
                    @Nonnull final ChangeProvider change,
                    @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
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
        actionApiDTO.setDetails(actionDetails(hasSource, actionApiDTO, change, context));
        return actionApiDTO;
    }

    private String actionDetails(boolean hasSource, ActionApiDTO actionApiDTO,
                                 ChangeProvider change, ActionSpecMappingContext context)
            throws ExecutionException, InterruptedException {
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
            String verb = TIER_VALUES.contains(destination.get().getClassName())
                    && TIER_VALUES.contains(source.get().getClassName()) ? "Scale" : "Move";
            String resource = "";
            if (change.hasResource()) {
                Optional<ServiceEntityApiDTO> resourceEntity = context.getOptionalEntity(
                        change.getResource().getId());
                if (resourceEntity.isPresent()) {
                    resource = readableEntityTypeAndName(resourceEntity.get()) + " of ";
                }
            }
            return MessageFormat.format("{0} {1}{2} from {3} to {4}", verb, resource,
                    readableEntityTypeAndName(actionApiDTO.getTarget()),
                                    actionApiDTO.getCurrentEntity().getDisplayName(),
                                    actionApiDTO.getNewEntity().getDisplayName());

        }
    }

    private void addReconfigureInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                    @Nonnull final Reconfigure reconfigure,
                                    @Nonnull final ReconfigureExplanation explanation,
                                    @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        actionApiDTO.setActionType(ActionType.RECONFIGURE);

        actionApiDTO.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(reconfigure.getTarget().getId())));
        if (reconfigure.hasSource()) {
            actionApiDTO.setCurrentEntity(
                ServiceEntityMapper.copyServiceEntityAPIDTO(
                    context.getEntity(reconfigure.getSource().getId())));
        }

        actionApiDTO.setCurrentValue(Long.toString(reconfigure.getSource().getId()));

        if (reconfigure.hasSource()) {
            actionApiDTO.setDetails(MessageFormat.format(
                "Reconfigure {0} which requires {1} but is hosted by {2} which does not provide {1}",
                readableEntityTypeAndName(actionApiDTO.getTarget()),
                readableCommodityTypes(explanation.getReconfigureCommodityList()),
                readableEntityTypeAndName(actionApiDTO.getCurrentEntity())));
        } else {
            actionApiDTO.setDetails(MessageFormat.format(
                "Reconfigure {0} as it is unplaced",
                readableEntityTypeAndName(actionApiDTO.getTarget())));
        }
    }

    private void addProvisionInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                  @Nonnull final Provision provision,
                                  @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        final long currentEntityId = provision.getEntityToClone().getId();
        final String provisionedSellerUuid = Long.toString(provision.getProvisionedSeller());

        actionApiDTO.setActionType(ActionType.PROVISION);

        actionApiDTO.setCurrentValue(Long.toString(currentEntityId));
        actionApiDTO.setCurrentEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(
                context.getEntity(currentEntityId)));

        actionApiDTO.setNewValue(provisionedSellerUuid);

        ServiceEntityApiDTO currentEntity = actionApiDTO.getCurrentEntity();
        actionApiDTO.setNewEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(currentEntity));
        actionApiDTO.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(actionApiDTO.getNewEntity()));

        actionApiDTO.setDetails(MessageFormat.format("Provision {0}",
                readableEntityTypeAndName(currentEntity)));
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

        final CommodityDTO.CommodityType commodityType = CommodityDTO.CommodityType.forNumber(
                resize.getCommodityType().getType());
        Objects.requireNonNull(commodityType, "Commodity for number "
                + resize.getCommodityType().getType());
        actionApiDTO.getRisk().setReasonCommodity(commodityType.name());
        // Check if we need to describe the action as a "remove limit" instead of regular resize.
        if (resize.getCommodityAttribute() == CommodityAttribute.LIMIT) {
            actionApiDTO.setDetails(MessageFormat.format("Remove {0} limit on entity {1}",
                    readableCommodityTypes(Collections.singletonList(resize.getCommodityType())),
                    readableEntityTypeAndName(actionApiDTO.getTarget())));
        } else {
            // Regular case
            actionApiDTO.setDetails(MessageFormat.format("Resize {0} {1} for {2} from {3} to {4}",
                    resize.getNewCapacity() > resize.getOldCapacity() ? UP : DOWN,
                    readableCommodityTypes(Collections.singletonList(resize.getCommodityType())),
                    readableEntityTypeAndName(actionApiDTO.getTarget()),
                    formatResizeActionCommodityValue(commodityType, resize.getOldCapacity()),
                    formatResizeActionCommodityValue(commodityType, resize.getNewCapacity())));
        }
        if (resize.hasCommodityAttribute()) {
            actionApiDTO.setResizeAttribute(resize.getCommodityAttribute().name());
        }
        actionApiDTO.setCurrentValue(Float.toString(resize.getOldCapacity()));
        actionApiDTO.setResizeToValue(Float.toString(resize.getNewCapacity()));
    }

    /**
     * Format resize actions commodity capacity value to more readable format.
     *
     * @param commodityType commodity type.
     * @param capacity commodity capacity which needs to format.
     * @return a string after format.
     */
    private String formatResizeActionCommodityValue(@Nonnull final CommodityDTO.CommodityType commodityType,
                                                    final double capacity) {
        // Currently all items in this map are converted from default units to GB.
        if (commodityTypeToDefaultUnits.keySet().contains(commodityType)) {
            return MessageFormat.format("{0} GB", capacity / (Units.GBYTE / commodityTypeToDefaultUnits.get(commodityType)));
        } else {
            return MessageFormat.format("{0}", capacity);
        }
    }

    private void addActivateInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                 @Nonnull final Activate activate,
                                 @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        actionApiDTO.setActionType(ActionType.START);
        final long targetEntityId = activate.getTarget().getId();
        actionApiDTO.setTarget(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(targetEntityId)));
        actionApiDTO.setCurrentEntity(
            ServiceEntityMapper.copyServiceEntityAPIDTO(context.getEntity(targetEntityId)));

        final List<String> reasonCommodityNames =
            activate.getTriggeringCommoditiesList().stream()
                .map(commodityType -> CommodityDTO.CommodityType
                    .forNumber(commodityType.getType()))
                .map(CommodityDTO.CommodityType::name)
                .collect(Collectors.toList());

        actionApiDTO.getRisk()
            .setReasonCommodity(reasonCommodityNames.stream().collect(Collectors.joining(",")));

        String detailsMessage = MessageFormat.format(
            "Start {0} due to increased demand for resources",
            readableEntityTypeAndName(actionApiDTO.getTarget()));

        actionApiDTO.setDetails(detailsMessage);
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

        actionApiDTO.setActionType(ActionType.SUSPEND);

        final List<String> reasonCommodityNames =
                deactivate.getTriggeringCommoditiesList().stream()
                        .map(commodityType -> CommodityDTO.CommodityType
                                .forNumber(commodityType.getType()))
                        .map(CommodityDTO.CommodityType::name)
                        .collect(Collectors.toList());

        actionApiDTO.getRisk().setReasonCommodity(
            reasonCommodityNames.stream().collect(Collectors.joining(",")));

        String detailsMessage = MessageFormat.format("{0} {1}",
            // this will convert from "SUSPEND" to "Suspend" case format
            CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, ActionType.SUSPEND.name()
                .toLowerCase()),
            readableEntityTypeAndName(actionApiDTO.getTarget()));
        actionApiDTO.setDetails(detailsMessage);
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
        // TODO need to give savings in terms of cost instead of file size for Cloud entities
        String detailsMessage = MessageFormat.format("Delete wasted file ''{0}'' from {1}{2}",
            delete.getFilePath().substring(delete.getFilePath().lastIndexOf('/') + 1),
            readableEntityTypeAndName(actionApiDTO.getTarget()),
            " to free up " + FileUtils
                .byteCountToDisplaySize(deleteExplanation.getSizeKb() * FileUtils.ONE_KB));
        actionApiDTO.setDetails(detailsMessage);
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
        return String.format("%s %s",
            ActionDTOUtil.getSpaceSeparatedWordsFromCamelCaseString(entityDTO.getClassName()),
            entityDTO.getDisplayName()
        );
    }

    /**
     * Convert a list of commodity type numbers to a comma-separated string of readable commodity names.
     *
     * Example: BALLOONING, SWAPPING, CPU_ALLOCATION -> Ballooning, Swapping, Cpu Allocation
     *
     * @param commodityTypes commodity types
     * @return comma-separated string commodity types
     */
    private String readableCommodityTypes(@Nonnull final List<TopologyDTO.CommodityType> commodityTypes) {
        return commodityTypes.stream()
            .map(commodityType -> ActionDTOUtil.getCommodityDisplayName(commodityType))
            .collect(Collectors.joining(", "));
    }

    /**
     * Similar to {@link ActionSpecMapper#createActionFilter(ActionApiInputDTO, Optional)}, but
     * ignores the start/end date in the request - the returned query will only request the current
     * "live" actions.
     *
     * @param inputDto See {@link ActionSpecMapper#createActionFilter(ActionApiInputDTO, Optional)}.
     * @param involvedEntities See {@link ActionSpecMapper#createActionFilter(ActionApiInputDTO, Optional)}.
 * @return The {@link ActionQueryFilter} instance.
     */
    public ActionQueryFilter createLiveActionFilter(@Nullable final ActionApiInputDTO inputDto,
                                                    @Nonnull final Optional<Set<Long>> involvedEntities) {
        return createActionFilterBuilder(inputDto, involvedEntities)
            .clearStartDate()
            .clearEndDate()
            .build();
    }

    /**
     * Creates an {@link ActionQueryFilter} instance based on a given {@link ActionApiInputDTO}
     * and an oid collection of involved entities.
     *
     * @param inputDto The {@link ActionApiInputDTO} instance, where only action states are used.
     * @param involvedEntities The oid collection of involved entities.
     * @return The {@link ActionQueryFilter} instance.
     */
    public ActionQueryFilter createActionFilter(@Nullable final ActionApiInputDTO inputDto,
                                                @Nonnull final Optional<Set<Long>> involvedEntities) {
        return createActionFilterBuilder(inputDto, involvedEntities).build();
    }

    private ActionQueryFilter.Builder createActionFilterBuilder(@Nullable final ActionApiInputDTO inputDto,
                                                                @Nonnull final Optional<Set<Long>> involvedEntities) {
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

            if (CollectionUtils.isNotEmpty(inputDto.getActionTypeList())) {
                inputDto.getActionTypeList().stream()
                    .map(ActionTypeMapper::fromApi)
                    .forEach(queryBuilder::addAllTypes);
            }

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
        } else {
            // When "inputDto" is null, we should automatically insert the operational action states.
            Stream.of(OPERATIONAL_ACTION_STATES).forEach(queryBuilder::addStates);
        }
        involvedEntities.ifPresent(entities -> queryBuilder.setInvolvedEntities(
            ActionQueryFilter.InvolvedEntities.newBuilder()
                .addAllOids(entities)
                .build()));

        return queryBuilder;
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
