package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.common.protobuf.ActionDTOUtil.TRANSLATION_PATTERN;
import static com.vmturbo.common.protobuf.ActionDTOUtil.TRANSLATION_PREFIX;

import java.beans.PropertyDescriptor;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Currency;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ChangeProviderExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityAttribute;
import com.vmturbo.commons.Units;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.sdk.common.CloudCostDTO;
import com.vmturbo.reports.db.StringConstants;

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
    private static final String PHYSICAL_MACHINE_VALUE = UIEntityType.PHYSICAL_MACHINE.getValue();
    private static final String DISK_ARRAY_VALUE = UIEntityType.DISKARRAY.getValue();

    private final PolicyServiceGrpc.PolicyServiceBlockingStub policyService;

    private final RepositoryApi repositoryApi;

    private final ExecutorService executorService;

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

    public ActionSpecMapper(@Nonnull final RepositoryApi repositoryApi,
                    @Nonnull PolicyServiceGrpc.PolicyServiceBlockingStub policyService,
                    @Nonnull ExecutorService executorService) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
        this.policyService = Objects.requireNonNull(policyService);
        this.executorService = Objects.requireNonNull(executorService);
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
                    throws UnsupportedActionException, UnknownObjectException, ExecutionException,
                    InterruptedException {
        final List<ActionDTO.Action> recommendations =
                actionSpecs.stream()
                        .map(ActionSpec::getRecommendation)
                        .collect(Collectors.toList());

        final Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntityIds(recommendations);

        final Future<Map<Long, PolicyDTO.Policy>> policies = executorService.submit(this::getPolicies);
        final Future<Map<Long, Optional<ServiceEntityApiDTO>>> entities = executorService
                        .submit(() -> getEntities(topologyContextId, involvedEntities));
        final ActionSpecMappingContext context = new ActionSpecMappingContext(entities.get(), policies.get());

        final ImmutableList.Builder<ActionApiDTO> actionApiDTOS = ImmutableList.builder();

        for (ActionSpec spec : actionSpecs) {
            try {
                final ActionApiDTO actionApiDTO = mapActionSpecToActionApiDTOInternal(spec, context);
                if (Objects.nonNull(actionApiDTO)) {
                    actionApiDTOS.add(actionApiDTO);
                }
            } catch (UnknownObjectException e) {
                logger.error(String.format("Coulnd't resolve entity from spec %s", spec), e);
            }
        }
        return actionApiDTOS.build();
    }

    @Nonnull
    private Map<Long, PolicyDTO.Policy> getPolicies() {
        final Map<Long, PolicyDTO.Policy> policies = new HashMap<>();
        policyService.getAllPolicies(PolicyDTO.PolicyRequest.newBuilder().build()).forEachRemaining(
                        response -> policies
                                        .put(response.getPolicy().getId(), response.getPolicy()));
        return policies;
    }

    /**
     * We always search the projected topology because the projected topology is
     * a super-set of the source topology. All involved entities that are in
     * the source topology will also be in the projected topology, but there will
     * be entities that are ONLY in the projected topology (e.g. actions involving
     * newly provisioned hosts/VMs).

     * @return maped entities
     */
    private Map<Long, Optional<ServiceEntityApiDTO>> getEntities(long topologyContextId,
                    @Nonnull Set<Long> involvedEntities) {
        return repositoryApi.getServiceEntitiesById(
                        ServiceEntitiesRequest.newBuilder(involvedEntities)
                                        .setTopologyContextId(topologyContextId)
                                        .searchProjectedTopology().build());
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
        final Set<Long> involvedEntities =
                    ActionDTOUtil.getInvolvedEntityIds(actionSpec.getRecommendation());

        final Future<Map<Long, PolicyDTO.Policy>> policies = executorService.submit(this::getPolicies);
        final Future<Map<Long, Optional<ServiceEntityApiDTO>>> entities = executorService
                        .submit(() -> getEntities(topologyContextId, involvedEntities));
        final ActionSpecMappingContext context = new ActionSpecMappingContext(entities.get(), policies.get());
        return mapActionSpecToActionApiDTOInternal(actionSpec, context);
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
     * Map an API category string to an equivalent XL category.
     *
     * @param category The string representing the action category in the UI.
     * @return An optional containing a {@link ActionDTO.ActionCategory}, or an empty optional if
     *         no equivalent category exists in XL.
     */
    @Nonnull
    private Optional<ActionDTO.ActionCategory> mapApiActionCategoryToXl(@Nonnull final String category) {
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
            @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        // Construct a response ActionApiDTO to return
        final ActionApiDTO actionApiDTO = new ActionApiDTO();
        // actionID and uuid are the same
        actionApiDTO.setUuid(Long.toString(actionSpec.getRecommendation().getId()));
        actionApiDTO.setActionID(actionSpec.getRecommendation().getId());
        // actionMode is direct translation
        final ActionDTO.ActionMode actionMode = actionSpec.getActionMode();
        actionApiDTO.setActionMode(ActionMode.valueOf(actionMode.name()));
        // special case translation for the actionState: READY from A-O -> PENDING_ACCEPT for the UX
        final ActionDTO.ActionState actionState = actionSpec.getActionState();
        if (actionState == ActionDTO.ActionState.READY) {
            if (actionMode == ActionDTO.ActionMode.RECOMMEND) {
                actionApiDTO.setActionState(ActionState.RECOMMENDED);
            } else {
                actionApiDTO.setActionState(ActionState.PENDING_ACCEPT);
            }
        } else {
            actionApiDTO.setActionState(ActionState.valueOf(actionState.name()));
        }

        actionApiDTO.setDisplayName(actionMode.name());

        // map the recommendation info
        final ActionDTO.Action recommendation = actionSpec.getRecommendation();
        LogEntryApiDTO risk = new LogEntryApiDTO();
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
        actionApiDTO.setTarget(new ServiceEntityApiDTO());
        actionApiDTO.setCurrentEntity(new ServiceEntityApiDTO());
        actionApiDTO.setNewEntity(new ServiceEntityApiDTO());
        actionApiDTO.setStats(createStats(actionSpec));

        final ActionDTO.ActionInfo info = recommendation.getInfo();
        // handle different action types
        switch (info.getActionTypeCase()) {
            case MOVE:
                addMoveInfo(actionApiDTO, info.getMove(),
                    recommendation.getExplanation().getMove(), context);
                break;
            case RECONFIGURE:
                addReconfigureInfo(actionApiDTO, info.getReconfigure(),
                    recommendation.getExplanation().getReconfigure(), context);
                break;
            case PROVISION:
                addProvisionInfo(actionApiDTO, info.getProvision(), context);
                break;
            case RESIZE:
                addResizeInfo(actionApiDTO, info.getResize(), context);
                break;
            case ACTIVATE:
                addActivateInfo(actionApiDTO, info.getActivate(), context);
                break;
            case DEACTIVATE:
                addDeactivateInfo(actionApiDTO, info.getDeactivate(), context);
                break;
            default: {
                logger.info("Unhandled action, type: {}", info.getActionTypeCase().toString());
                break;
            }
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

        return actionApiDTO;
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
            } catch (ExecutionException | InterruptedException ex) {
                logger.error("Failed to get placement policies", ex);
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
        final ActionDTO.Explanation.ChangeProviderExplanation explanation =
                        recommendation.getExplanation().getMove().getChangeProviderExplanation(0);
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
     * @throws UnknownObjectException when the actions involve an unrecognized (out of
     * context) oid
     */
    private void addMoveInfo(@Nonnull final ActionApiDTO wrapperDto,
                             @Nonnull final Move move,
                             final MoveExplanation moveExplanation,
                             @Nonnull final ActionSpecMappingContext context)
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

        wrapperDto.setActionType(initialPlacement ? ActionType.START : actionType(move, context));
        // Set entity DTO fields for target, source (if needed) and destination entities
        setEntityDtoFields(wrapperDto.getTarget(), move.getTarget().getId(), context);

        ChangeProvider primaryChange = primaryChange(move, context);
        final boolean hasPrimarySource = !initialPlacement && primaryChange.getSource().hasId();
        if (hasPrimarySource) {
            long primarySourceId = primaryChange.getSource().getId();
            wrapperDto.setCurrentValue(Long.toString(primarySourceId));
            setEntityDtoFields(wrapperDto.getCurrentEntity(), primarySourceId, context);
        }
        long primaryDestinationId = primaryChange.getDestination().getId();
        wrapperDto.setNewValue(Long.toString(primaryDestinationId));
        setEntityDtoFields(wrapperDto.getNewEntity(), primaryDestinationId, context);

        List<ActionApiDTO> actions = Lists.newArrayList();
        for (ChangeProvider change : move.getChangesList()) {
            actions.add(singleMove(wrapperDto, move.getTarget().getId(), change, context));
        }
        wrapperDto.addCompoundActions(actions);
        wrapperDto.setDetails(actionDetails(hasPrimarySource, wrapperDto));
    }

    /**
     * Provider change to be used in wrapper action DTO details and in the
     * currentEntity/newEntity.
     *
     * @param move a Move action with one or more provider changes
     * @param context mapping from {@link ActionSpec} to {@link ActionApiDTO}
     * @return a host change if exists, first change otherwise
     */
    private ChangeProvider primaryChange(Move move, ActionSpecMappingContext context)
                    throws ExecutionException, InterruptedException {
        for (ChangeProvider change : move.getChangesList()) {
            if (isHost(context.getOptionalEntity(change.getDestination().getId()))) {
                return change;
            }
        }
        return move.getChanges(0);
    }

    private boolean isHost(Optional<ServiceEntityApiDTO> entity) {
        return entity.map(ServiceEntityApiDTO::getClassName).map(PHYSICAL_MACHINE_VALUE::equals).orElse(false);
    }

    private ActionApiDTO singleMove(ActionApiDTO compoundDto,
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

        ServiceEntityApiDTO destinationEntity = context.getEntity(destinationId);
        boolean isStorage = destinationEntity.getClassName().equals(STORAGE_VALUE);
        actionApiDTO.setActionType(isStorage ? ActionType.START : ActionType.CHANGE);
        // Set entity DTO fields for target, source (if needed) and destination entities
        setEntityDtoFields(actionApiDTO.getTarget(), targetId, context);

        final boolean hasSource = change.getSource().hasId();
        if (hasSource) {
            final long sourceId = change.getSource().getId();
            actionApiDTO.setCurrentValue(Long.toString(sourceId));
            setEntityDtoFields(actionApiDTO.getCurrentEntity(), sourceId, context);
        }
        actionApiDTO.setNewValue(Long.toString(destinationId));
        setEntityDtoFields(actionApiDTO.getNewEntity(), destinationId, context);

        // Set action details
        actionApiDTO.setDetails(actionDetails(hasSource, actionApiDTO));
        return actionApiDTO;
    }

    private String actionDetails(boolean hasSource, ActionApiDTO actionApiDTO) {
        return hasSource ?
            MessageFormat.format("Move {0} from {1} to {2}",
                readableEntityTypeAndName(actionApiDTO.getTarget()),
                readableEntityTypeAndName(actionApiDTO.getCurrentEntity()),
                readableEntityTypeAndName(actionApiDTO.getNewEntity()))
            : MessageFormat.format("Start {0} on {1}",
                readableEntityTypeAndName(actionApiDTO.getTarget()),
                readableEntityTypeAndName(actionApiDTO.getNewEntity()));
    }

    /**
     * If the move contains multiple changes then it is MOVE.
     * If move action doesn't have the source entity/id, it's START except Storage;
     * If one Storage change it is a CHANGE and if one host change then a MOVE.
     *
     * @param move a Move action
     * @param context mapping from {@link ActionSpec} to {@link ActionApiDTO}
     * @return CHANGE or MOVE type.
     */
    private ActionType actionType(@Nonnull final Move move,
                    @Nonnull final ActionSpecMappingContext context)
                    throws ExecutionException, InterruptedException {
        if (move.getChangesCount() > 1) {
            return ActionType.MOVE;
        }

        long destinationId = move.getChanges(0).getDestination().getId();
        boolean isStorage = context.getOptionalEntity(destinationId)
                .map(ServiceEntityApiDTO::getClassName)
                .map(STORAGE_VALUE::equals)
                .orElseGet(() -> false);
        // if move action doesn't have the source entity/id, it's START except Storage
        if (!move.getChanges(0).hasSource()) {
            return isStorage ? ActionType.ADD_PROVIDER : ActionType.START;
        }
        return  isStorage ? ActionType.CHANGE : ActionType.MOVE;
    }

    /**
     * Similar to 6.1, if entity is Disk Array, then it's DELETE, else it's SUSPEND.
     *
     * @param deactivate Deactivate action
     * @param context mapping from {@link ActionSpec} to {@link ActionApiDTO}
     * @return DELETE or SUSPEND type
     */
    private ActionType actionType(@Nonnull final Deactivate deactivate,
                                  @Nonnull final ActionSpecMappingContext context)
                    throws ExecutionException, InterruptedException {
        if (deactivate.hasTarget()) {
            long targetId = deactivate.getTarget().getId();
            return context.getOptionalEntity(targetId)
                    .map(ServiceEntityApiDTO::getClassName)
                    .map(DISK_ARRAY_VALUE::equals)
                    .orElseGet(() -> false)
                    ? ActionType.DELETE
                    : ActionType.SUSPEND;
        }
        return ActionType.SUSPEND;
    }

    /**
     * Similar to 6.1, if entity is STORAGE, then it's ADD_PROVIDER, else it's START.
     *
     * @param activate activate action
     * @param context mapping from {@link ActionSpec} to {@link ActionApiDTO}
     * @return ADD_PROVIDER or START
     */
    private ActionType actionType(@Nonnull final Activate activate,
                                  @Nonnull final ActionSpecMappingContext context)
                    throws ExecutionException, InterruptedException {

        if (activate.hasTarget()) {
            long targetId = activate.getTarget().getId();
            return context.getOptionalEntity(targetId)
                    .map(ServiceEntityApiDTO::getClassName)
                    .map(STORAGE_VALUE::equals)
                    .orElseGet(() -> false)
                    ? ActionType.ADD_PROVIDER
                    : ActionType.START;
        }
        return ActionType.START;
    }


    private void addReconfigureInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                    @Nonnull final Reconfigure reconfigure,
                                    @Nonnull final ReconfigureExplanation explanation,
                                    @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        actionApiDTO.setActionType(ActionType.RECONFIGURE);

        setEntityDtoFields(actionApiDTO.getTarget(), reconfigure.getTarget().getId(), context);
        if (reconfigure.hasSource()) {
            setEntityDtoFields(actionApiDTO.getCurrentEntity(), reconfigure.getSource().getId(), context);
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
                "Reconfigure {0} as it is unplaced.",
                readableEntityTypeAndName(actionApiDTO.getTarget())));
        }
    }

    private void addProvisionInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                  @Nonnull final Provision provision,
                                  @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        actionApiDTO.setActionType(ActionType.PROVISION);

        final String provisionedSellerUuid = Long.toString(provision.getProvisionedSeller());
        setNewEntityDtoFields(actionApiDTO.getTarget(), provision.getEntityToClone().getId(),
                provisionedSellerUuid, context);

        actionApiDTO.setCurrentValue(Long.toString(provision.getEntityToClone().getId()));
        setEntityDtoFields(actionApiDTO.getCurrentEntity(), provision.getEntityToClone().getId(), context);

        actionApiDTO.setNewValue(provisionedSellerUuid);
        setNewEntityDtoFields(actionApiDTO.getNewEntity(), provision.getEntityToClone().getId(),
                provisionedSellerUuid, context);

        actionApiDTO.setDetails(MessageFormat.format("Provision {0}",
                readableEntityTypeAndName(actionApiDTO.getCurrentEntity())));
    }

    private void addResizeInfo(@Nonnull final ActionApiDTO actionApiDTO,
                               @Nonnull final Resize resize,
                               @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        actionApiDTO.setActionType(ActionType.RESIZE);

        long originalEntityOid = resize.getTarget().getId();
        setEntityDtoFields(actionApiDTO.getTarget(), originalEntityOid, context);
        setEntityDtoFields(actionApiDTO.getCurrentEntity(), originalEntityOid, context);
        setEntityDtoFields(actionApiDTO.getNewEntity(), originalEntityOid, context);

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
            actionApiDTO.setDetails(MessageFormat.format("Resize {0} on entity {1} from {2} to {3}",
                    readableCommodityTypes(Collections.singletonList(resize.getCommodityType())),
                    readableEntityTypeAndName(actionApiDTO.getTarget()),
                    formatResizeActionCommodityValue(commodityType, resize.getOldCapacity()),
                    formatResizeActionCommodityValue(commodityType, resize.getNewCapacity())));
        }
        actionApiDTO.setCurrentValue(Float.toString(resize.getOldCapacity()));
        actionApiDTO.setResizeToValue(Float.toString(resize.getNewCapacity()));
    }

    /**
     * Format resize actions commodity capacity value to more readable format. If it is vMem commodity,
     * format it from default KB to GB unit. Otherwise, it will keep its original format.
     *
     * @param commodityType commodity type.
     * @param capacity commodity capacity which needs to format.
     * @return a string after format.
     */
    private String formatResizeActionCommodityValue(@Nonnull final CommodityDTO.CommodityType commodityType,
                                                    final double capacity) {
        if (commodityType.equals(CommodityDTO.CommodityType.VMEM)) {
            // if it is vMem commodity, it needs to convert to GB units. And its default capacity unit is KB.
            return MessageFormat.format("{0} GB", capacity / (Units.GBYTE / Units.KBYTE));
        } else {
            return MessageFormat.format("{0}", capacity);
        }
    }

    private void addActivateInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                 @Nonnull final Activate activate,
                                 @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        ActionType actionType = actionType(activate, context);
        actionApiDTO.setActionType(actionType);
        setEntityDtoFields(actionApiDTO.getTarget(), activate.getTarget().getId(), context);

        final List<String> reasonCommodityNames =
                activate.getTriggeringCommoditiesList().stream()
                        .map(commodityType -> CommodityDTO.CommodityType
                                .forNumber(commodityType.getType()))
                        .map(CommodityDTO.CommodityType::name)
                        .collect(Collectors.toList());

        actionApiDTO.getRisk()
                .setReasonCommodity(reasonCommodityNames.stream().collect(Collectors.joining(",")));

        final StringBuilder detailStrBuilder = new StringBuilder()
                .append(actionType == ActionType.START ? "Start " : "Add provider " )
                .append(readableEntityTypeAndName(actionApiDTO.getTarget()))
                .append(" due to increased demand for resources");
        actionApiDTO.setDetails(detailStrBuilder.toString());
    }

    private void addDeactivateInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                   @Nonnull final Deactivate deactivate,
                                   @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        setEntityDtoFields(actionApiDTO.getTarget(), deactivate.getTarget().getId(), context);
        ActionType actionType = actionType(deactivate, context);
        actionApiDTO.setActionType(actionType);

        final List<String> reasonCommodityNames =
                deactivate.getTriggeringCommoditiesList().stream()
                        .map(commodityType -> CommodityDTO.CommodityType
                                .forNumber(commodityType.getType()))
                        .map(CommodityDTO.CommodityType::name)
                        .collect(Collectors.toList());

        actionApiDTO.getRisk().setReasonCommodity(
            reasonCommodityNames.stream().collect(Collectors.joining(",")));
        String detailStrBuilder = MessageFormat.format(CaseFormat
                        .LOWER_CAMEL
                        .to(CaseFormat.UPPER_CAMEL, actionType.name().toLowerCase()) + " {0}.",
                readableEntityTypeAndName(actionApiDTO.getTarget()));
        actionApiDTO.setDetails(detailStrBuilder);
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
     * Creates an {@link ActionQueryFilter} instance based on a given {@link ActionApiInputDTO}
     * and an oid collection of involved entities.
     *
     * @param inputDto The {@link ActionApiInputDTO} instance, where only action states are used.
     * @param involvedEntities The oid collection of involved entities.
     * @return The {@link ActionQueryFilter} instance.
     */
    public ActionQueryFilter createActionFilter(
                            @Nullable final ActionApiInputDTO inputDto,
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
                        .map(this::stateToEnum)
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
                        .map(this::modeToEnum)
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
                queryBuilder.setStartDate(Long.parseLong(inputDto.getStartTime()));
            }

            if (inputDto.getEndTime() != null && !inputDto.getEndTime().isEmpty()) {
                queryBuilder.setEndDate(Long.parseLong(inputDto.getEndTime()));
            }
        } else {
            // When "inputDto" is null, we should automatically insert the operational action states.
            Stream.of(OPERATIONAL_ACTION_STATES).forEach(queryBuilder::addStates);
        }
        involvedEntities.ifPresent(entities -> queryBuilder.setInvolvedEntities(
            ActionQueryFilter.InvolvedEntities.newBuilder()
                                              .addAllOids(entities)
                                              .build()));

        return queryBuilder.build();
    }

    private Optional<ActionDTO.ActionState> stateToEnum(final ActionState stateStr) {
        switch (stateStr) {
            case PENDING_ACCEPT:
                return Optional.of(ActionDTO.ActionState.READY);
            case ACCEPTED: case QUEUED:
                return Optional.of(ActionDTO.ActionState.QUEUED);
            case SUCCEEDED:
                return Optional.of(ActionDTO.ActionState.SUCCEEDED);
            case IN_PROGRESS:
                return Optional.of(ActionDTO.ActionState.IN_PROGRESS);
            case FAILED:
                return Optional.of(ActionDTO.ActionState.FAILED);
            // These don't map to ActionStates directly, because in XL we separate the concept
            // of a "decision" from the state of the action, and these relate to the decision.
            case REJECTED: case RECOMMENDED: case DISABLED: case CLEARED:
                logger.warn("Not able to convert {} to ActionState.", stateStr);
                return Optional.empty();
            default:
                logger.error("Unknown action state {}", stateStr);
                return Optional.empty();
        }
    }

    /**
     * Map UI's ActionMode to ActionDTO.ActionMode
     *
     * @param actionMode UI's ActionMode
     * @return ActionDTO.ActionMode
     */
    private Optional<ActionDTO.ActionMode> modeToEnum(final ActionMode actionMode) {
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


    /**
     * Populate the necessary fields in the response {@link ServiceEntityApiDTO} for a newly
     * created entity from the related entity OID as listed in the {link ActionSpecMappingContext}.
     *
     * TODO: this method will need to accommodate both OID and uuid in the return object
     * when we implement the legacy UUID in TopologyDTO.proto - see OM-14309.
     *
     * @param responseEntityApiDTO the response {@link ServiceEntityApiDTO} to populate as "new"
     * @param originalEntityOid the OID of the original {@link ServiceEntityApiDTO}  to copy from
     * @param newEntityOid the OID of the newly provisioned entity
     * @param context the {@link ActionSpecMappingContext} in which to look up the original
     *                entity OID
     * @throws UnknownObjectException if the originalEntityOid is not found in the context
     */
    private void setNewEntityDtoFields(@Nonnull final BaseApiDTO responseEntityApiDTO,
                                       final long originalEntityOid,
                                       @Nonnull String newEntityOid,
                                       @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        final ServiceEntityApiDTO originalEntity = context.getEntity(originalEntityOid);
        responseEntityApiDTO.setDisplayName("New Entity");
        responseEntityApiDTO.setUuid(newEntityOid);
        responseEntityApiDTO.setClassName(originalEntity.getClassName());
    }

    /**
     * Populate the necessary fields in the response {@link ServiceEntityApiDTO} from the
     * related entity OID as listed in the {@link ActionSpecMappingContext}.
     *
     * TODO: this method will need to accommodate both OID and uuid in the return object
     * when we implement the legacy UUID in TopologyDTO.proto - see OM-14309.
     *
     * @param responseEntityApiDTO the response {@link ServiceEntityApiDTO} to populate
     * @param originalEntityOid the OID of the original {@link ServiceEntityApiDTO}  to copy from
     * @param context the {@link ActionSpecMappingContext} in which to look up the original
     *                entity OID
     * @throws UnknownObjectException if the originalEntityOid is not found in the context
     */
    private void setEntityDtoFields(@Nonnull final BaseApiDTO responseEntityApiDTO,
                                    final long originalEntityOid,
                                    @Nonnull final ActionSpecMappingContext context)
                    throws UnknownObjectException, ExecutionException, InterruptedException {
        final ServiceEntityApiDTO originalEntity = context.getEntity(originalEntityOid);
        responseEntityApiDTO.setDisplayName(originalEntity.getDisplayName());
        responseEntityApiDTO.setUuid(originalEntity.getUuid());
        responseEntityApiDTO.setClassName(originalEntity.getClassName());
    }

    /**
     * The context of a mapping operation from {@link ActionSpec} to a {@link ActionApiDTO}.
     *
     * <p>Caches information stored from calls to other components to allow a single set of
     * remote calls to obtain all the information required to map a set of {@link ActionSpec}s.</p>
     */
    @VisibleForTesting
    public static class ActionSpecMappingContext {

        private final Logger logger = LogManager.getLogger();

        private final Map<Long, Optional<ServiceEntityApiDTO>> entities;

        private final Map<Long, PolicyDTO.Policy> policies;

        ActionSpecMappingContext(@Nonnull Map<Long, Optional<ServiceEntityApiDTO>> entities,
                        @Nonnull Map<Long, PolicyDTO.Policy> policies) {
            this.entities = Objects.requireNonNull(entities);
            this.policies = Objects.requireNonNull(policies);
        }

        private PolicyDTO.Policy getPolicy(long id)
                        throws ExecutionException, InterruptedException {
            return policies.get(id);
        }

        @Nonnull
        private ServiceEntityApiDTO getEntity(final long oid)
                        throws UnknownObjectException, ExecutionException, InterruptedException {
            return entities.getOrDefault(oid, Optional.empty())
                          .orElseThrow(() -> new UnknownObjectException("Entity: " + oid
                                  + " not found."));
        }

        private Optional<ServiceEntityApiDTO> getOptionalEntity(final long oid)
                        throws ExecutionException, InterruptedException {
            return entities.get(oid);
        }
    }
}
