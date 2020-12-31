package com.vmturbo.topology.processor.actions.data.context;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.RiskUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowParameter;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.WorkflowProperty;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.Builder;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.Risk;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.topology.processor.actions.data.EntityRetrievalException;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.PolicyRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.operation.ActionConversions;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * A super-class for action execution context implementations.
 * Contains logic common to all action execution contexts.
 */
public abstract class AbstractActionExecutionContext implements ActionExecutionContext {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Token to use generating error logging during entity lookup. This corresponds to the
     * main entity in any action, the entity being acted upon.
     */
    private static final String TARGET_LOOKUP_TOKEN = "primary";

    /**
     * The id of this action, as sent from Action Orchestrator.
     */
    private final long actionId;

    /**
     * The id of the target on which to execute this action, as sent from Action Orchestrator.
     */
    private final long targetId;

    /**
     * Workflow, if any.
     * Workflows allow actions to be executed through a third party action orchestrator
     */
    private final Workflow workflow;

    /**
     * Provides additional data for handling action execution special cases (i.e. complex actions)
     */
    private final ActionDataManager dataManager;

    /**
     * Cache a reference to the entityStore to retrieve target info about entities involved in the
     * action.
     */
    private final EntityStore entityStore;

    /**
     * Used for determining the target type of a given target.
     */
    private final TargetStore targetStore;

    /**
     * Used for determining the action policy of a given target.
     */
    private final ProbeStore probeStore;

    /**
     * Retrieves and converts an entity in order to provide the full entity data for action execution.
     */
    protected final EntityRetriever entityRetriever;

    /**
     * Retrieves policies required for action execution.
     */
    protected final PolicyRetriever policyRetriever;

    /**
     * A list of {@link ActionItemDTO} to send to the probe for action execution.
     * This is the main carrier of data to the probes when executing an action.
     * By convention, the first ActionItem in the list will declare the overarching type of the
     *   action being executed, as well as include any additional ContextData needed to execute
     *   the action.
     */
    protected List<ActionItemDTO> actionItems;

    protected final ActionDTO.ActionSpec actionSpec;

    /**
     * The SDK (probe-facing) type that will be sent to the probes.
     */
    protected ActionItemDTO.ActionType sdkActionType;

    protected ActionDTO.ActionType actionType;

    @Nullable
    protected final String explanation;

    protected AbstractActionExecutionContext(@Nonnull final ExecuteActionRequest request,
                                             @Nonnull final ActionDataManager dataManager,
                                             @Nonnull final EntityStore entityStore,
                                             @Nonnull final EntityRetriever entityRetriever,
                                             @Nonnull final TargetStore targetStore,
                                             @Nonnull final ProbeStore probeStore,
                                             @Nonnull final PolicyRetriever policyRetriever) {
        Objects.requireNonNull(request);
        this.actionId = request.getActionId();
        this.targetId = request.getTargetId();
        this.workflow = request.hasWorkflowInfo() ? buildWorkflow(request.getWorkflowInfo()) : null;
        this.dataManager = Objects.requireNonNull(dataManager);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.entityRetriever = Objects.requireNonNull(entityRetriever);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.actionType = Objects.requireNonNull(request.getActionType());
        this.explanation = request.getExplanation();
        this.actionSpec = request.getActionSpec();
        this.policyRetriever = Objects.requireNonNull(policyRetriever);
    }

    @Nonnull
    @Override
    public final ActionItemDTO.ActionType getSDKActionType() throws ContextCreationException {
        if (sdkActionType == null) {
            sdkActionType = doesProbeSupportV2ActionType()
                ? getApiActionType() : getLegacySDKActionType();
        }
        return sdkActionType;
    }

    /**
     * Determines if action type should be populate V2 action type which is consistent with API
     * action types
     * of not.
     *
     * @return true if the probe executing action supports API consistent action types.
     * @throws ContextCreationException if we cannot lookup target or probe for the action.
     */
    private boolean doesProbeSupportV2ActionType() throws ContextCreationException {
        return getProbeInfo().map(MediationMessage.ProbeInfo::getSupportsV2ActionTypes)
            .orElseThrow(() -> new ContextCreationException("Cannot find the target or probe "
                + "associated with the action with id \"" + actionId + "\" and target id \""
                + targetId + "\"."));
    }

    /**
     * Gets the SDK (probe-facing) type of the over-arching action being executed. This returns the
     * action type which is supported by the probes in legacy way. Already implemented probes do
     * expect action types that are not consistent with what is seen in API. These probes will
     * be updated over time to expect action types that are consistent with API.
     *
     * @return the sdk action type.
     * @throws ContextCreationException the SDK type cannot be determined.
     */
    @Nonnull
    protected ActionItemDTO.ActionType getLegacySDKActionType() throws ContextCreationException {
        ActionType sdkActionType = calculateSDKActionType(actionType);
        if (sdkActionType == null) {
            throw new ContextCreationException("The SDK type for action with Id \""
                + actionId + "\" with internal type \"" + actionType
                + "\" cannot be calculated using legacy logic.");
        }

        return sdkActionType;
    }

    /**
     * Gets the probe-facing type of the over-arching action being executed.
     * {@link ActionItemDTO.ActionType} is what is used by the probes to identify the type of an
     * action.
     *
     * <p>This type should be consistent with the type seen in the API. This is very critical as
     * customers start to write their own probes (for example to handle action execution), the
     * expect to see the same action type seen in the API in the probe. We are in the process of
     * updating all probe to support API consistent action type.
     * </p>
     *
     * @return the sdk action type.
     * @throws ContextCreationException the SDK type cannot be determined.
     */
    @Nonnull
    protected ActionItemDTO.ActionType getApiActionType() throws ContextCreationException {
        ActionType sdkActionType = ActionConversions.convertToSdkV2ActionType(actionType);
        if (sdkActionType == null) {
            throw new ContextCreationException("The SDK type for action with Id \""
                + actionId + "\" with internal type \"" + actionType
                + "\" cannot be calculated using api consistent logic.");
        }

        return sdkActionType;
    }

    /**
     * Calculates the {@link ActionItemDTO.ActionType} that will be sent to probes from the one
     * received from Action Orchestrator {@link ActionDTO.ActionType}.
     *
     * @param actionType The {@link ActionDTO.ActionType} received from AO
     * @return The {@link ActionItemDTO.ActionType} that will be sent to the probes
     */
    protected ActionItemDTO.ActionType calculateSDKActionType(@Nonnull final ActionDTO.ActionType actionType) {
        return ActionConversions.convertActionType(actionType);
    }

    private void buildActionItems() throws ContextCreationException {
        // Build the action items, the primary data carrier to the probe for action execution
        List<ActionItemDTO.Builder> actionItemBuilders = initActionItemBuilders();
        actionItems = actionItemBuilders.stream()
                .map(Builder::build)
                .collect(Collectors.toList());
    }

    /**
     * The id of the overarching action. This is the ID that gets assigned by the Action Orchestrator.
     *
     * @return the id of the overarching action
     */
    @Override
    public long getActionId() {
        return actionId;
    }

    /**
     * The id of the target containing the entities for the action, which will be used to execute
     * the action.
     *
     * @return the id of the target containing the entities for the action.
     */
    @Override
    public long getTargetId() {
        return targetId;
    }

    @Nonnull
    @Override
    public Optional<Workflow> getWorkflow() {
        return Optional.ofNullable(workflow);
    }

    /**
     * Get all of the action item DTOs associated with executing this action.
     *
     * @return all of the action item DTOs associated with executing this action
     * @throws ContextCreationException if failure occurred while constructing context
     */
    @Override
    public List<ActionItemDTO> getActionItems() throws ContextCreationException {
        if (actionItems == null) {
            buildActionItems();
        }
        return actionItems;
    }

    /**
     * Return a Set of entities to that are directly involved in the action.
     *
     * <p>This default implementation assumes that only the primary entity (the entity being acted upon)
     * is affected by the action.</p>
     *
     * @return a Set of entities involved in the action
     */
    @Override
    public Set<Long> getControlAffectedEntities() {
        return Collections.singleton(getPrimaryEntityId());
    }

    /**
     * Get the secondary target involved in this action, or null if no secondary target is involved.
     *
     * @return the secondary target involved in this action, or null if no secondary target is
     * involved
     */
    @Nullable
    @Override
    public Long getSecondaryTargetId() throws ContextCreationException {
        return null;
    }

    /**
     * By convention, the first ActionItem in the list will declare the overarching type of the
     * action being executed, as well as include any additional ContextData needed to execute
     * the action.
     *
     * <p>TODO: Improve this logic. The reason this is here is because it's a convention that was
     * established in OpsManager, and now several (many?) probes rely on the order of the action
     * items to determine their meaning. Hopefully someday we can remove the importance of ordering
     * action items, and remove this logic that treats the first item in the list as special.</p>
     *
     * @param actionItemBuilders the full list of action items, from which the primary one will be extracted
     * @return the primary {@link ActionItemDTO.Builder} for this action
     * @throws ContextCreationException if failure occurred while constructing context
     */
    protected ActionItemDTO.Builder getPrimaryActionItemBuilder(
            @Nonnull List<ActionItemDTO.Builder> actionItemBuilders)
            throws ContextCreationException {
        return actionItemBuilders.stream()
                .findFirst()
                .orElseThrow(() -> new ContextCreationException("No action item builders found. "
                        + "An action should have at least one action item."));
    }

    /**
     * Get the primary entity ID for this action
     * Corresponds to the logic in
     *   {@link ActionDTOUtil#getPrimaryEntityId(Action) ActionDTOUtil.getPrimaryEntityId}.
     * In comparison to that utility method, because we know the type here we avoid the switch
     * statement and the corresponding possiblity of an {@link UnsupportedActionException} being
     * thrown.
     *
     * @return the ID of the primary entity for this action (the entity being acted upon)
     */
    protected abstract long getPrimaryEntityId();

    protected ActionInfo getActionInfo() {
        return actionSpec.getRecommendation().getInfo();
    }

    protected EntityStore getEntityStore() {
        return entityStore;
    }

    /**
     * Create builders for the actionItemDTOs needed to execute this action and populate them with
     *   data. A builder is returned so that further modifications can be made by subclasses, before
     *   the final build is done.
     *
     * <p>The default implementation creates a single {@link ActionItemDTO.Builder}</p>
     *
     * @return a list of {@link ActionItemDTO.Builder ActionItemDTO builders}
     * @throws ContextCreationException if failure occurred while constructing context
     */
    protected List<ActionItemDTO.Builder> initActionItemBuilders() throws ContextCreationException {
        // Get the full entity, including a combination of both stitched and raw data
        final EntityDTO fullEntityDTO = getFullEntityDTO(getPrimaryEntityId());
        return buildPrimaryActionItem(fullEntityDTO);
    }

    /**
     * Build the primary actionItem for this action.
     * This is separated out from the call to retrieve the fullEntityDTO so that subclasses
     * don't need to repeat the remote call.
     *
     * @param fullEntityDTO a {@link EntityDTO} representing the primary entity for this action
     * @return a list of {@link ActionItemDTO.Builder ActionItemDTO builders} containing a single
     * item representing the primary actionItem for this action
     * @throws ContextCreationException if failure occurred while constructing context
     */
    @Nonnull
    protected List<ActionItemDTO.Builder> buildPrimaryActionItem(final EntityDTO fullEntityDTO)
            throws ContextCreationException {
        final ActionItemDTO.Builder actionItemBuilder = ActionItemDTO.newBuilder();
        actionItemBuilder.setActionType(getSDKActionType());
        actionItemBuilder.setUuid(Long.toString(getActionId()));
        actionItemBuilder.setTargetSE(fullEntityDTO);

        // Add additional data for action execution
        actionItemBuilder.addAllContextData(getContextData());

        // TODO: Determine if this special case should be converted to context data
        // Right now, this is treated as a fourth entity (hostedBySE) on the ActionItemDTO
        getHost(fullEntityDTO).ifPresent(actionItemBuilder::setHostedBySE);

        // Using an ArrayList in order to ensure the returned list is mutable so that subclasses
        // can add additional items
        List<ActionItemDTO.Builder> builders = new ArrayList<>();
        builders.add(actionItemBuilder);

        // populate description, risk, execution characteristics
        populatedPrimaryActionAdditionalFields(builders);
        return builders;
    }

    /**
     * Populates some fields that are specific to primary action.
     *
     * @param builders the list of builders.
     * @throws ContextCreationException if there is no action builder.
     */
    protected void populatedPrimaryActionAdditionalFields(List<ActionItemDTO.Builder> builders)
          throws ContextCreationException {
        ActionItemDTO.Builder primaryAction = getPrimaryActionItemBuilder(builders);
        // set the risk
        primaryAction.setRisk(getRisk());
        // set the description
        primaryAction.setDescription(getActionDescription());
        // set execution characteristics
        getExecutionCharacteristics().ifPresent(primaryAction::setCharacteristics);
    }

    protected EntityDTO getFullEntityDTO(long entityId) throws ContextCreationException {
        try {
            return entityRetriever.fetchAndConvertToEntityDTO(entityId);
        } catch (EntityRetrievalException e) {
            throw new ContextCreationException("Unable to execute action because the full entity"
                    + "data for entity " + entityId + " could not be retrieved.", e);
        }
    }

    private PerTargetInfo getPerTargetInfo(final long targetId, final long entityId,
            final String entityType) throws ContextCreationException {
        return entityStore.getEntity(entityId)
                .orElseThrow(() -> ContextCreationException.noEntity(entityType, entityId))
                .getEntityInfo(targetId)
                .orElseThrow(() -> ContextCreationException.noEntityTargetInfo(
                        entityType, entityId, targetId));
    }

    @Nonnull
    protected Optional<EntityDTO> getHost(final EntityDTO entity) throws ContextCreationException {
        // Right now hosted by only gets set for VM -> PM and Container -> Pod relationships.
        // Hosted by for VM -> PM relationships is most notably required by the HyperV probe.
        // TODO (roman, May 16 2017): Generalize to all entities where this is necessary.
        final EntityType entityType = entity.getEntityType();
        if (entityType.equals(EntityType.VIRTUAL_MACHINE)
                || entityType.equals(EntityType.CONTAINER)) {
            // Look up the raw entity info to find the host
            // TODO: This is bad. Why store host separately from the rest of the entity data? This
            //     should be stored somewhere in the EntityDTO, not in the PerTargetInfo itself!
            //     If that were the case, this getPerTargetInfo lookup would not be needed.
            // Get the raw discovery data discovered for this entity by this particular target
            final PerTargetInfo entityInfo = getPerTargetInfo(getTargetId(),
                    getPrimaryEntityId(),
                    TARGET_LOOKUP_TOKEN);
            // Look up the raw entity info for the host
            final long host = entityInfo.getHost();
            if (host != 0) {
                return Optional.of(getFullEntityDTO(host));
            }
        }
        return Optional.empty();
    }

    /**
     * Add any additional ContextData needed to execute the action.
     *
     * @return a set of context data containing data related to action execution
     */
    protected List<ContextData> getContextData() {
        return dataManager.getContextData(actionSpec.getRecommendation().getInfo());
    }

    @Override
    @Nonnull
    public ActionExecutionDTO buildActionExecutionDto() throws ContextCreationException {
        final ActionExecutionDTO.Builder actionExecutionBuilder = ActionExecutionDTO.newBuilder()
                .setActionOid(getActionId())
                .setActionType(getSDKActionType())
                .setActionState(getActionState())
                .addAllActionItem(getActionItems());

        if (actionSpec.hasRecommendationTime()) {
            actionExecutionBuilder.setCreateTime(actionSpec.getRecommendationTime());
        }

        if (actionSpec.hasDecision()
            && actionSpec.getDecision().hasDecisionTime()) {
            actionExecutionBuilder.setUpdateTime(actionSpec.getDecision().getDecisionTime());
        }

        // populate the user uuid for the action that is manually accepted
        if (actionSpec.hasDecision()
            && actionSpec.getDecision().hasExecutionDecision()
            && actionSpec.getDecision().getExecutionDecision().hasUserUuid()) {
            actionExecutionBuilder.setAcceptedBy(
                actionSpec.getDecision().getExecutionDecision().getUserUuid());
        } else if (actionSpec.hasActionSchedule()
            && actionSpec.getActionSchedule().hasAcceptingUser()) {
            // set the accepting user from schedule
            actionExecutionBuilder.setAcceptedBy(actionSpec.getActionSchedule().getAcceptingUser());
        }

        if (explanation != null) {
            actionExecutionBuilder.setExplanation(explanation);
        }

        // if a WorkflowInfo action execution override is present, translate it to a NonMarketEntity
        // and include it in the ActionExecution to be sent to the target
        getWorkflow().ifPresent(actionExecutionBuilder::setWorkflow);
        return actionExecutionBuilder.build();
    }

    /**
     * Gets the state of the action.
     *
     * @return the state of action.
     */
    public ActionResponseState getActionState() {
        switch (actionSpec.getActionState()) {
            case READY:
                return ActionResponseState.PENDING_ACCEPT;
            case QUEUED:
                return ActionResponseState.QUEUED;
            case IN_PROGRESS:
            case PRE_IN_PROGRESS:
            case POST_IN_PROGRESS:
                return ActionResponseState.IN_PROGRESS;
            case FAILING:
                return ActionResponseState.FAILING;
            case ACCEPTED:
                return ActionResponseState.ACCEPTED;
            case CLEARED:
                return ActionResponseState.CLEARED;
            case REJECTED:
                return ActionResponseState.REJECTED;
            case SUCCEEDED:
                return ActionResponseState.SUCCEEDED;
            case FAILED:
                return ActionResponseState.FAILED;
            default:
                throw new IllegalStateException("ActionState from action orchestrator "
                    + actionSpec.getActionState() + " is not supported yet");
        }
    }

    /**
     * Get the description of action.
     *
     * @return the human readable action.
     */
    public String getActionDescription() {
        return actionSpec.getDescription();
    }

    /**
     * Gets the risk for primary action.
     *
     * @return the risk for the main action.
     */
    public Risk getRisk() {
        Risk.Builder risk = Risk.newBuilder();
        risk.setDescription(getActionRiskDescription());
        Risk.Category category = convertCategory(actionSpec.getCategory());
        if (category != null) {
            risk.setCategory(category);
        }

        // add the affected commodities
        getRiskCommodities().stream()
            .map(c -> CommonDTO.CommodityDTO.CommodityType.forNumber(c.getType()))
            .forEach(risk::addAffectedCommodity);

        risk.setSeverity(convertSeverity(actionSpec.getSeverity()));
        return risk.build();
    }

    /**
     * Gets the risk commodity for the action or empty list if there is no risk.
     *
     * @return the list of commodities at risk.
     */
    @Nonnull
    public List<TopologyDTO.CommodityType> getRiskCommodities() {
        // many of actions don't have a risk so we return no risk by default.
        return Collections.emptyList();
    }

    /**
     * Gets the execution characteristics.
     *
     * @return the execution characteristics.
     */
    public Optional<ActionItemDTO.ExecutionCharacteristics> getExecutionCharacteristics() {
        if (actionSpec.getRecommendation().hasDisruptive()
                || actionSpec.getRecommendation().hasReversible()) {
            ActionItemDTO.ExecutionCharacteristics.Builder characteristics =
                ActionItemDTO.ExecutionCharacteristics.newBuilder();
            if (actionSpec.getRecommendation().hasDisruptive()) {
                characteristics.setDisruptive(actionSpec.getRecommendation().getDisruptive());
            }
            if (actionSpec.getRecommendation().hasReversible()) {
                characteristics.setReversible(actionSpec.getRecommendation().getReversible());
            }
            return Optional.of(characteristics.build());
        }
        return Optional.empty();
    }

    /**
     * Converts action category to risk category.
     *
     * @param category the category.
     * @return the converted category.
     */
    @Nullable
    protected Risk.Category convertCategory(ActionDTO.ActionCategory category) {
        switch (category) {
            case PERFORMANCE_ASSURANCE:
                return Risk.Category.PERFORMANCE_ASSURANCE;
            case EFFICIENCY_IMPROVEMENT:
                return Risk.Category.EFFICIENCY_IMPROVEMENT;
            case PREVENTION:
                return Risk.Category.PREVENTION;
            case COMPLIANCE:
                return Risk.Category.COMPLIANCE;
            default:
                return null;
        }
    }

    /**
     * Converts action severity to sdk action severity.
     *
     * @param severity the action severity.
     * @return the converted severity.
     */
    @Nullable
    protected Risk.Severity convertSeverity(ActionDTO.Severity severity) {
        switch (severity) {
            case NORMAL:
                return Risk.Severity.NORMAL;
            case MINOR:
                return Risk.Severity.MINOR;
            case MAJOR:
                return Risk.Severity.MAJOR;
            case CRITICAL:
                return Risk.Severity.CRITICAL;
            default:
                return Risk.Severity.UNKNOWN;
        }
    }

    @Nullable
    protected String getActionRiskDescription() {
        try {
            return RiskUtil.createRiskDescription(actionSpec,
                policyRetriever::retrievePolicy,
                oid -> entityRetriever.retrieveTopologyEntity(oid)
                    .map(TopologyDTO.TopologyEntityDTO::getDisplayName)
                    .orElse(null));
        } catch (UnsupportedActionException e) {
            logger.error("Cannot calculate the risk for action with oid {} and type {} as it is "
                + "unsupported",
                actionSpec.getRecommendationId(),
                actionSpec.getRecommendation().getInfo().getActionTypeCase());
            return null;
        }
    }

    /**
     * Create a Workflow DTO representing the given {@link WorkflowDTO.WorkflowInfo}.
     *
     * @param workflowInfo the information describing this Workflow, including ID, displayName,
     *                     and defining data - parameters and properties.
     * @return a newly created {@link Workflow} DTO
     */
    private static Workflow buildWorkflow(WorkflowDTO.WorkflowInfo workflowInfo) {
        final Workflow.Builder wfBuilder = Workflow.newBuilder();
        if (workflowInfo.hasDisplayName()) {
            wfBuilder.setDisplayName(workflowInfo.getDisplayName());
        }
        if (workflowInfo.hasName()) {
            wfBuilder.setId(workflowInfo.getName());
        }
        if (workflowInfo.hasDescription()) {
            wfBuilder.setDescription(workflowInfo.getDescription());
        }
        wfBuilder.addAllParam(workflowInfo.getWorkflowParamList().stream()
                .map(AbstractActionExecutionContext::buildWorkflowParameter)
                .collect(Collectors.toList()));
        // include the 'property' entries from the Workflow
        wfBuilder.addAllProperty(workflowInfo.getWorkflowPropertyList().stream()
                .map(AbstractActionExecutionContext::buildWorkflowProperty)
                .collect(Collectors.toList()));
        if (workflowInfo.hasScriptPath()) {
            wfBuilder.setScriptPath(workflowInfo.getScriptPath());
        }
        if (workflowInfo.hasEntityType()) {
            wfBuilder.setEntityType(EntityDTO.EntityType.forNumber(workflowInfo.getEntityType()));
        }
        if (workflowInfo.hasActionType()) {
            ActionType converted = ActionConversions.convertActionType(workflowInfo.getActionType());
            if (converted != null) {
                wfBuilder.setActionType(converted);
            }
        }
        if (workflowInfo.hasActionPhase()) {
            final ActionScriptPhase converted = ActionConversions.convertActionPhase(workflowInfo.getActionPhase());
            if (converted != null) {
                wfBuilder.setPhase(converted);
            }
        }
        if (workflowInfo.hasTimeLimitSeconds()) {
            wfBuilder.setTimeLimitSeconds(workflowInfo.getTimeLimitSeconds());
        }
        return wfBuilder.build();
    }

    @Nonnull
    private static Workflow.Parameter buildWorkflowParameter(
            @Nonnull WorkflowParameter workflowParam) {
        final Workflow.Parameter.Builder parmBuilder = Workflow.Parameter.newBuilder();
        if (workflowParam.hasDescription()) {
            parmBuilder.setDescription(workflowParam.getDescription());
        }
        if (workflowParam.hasName()) {
            parmBuilder.setName(workflowParam.getName());
        }
        if (workflowParam.hasType()) {
            parmBuilder.setType(workflowParam.getType());
        }
        if (workflowParam.hasMandatory()) {
            parmBuilder.setMandatory(workflowParam.getMandatory());
        }
        return parmBuilder.build();
    }

    @Nonnull
    private static Workflow.Property buildWorkflowProperty(
            @Nonnull WorkflowProperty workflowProperty) {
        final Workflow.Property.Builder propBUilder = Workflow.Property.newBuilder();
        if (workflowProperty.hasName()) {
            propBUilder.setName(workflowProperty.getName());
        }
        if (workflowProperty.hasValue()) {
            propBUilder.setValue(workflowProperty.getValue());
        }
        return propBUilder.build();
    }

    /**
     * Returns the target store.
     *
     * @return the target store.
     */
    @Nonnull
    TargetStore getTargetStore() {
        return targetStore;
    }

    /**
     * Gets the information about probe associated with this action.
     *
     * @return the information about probe info.
     */
    @Nonnull
    Optional<MediationMessage.ProbeInfo> getProbeInfo() {
        Optional<Target> target = targetStore.getTarget(targetId);
        if (!target.isPresent()) {
            logger.error("Couldn't find the target with ID {} for action \"{}\" ", getTargetId(),
                actionSpec.getRecommendation().getInfo());
            return Optional.empty();
        }

        final long probeId = target.get().getProbeId();
        return probeStore.getProbe(probeId);
    }
}
