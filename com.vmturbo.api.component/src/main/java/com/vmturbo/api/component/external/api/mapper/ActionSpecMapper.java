package com.vmturbo.api.component.external.api.mapper;

import java.text.MessageFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.ServiceEntitiesRequest;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.action.ActionApiInputDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.enums.ActionMode;
import com.vmturbo.api.enums.ActionState;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Activate;
import com.vmturbo.common.protobuf.action.ActionDTO.Deactivate;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.MoveExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ReconfigureExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.action.ActionDTO.Provision;
import com.vmturbo.common.protobuf.action.ActionDTO.Reconfigure;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Map an ActionSpec returned from the ActionOrchestrator into an {@link ActionApiDTO} to be
 * returned from the API.
 */
public class ActionSpecMapper {

    private final RepositoryApi repositoryApi;

    private final Logger logger = LogManager.getLogger();

    public ActionSpecMapper(@Nonnull final RepositoryApi repositoryApi) {
        this.repositoryApi = Objects.requireNonNull(repositoryApi);
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
            final long topologyContextId) throws UnsupportedActionException {
        final List<ActionDTO.Action> recommendations =
                actionSpecs.stream()
                        .map(ActionSpec::getRecommendation)
                        .collect(Collectors.toList());

        final Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntities(recommendations);

        final ActionSpecMappingContext context = new ActionSpecMappingContext(topologyContextId,
                involvedEntities, repositoryApi);

        final ImmutableList.Builder<ActionApiDTO> retBuilder = new ImmutableList.Builder<>();
        for (final ActionSpec spec : actionSpecs) {
            try {
                retBuilder.add(mapActionSpecToActionApiDTOInternal(spec, context));
            } catch (UnknownObjectException e) {
                logger.error("Unable to map action spec: " + e);
            }
        }
        return retBuilder.build();
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
            throws UnknownObjectException, UnsupportedActionException {
        final Set<Long> involvedEntities = ActionDTOUtil.getInvolvedEntities(actionSpec
                .getRecommendation());

        final ActionSpecMappingContext context = new ActionSpecMappingContext(topologyContextId,
                involvedEntities, repositoryApi);
        return mapActionSpecToActionApiDTOInternal(actionSpec, context);
    }

    @Nonnull
    private ActionApiDTO mapActionSpecToActionApiDTOInternal(
            @Nonnull final ActionSpec actionSpec,
            @Nonnull final ActionSpecMappingContext context) throws UnknownObjectException {
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
        risk.setDescription(actionSpec.getExplanation());
        risk.setSubCategory(actionSpec.getCategory());
        risk.setSeverity(
            ActionDTOUtil.getSeverityName(ActionDTOUtil.mapImportanceToSeverity(recommendation
                    .getImportance())));
        risk.setReasonCommodity("");
        actionApiDTO.setRisk(risk);

        // The target definition
        actionApiDTO.setTarget(new ServiceEntityApiDTO());
        actionApiDTO.setCurrentEntity(new ServiceEntityApiDTO());
        actionApiDTO.setNewEntity(new ServiceEntityApiDTO());

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
        actionApiDTO.setCreateTime(LocalDateTime.ofInstant(
            Instant.ofEpochMilli(actionSpec.getRecommendationTime()), ZoneOffset.systemDefault())
                .toString());

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
                final long decisionUserId = executionDecision.getUserId();
                actionApiDTO.setUserName(Long.toString(decisionUserId));
            }
        }

        return actionApiDTO;
    }

    private void addMoveInfo(@Nonnull final ActionApiDTO actionApiDTO,
                             @Nonnull final Move move,
                             final MoveExplanation moveExplanation,
                             @Nonnull final ActionSpecMappingContext context)
            throws UnknownObjectException {

        final boolean initialPlacement = moveExplanation.hasInitialPlacement();
        if (initialPlacement) {
            actionApiDTO.setActionType(com.vmturbo.api.enums.ActionType.START);
        } else {
            // The UI (and legacy) expect the action type to be CHANGE for storage moves
            // and MOVE for host moves. We determine if we're dealing with a storage move or
            // a host move by looking at the type of the source entity.
            final String sourceType = context.getEntity(move.getSourceId()).getClassName();
            if (sourceType.equals(UIEntityType.STORAGE.getValue())) {
                actionApiDTO.setActionType(com.vmturbo.api.enums.ActionType.CHANGE);
            } else {
                actionApiDTO.setActionType(com.vmturbo.api.enums.ActionType.MOVE);
            }
        }

        // Set entity DTO fields for target, source (if needed) and destination entities
        setEntityDtoFields(actionApiDTO.getTarget(), move.getTargetId(), context);
        if (!initialPlacement) {
            long sourceId = move.getSourceId();
            actionApiDTO.setCurrentValue(Long.toString(sourceId));
            setEntityDtoFields(actionApiDTO.getCurrentEntity(), sourceId, context);
        }
        actionApiDTO.setNewValue(Long.toString(move.getDestinationId()));
        setEntityDtoFields(actionApiDTO.getNewEntity(), move.getDestinationId(), context);

        // Set action details
        actionApiDTO.setDetails(initialPlacement
            ? MessageFormat.format("Start {0} on {1}",
                readableEntityTypeAndName(actionApiDTO.getTarget()),
                readableEntityTypeAndName(actionApiDTO.getNewEntity()))
           : MessageFormat.format("Move {0} from {1} to {2}",
                readableEntityTypeAndName(actionApiDTO.getTarget()),
                readableEntityTypeAndName(actionApiDTO.getCurrentEntity()),
                readableEntityTypeAndName(actionApiDTO.getNewEntity())));
        // TODO: refer to MT ActionItemImpl for further explanation, e.g. "due to overutilized"
    }

    private void addReconfigureInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                    @Nonnull final Reconfigure reconfigure,
                                    @Nonnull final ReconfigureExplanation explanation,
                                    @Nonnull final ActionSpecMappingContext context)
            throws UnknownObjectException {
        actionApiDTO.setActionType(com.vmturbo.api.enums.ActionType.RECONFIGURE);

        setEntityDtoFields(actionApiDTO.getTarget(), reconfigure.getTargetId(), context);
        setEntityDtoFields(actionApiDTO.getCurrentEntity(), reconfigure.getSourceId(), context);

        actionApiDTO.setCurrentValue(Long.toString(reconfigure.getSourceId()));

        actionApiDTO.setDetails(MessageFormat.format(
            "Reconfigure {0} which requires {1} but is hosted by {2} which does not provide {1}",
                readableEntityTypeAndName(actionApiDTO.getTarget()),
                readableCommodityTypes(explanation.getReconfigureCommodityList()),
                readableEntityTypeAndName(actionApiDTO.getCurrentEntity())));
    }

    private void addProvisionInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                  @Nonnull final Provision provision,
                                  @Nonnull final ActionSpecMappingContext context)
            throws UnknownObjectException {
        actionApiDTO.setActionType(com.vmturbo.api.enums.ActionType.PROVISION);

        final String provisionedSellerUuid = Long.toString(provision.getProvisionedSeller());
        setNewEntityDtoFields(actionApiDTO.getTarget(), provision.getEntityToCloneId(),
                provisionedSellerUuid, context);

        actionApiDTO.setCurrentValue(Long.toString(provision.getEntityToCloneId()));
        setEntityDtoFields(actionApiDTO.getCurrentEntity(), provision.getEntityToCloneId(), context);

        actionApiDTO.setNewValue(provisionedSellerUuid);
        setNewEntityDtoFields(actionApiDTO.getNewEntity(), provision.getEntityToCloneId(),
                provisionedSellerUuid, context);

        actionApiDTO.setDetails(MessageFormat.format("Clone {0}",
                readableEntityTypeAndName(actionApiDTO.getCurrentEntity())));
    }

    private void addResizeInfo(@Nonnull final ActionApiDTO actionApiDTO,
                               @Nonnull final Resize resize,
                               @Nonnull final ActionSpecMappingContext context)
            throws UnknownObjectException {
        actionApiDTO.setActionType(com.vmturbo.api.enums.ActionType.RESIZE);

        setEntityDtoFields(actionApiDTO.getTarget(), resize.getTargetId(), context);
        setEntityDtoFields(actionApiDTO.getCurrentEntity(), resize.getTargetId(), context);
        setEntityDtoFields(actionApiDTO.getNewEntity(), resize.getTargetId(), context);

        final CommodityType commodityType = CommodityDTO.CommodityType.forNumber(
                resize.getCommodityType().getType());
        Objects.requireNonNull(commodityType, "Commodity for number "
                + resize.getCommodityType().getType());
        actionApiDTO.getRisk().setReasonCommodity(commodityType.name());
        actionApiDTO.setDetails(MessageFormat.format("Resize commodity {0} on entity {1} from {2} to {3}",
                readableCommodityTypes(Collections.singletonList(resize.getCommodityType().getType())),
                readableEntityTypeAndName(actionApiDTO.getTarget()),
                resize.getOldCapacity(),
                resize.getNewCapacity()));
    }

    private void addActivateInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                 @Nonnull final Activate activate,
                                 @Nonnull final ActionSpecMappingContext context)
            throws UnknownObjectException {
        actionApiDTO.setActionType(com.vmturbo.api.enums.ActionType.START);
        setEntityDtoFields(actionApiDTO.getTarget(), activate.getTargetId(), context);

        final List<String> reasonCommodityNames =
                activate.getTriggeringCommoditiesList().stream()
                        .map(commodityType -> CommodityDTO.CommodityType
                                .forNumber(commodityType.getType()))
                        .map(CommodityType::name)
                        .collect(Collectors.toList());

        actionApiDTO.getRisk().setReasonCommodity(reasonCommodityNames.stream()
                                                                      .collect(Collectors.joining(
                                                                              ",")));

        final StringBuilder detailStrBuilder = new StringBuilder()
                .append("Activate ")
                .append(readableEntityTypeAndName(actionApiDTO.getTarget()))
                .append(" due to increased demand for:");

        reasonCommodityNames.forEach(commodityType -> detailStrBuilder.append("\n")
                .append(commodityType));
        actionApiDTO.setDetails(detailStrBuilder.toString());
    }

    private void addDeactivateInfo(@Nonnull final ActionApiDTO actionApiDTO,
                                   @Nonnull final Deactivate deactivate,
                                   @Nonnull final ActionSpecMappingContext context)
            throws UnknownObjectException {
        setEntityDtoFields(actionApiDTO.getTarget(), deactivate.getTargetId(), context);
        actionApiDTO.setActionType(com.vmturbo.api.enums.ActionType.DEACTIVATE);

        final List<String> reasonCommodityNames =
                deactivate.getTriggeringCommoditiesList().stream()
                        .map(commodityType -> CommodityDTO.CommodityType
                                .forNumber(commodityType.getType()))
                        .map(CommodityType::name)
                        .collect(Collectors.toList());

        actionApiDTO.getRisk().setReasonCommodity(reasonCommodityNames.stream()
                                                                      .collect(Collectors.joining(
                                                                              ",")));
        String detailStrBuilder = MessageFormat.format("Deactivate {0}.",
                readableEntityTypeAndName(actionApiDTO.getTarget()));
        actionApiDTO.setDetails(detailStrBuilder);
    }

    /**
     * Return a nicely formatted string like:
     *
     * <p><code>Virtual Machine 'vm-test 01 for now'</code>
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
        return String.format("%s '%s'",
            formatString(entityDTO.getClassName()),
            entityDTO.getDisplayName()
        );
    }

    /**
     * Convert a list of commodity type numbers to a comma-separated string of readable commodity names.
     *
     * Example: BALLOONING, SWAPPING, CPU_ALLOCATION -> Ballooning, Swapping, Cpu Allocation
     *
     * @param commodityTypes numerical commodity types
     * @return comma-separated string commodity types
     */
    private String readableCommodityTypes(@Nonnull final List<Integer> commodityTypes) {
        return commodityTypes.stream()
            .map(CommodityType::forNumber)
            .map(Enum::name)
            .map(name -> formatString(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, name)))
            .collect(Collectors.joining(", "));
    }

    /**
     * Convert camel case (e.g. PhysicalMachine) into strings with the same
     * capitalization plus blank spaces (e.g. "Physical Machine"). It also splits numbers,
     * e.g. "May5" -> "May 5" and respects upper case runs, e.g. (PDFLoader -> "PDF Loader").
     *
     * The regex uses zero-length pattern matching with look-behind and look-forward, and is
     * taken from - http://stackoverflow.com/questions/2559759.
     *
     * @param str any string
     * @return see description
     */
    private String formatString(@Nonnull final String str) {
        return str.replaceAll(String.format("%s|%s|%s",
                "(?<=[A-Z])(?=[A-Z][a-z])",
                "(?<=[^A-Z])(?=[A-Z])",
                "(?<=[A-Za-z])(?=[^A-Za-z])"),
            " ");
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
                            @Nonnull final Optional<Collection<Long>> involvedEntities) {
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
            }
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
     * Populate the necessary fields in the response {@link ServiceEntityApiDTO} for a newly
     * created entity from the related entity OID as listed in the {link ActionSpecMappingContext}.
     *
     * TODO: this method will need to accommodate both OID and uuid in the return object
     * when we implement the legacy UUID in TopologyDTO.proto - see OM-14309.
     *
     * @param responseEntityApiDTO the response {@link ServiceEntityApiDTO} to populate as "new"
     * @param originalEntityOid the OID of the original {@link ServiceEntityApiDTO}  to copy from
     * @param newEntityOid the OID of the newly provisioned entity
     *@param context the {@link ActionSpecMappingContext} in which to look up the original
     *                entity OID  @throws UnknownObjectException if the originalEntityOid is not found in the context
     */
    private void setNewEntityDtoFields(@Nonnull final BaseApiDTO responseEntityApiDTO,
                                       final long originalEntityOid,
                                       @Nonnull String newEntityOid,
                                       @Nonnull final ActionSpecMappingContext context)
            throws UnknownObjectException {
        final ServiceEntityApiDTO originalEntity = context.getEntity(originalEntityOid);
        responseEntityApiDTO.setDisplayName("New Entity");
        responseEntityApiDTO.setUuid(newEntityOid);
        responseEntityApiDTO.setClassName(originalEntity.getClassName());
    }

    /**
     * Populate the necessary fields in the response {@link ServiceEntityApiDTO} from the
     * related entity OID as listed in the {link ActionSpecMappingContext}.
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
            throws UnknownObjectException {
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
    private static class ActionSpecMappingContext {
        private final Map<Long, Optional<ServiceEntityApiDTO>> entities;

        ActionSpecMappingContext(final long topologyContextId,
                                 @Nonnull final Set<Long> involvedEntities,
                                 @Nonnull final RepositoryApi repositoryApi) {
            final Map<Long, Optional<ServiceEntityApiDTO>> entities =
                    // We always search the projected topology because the projected topology is
                    // a super-set of the source topology. All involved entities that are in
                    // the source topology will also be in the projected topology, but there will
                    // be entities that are ONLY in the projected topology (e.g. actions involving
                    // newly provisioned hosts/VMs).
                    repositoryApi.getServiceEntitiesById(
                            ServiceEntitiesRequest.newBuilder(involvedEntities)
                                .setTopologyContextId(topologyContextId)
                                .searchProjectedTopology()
                                .build());
            this.entities = Collections.unmodifiableMap(entities);
        }

        @Nonnull
        ServiceEntityApiDTO getEntity(final long oid) throws UnknownObjectException {
            return Objects.requireNonNull(entities.get(oid))
                          .orElseThrow(() -> new UnknownObjectException("Entity: " + oid
                                  + " not found."));
        }
    }
}
