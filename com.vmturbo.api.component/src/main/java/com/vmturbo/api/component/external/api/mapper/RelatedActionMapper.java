package com.vmturbo.api.component.external.api.mapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.action.BasicActionApiDTO;
import com.vmturbo.api.dto.action.RelatedActionApiDTO;
import com.vmturbo.api.dto.entity.DiscoveredEntityApiDTO;
import com.vmturbo.api.enums.ActionRelationType;
import com.vmturbo.api.enums.ActionType;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockedByRelation.BlockedByRelationTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.BlockingRelation.BlockingRelationTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.CausedByRelation.CausedByRelationTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.CausingRelation.CausingRelationTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.RelatedAction;
import com.vmturbo.common.protobuf.action.ActionDTO.RelatedAction.ActionRelationTypeCase;

/**
 * A utility class to map list of {@link RelatedAction} from an {@link ActionSpec} to list
 * {@link RelatedActionApiDTO}.
 */
public class RelatedActionMapper {

    private static final Logger logger = LogManager.getLogger();

    private RelatedActionMapper() {}

    /**
     * Map list of {@link RelatedAction} from an {@link ActionSpec} to list of {@link RelatedActionApiDTO}.
     *
     * @param actionSpec {@link ActionSpec} from which list of {@link RelatedAction} is mapped to list
     *                   of {@link RelatedActionApiDTO}.
     * @return List of {@link RelatedActionApiDTO} mapped from list of {@link RelatedAction} in
     * {@link ActionSpec}.
     */
    @Nonnull
    public static List<RelatedActionApiDTO> mapXlRelatedActionsToApi(@Nonnull final ActionSpec actionSpec) {
        return actionSpec.getRelatedActionsList().stream()
                .map(relatedAction -> {
                    RelatedActionApiDTO relatedActionApiDTO = new RelatedActionApiDTO();
                    BasicActionApiDTO actionApiDTO = new BasicActionApiDTO();
                    if (relatedAction.hasRecommendationId()) {
                        actionApiDTO.setActionID(relatedAction.getRecommendationId());
                    }
                    mapXlActionRelationTypeToApi(relatedAction.getActionRelationTypeCase())
                            .ifPresent(relatedActionApiDTO::setActionRelationType);
                    getActionTypeFromRelatedAction(relatedAction)
                            .ifPresent(actionApiDTO::setActionType);
                    if (relatedAction.hasActionEntity()) {
                        DiscoveredEntityApiDTO entityApiDTO =
                                ActionSpecMapper.getDiscoveredEntityApiDTOFromActionEntity(relatedAction.getActionEntity());
                        entityApiDTO.setDisplayName(relatedAction.getActionEntityDisplayName());
                        actionApiDTO.setTarget(entityApiDTO);
                    }
                    if (relatedAction.hasDescription()) {
                        actionApiDTO.setDetails(relatedAction.getDescription());
                    }
                    relatedActionApiDTO.setAction(actionApiDTO);
                    return relatedActionApiDTO;
                }).collect(Collectors.toList());
    }

    /**
     * Return a map of related actions count by action relation type.
     *
     * @param actionSpec Given {@link ActionSpec}.
     * @return Map of related actions count by action relation type.
     */
    @Nonnull
    public static Map<ActionRelationType, Integer> countRelatedActionsByType(@Nonnull final ActionSpec actionSpec) {
        Map<ActionRelationType, Integer> relatedActionsByType =
                actionSpec.getRelatedActionsList().stream()
                .map(ra -> mapXlActionRelationTypeToApi(ra.getActionRelationTypeCase()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.summingInt(x -> 1)));
        /* If we don't have a related action and the underlying action
        is executable false we will add an entry into RelatedActionsByType
        with key BLOCKED_BY and value 0. This will make the action state reason
        as blocked by related action rather than blocked by policy. If market
        sets an action to no executable it is because there is a related action blocking it.
        Market is not able to identify the blocking action and pass on in the related actions..But
        setting the executable to false is a guaranteed sign that market is aware of a related action
        and that is why market set it to executable false. By adding an entry in this map we now
        can seamlessly display the reason for the action being not execuatale is due to
        blocking action and not due to policy.*/
        if (relatedActionsByType.isEmpty()
                && actionSpec.getRecommendation() != null
                && !actionSpec.getRecommendation().getExecutable()) {
            relatedActionsByType.put(ActionRelationType.BLOCKED_BY, 0);
        }
        return relatedActionsByType;
    }


    private static Optional<ActionRelationType> mapXlActionRelationTypeToApi(@Nonnull final ActionRelationTypeCase actionRelationTypeCase) {
        switch (actionRelationTypeCase) {
            case BLOCKED_BY_RELATION:
                return Optional.of(ActionRelationType.BLOCKED_BY);
            case BLOCKING_RELATION:
                return Optional.of(ActionRelationType.BLOCKING);
            case CAUSED_BY_RELATION:
                return Optional.of(ActionRelationType.CAUSED_BY);
            case CAUSING_RELATION:
                return Optional.of(ActionRelationType.CAUSING);
            default:
                logger.error("Unsupported action relation type {}", actionRelationTypeCase.name());
                return Optional.empty();
        }
    }

    private static Optional<ActionType> getActionTypeFromRelatedAction(@Nonnull final RelatedAction relatedAction) {
        ActionRelationTypeCase actionRelationTypeCase = relatedAction.getActionRelationTypeCase();
        switch (actionRelationTypeCase) {
            case BLOCKED_BY_RELATION:
                BlockedByRelationTypeCase blockedByRelationType =
                        relatedAction.getBlockedByRelation().getBlockedByRelationTypeCase();
                if (blockedByRelationType == BlockedByRelationTypeCase.RESIZE) {
                    return Optional.of(ActionType.RESIZE);
                } else {
                    logger.error("Unsupported BLOCKED_BY_RELATION type {}", blockedByRelationType);
                    return Optional.empty();
                }
            case BLOCKING_RELATION:
                BlockingRelationTypeCase blockingRelationType =
                        relatedAction.getBlockingRelation().getBlockingRelationTypeCase();
                if (blockingRelationType == BlockingRelationTypeCase.RESIZE) {
                    return Optional.of(ActionType.RESIZE);
                } else {
                    logger.error("Unsupported BLOCKING_RELATION type {}", blockingRelationType);
                    return Optional.empty();
                }
            case CAUSED_BY_RELATION:
                CausedByRelationTypeCase causedByRelationType =
                        relatedAction.getCausedByRelation().getCausedByRelationTypeCase();
                switch (causedByRelationType) {
                    case PROVISION:
                        return Optional.of(ActionType.PROVISION);
                    case SUSPENSION:
                        return Optional.of(ActionType.SUSPEND);
                    default:
                        logger.error("Unsupported CAUSED_BY_RELATION type {}", causedByRelationType);
                        return Optional.empty();
                }
            case CAUSING_RELATION:
                CausingRelationTypeCase causingRelationTypeCase =
                        relatedAction.getCausingRelation().getCausingRelationTypeCase();
                switch (causingRelationTypeCase) {
                    case PROVISION:
                        return Optional.of(ActionType.PROVISION);
                    case SUSPENSION:
                        return Optional.of(ActionType.SUSPEND);
                    default:
                        logger.error("Unsupported CAUSING_RELATION type {}", causingRelationTypeCase);
                        return Optional.empty();
                }
            default:
                logger.error("Unsupported action relation type {}", actionRelationTypeCase);
                return Optional.empty();
        }
    }

    /**
     * Map an API ActionRelationType to an equivalent XL ActionRelationType.
     *
     * @param apiRelatedAction TActionRelationType in the UI.
     * @return An optional containing a {@link ActionRelationTypeCase}, or an empty optional if
     *         no equivalent ActionRelationType exists in XL.
     */
    public static Optional<ActionDTO.ActionRelationType> mapApiActionRelationTypeEnumToXl(
            @Nonnull final ActionRelationType apiRelatedAction) {
        switch (apiRelatedAction) {
            case BLOCKING:
                return Optional.of(ActionDTO.ActionRelationType.BLOCKING);
            case BLOCKED_BY:
                return Optional.of(ActionDTO.ActionRelationType.BLOCKED_BY);
            case CAUSING:
                return Optional.of(ActionDTO.ActionRelationType.CAUSING);
            case CAUSED_BY:
                return Optional.of(ActionDTO.ActionRelationType.CAUSED_BY);
            case NONE:
                return Optional.of(ActionDTO.ActionRelationType.RELATION_NONE);
            default:
                return Optional.empty();
        }
    }
}
