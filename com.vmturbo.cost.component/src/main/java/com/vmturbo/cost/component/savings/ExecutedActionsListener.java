package com.vmturbo.cost.component.savings;

import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Listens for successful action executions and updates the state table, so that we can start
 * tracking those, and do things like read costs from bill for those entities.
 */
public class ExecutedActionsListener implements ActionsListener {
    private final Logger logger = LogManager.getLogger();

    private final Set<Integer> supportedEntityTypes;
    private final SavingsActionStore savingsActionStore;

    /**
     * Create new instance.
     *
     * @param supportedEntityTypes Supported entity types to listen actions for.
     */
    public ExecutedActionsListener(@Nonnull final Set<EntityType> supportedEntityTypes,
            @Nonnull final SavingsActionStore savingsActionStore) {
        this.supportedEntityTypes = supportedEntityTypes.stream()
                .map(EntityType::getNumber)
                .collect(Collectors.toSet());
        this.savingsActionStore = savingsActionStore;
    }

    @Override
    public void onActionSuccess(@Nonnull final ActionSuccess actionSuccess) {
        final Long actionId = actionSuccess.getActionId();
        final ActionSpec actionSpec = actionSuccess.getActionSpec();

        final Action action = actionSpec.getRecommendation();
        try {
            final ActionEntity entity = ActionDTOUtil.getPrimaryEntity(actionSpec.getRecommendation());
            if (!entity.hasEnvironmentType() || entity.getEnvironmentType() != EnvironmentType.CLOUD) {
                logger.trace("Skipping non-cloud executed action {} for entity {}.",
                        () -> actionId, () -> entity);
                return;
            }
            if (!supportedEntityTypes.contains(entity.getType())) {
                logger.trace("Skipping unsupported entity type {} for entity {}.",
                        entity::getType, () -> entity);
                return;
            }
            ActionInfo actionInfo = action.getInfo();
            if (!actionInfo.hasScale() && !actionInfo.hasDelete()) {
                logger.trace("Skipping non scale/delete executed action {} for entity {}.",
                        () -> actionInfo, () -> entity);
                return;
            }
            long entityId = entity.getId();
            // Notify savings store about this new action so that it can be marked dirty.
            savingsActionStore.onNewAction(actionId, entityId);
            logger.info("Detected executed cloud action {} for entity {}.", actionId, entityId);
        } catch (UnsupportedActionException e) {
            logger.warn("Unable to process action {} for bill savings.", actionId, e);
        }
    }
}
