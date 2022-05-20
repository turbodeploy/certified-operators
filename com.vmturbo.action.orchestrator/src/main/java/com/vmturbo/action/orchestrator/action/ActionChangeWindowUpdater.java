package com.vmturbo.action.orchestrator.action;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Listens for cloud action events and on successful completion, updates the change_window DB record.
 */
public class ActionChangeWindowUpdater implements ActionStateMachine.ActionEventListener {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Interested workloads.
     */
    private static final Set<Integer> WORKLOAD_TYPE_VALUES = Stream.concat(
                    TopologyDTOUtil.WORKLOAD_TYPES.stream(),
                    Stream.of(EntityType.VIRTUAL_VOLUME)).map(EntityType::getNumber)
            .collect(Collectors.toSet());

    /**
     * DB store.
     */
    private final ExecutedActionsChangeWindowDao actionDao;

    /**
     * Create new instance.
     *
     * @param actionDao DB store.
     */
    public ActionChangeWindowUpdater(@Nonnull final ExecutedActionsChangeWindowDao actionDao) {
        this.actionDao = Objects.requireNonNull(actionDao);
    }

    @Override
    public void onActionEvent(@Nonnull ActionView actionView, @Nonnull ActionState preState,
            @Nonnull ActionState postState, @Nonnull ActionEvent event,
            boolean performedTransition) {
        if (performedTransition && postState == ActionState.SUCCEEDED) {
            saveExecutedActionsChangeWindow(actionView);
        }
    }

    /**
     * Persist certain timeframe related information of successfully executed action to DB.
     * We are not saving the completion time initially, that will get updated later.
     *
     * @param action the executed action.
     */
    private void saveExecutedActionsChangeWindow(final ActionView action) {
        final long actionId = action.getRecommendation().getId();
        long entityId;
        ActionEntity actionEntity;
        try {
            actionEntity = ActionDTOUtil.getPrimaryEntity(action.getRecommendation());
            entityId = actionEntity.getId();
        } catch (UnsupportedActionException uae) {
            logger.error("Could not look up entity for action {}.", actionId, uae);
            return;
        }
        if (actionEntity.getEnvironmentType() != EnvironmentTypeEnum.EnvironmentType.CLOUD) {
            return;
        }
        if (!WORKLOAD_TYPE_VALUES.contains(actionEntity.getType())) {
            return;
        }
        try {
            actionDao.saveExecutedAction(actionId, entityId);
        } catch (ActionStoreOperationException e) {
            logger.error("Could not save change window record for action {}, entity {} ",
                    actionId, entityId, e);
        }
    }
}
