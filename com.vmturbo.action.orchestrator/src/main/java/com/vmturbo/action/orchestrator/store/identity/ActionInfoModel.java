package com.vmturbo.action.orchestrator.store.identity;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;

/**
 * Identity model for {@link com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo}.
 * This class is used in {@link IdentityServiceImpl} in order to determine that the market
 * recommendations are logically the same. If two models are equal, it means that they reflect
 * two actions that are logically the same.
 */
@Immutable
public class ActionInfoModel {
    private final ActionTypeCase actionType;
    private final long targetId;
    private final String details;

    /**
     * Consctructs action info model from the parts.
     *
     * @param actionType action type
     * @param targetId action target entity OID
     * @param details action details (varied by action type)
     */
    public ActionInfoModel(@Nonnull ActionTypeCase actionType, long targetId,
            @Nullable String details) {
        this.actionType = Objects.requireNonNull(actionType);
        this.targetId = targetId;
        this.details = details;
    }

    @Nonnull
    public ActionTypeCase getActionType() {
        return actionType;
    }

    public long getTargetId() {
        return targetId;
    }

    @Nonnull
    public Optional<String> getDetails() {
        return Optional.ofNullable(details);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionInfoModel)) {
            return false;
        }
        ActionInfoModel that = (ActionInfoModel)o;
        return targetId == that.targetId && actionType == that.actionType
                && Objects.equals(details, that.details);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actionType, targetId, details);
    }

    @Override
    public String toString() {
        return actionType + ":" + targetId;
    }
}
