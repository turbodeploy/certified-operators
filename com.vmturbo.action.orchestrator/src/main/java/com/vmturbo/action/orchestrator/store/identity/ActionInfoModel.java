package com.vmturbo.action.orchestrator.store.identity;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.logging.log4j.LogManager;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;

/**
 * Identity model for {@link com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo}.
 * This class is used in {@link IdentityServiceImpl} in order to determine that the market
 * recommendations are logically the same. If two models are equal, it means that they reflect
 * two actions that are logically the same.
 */
@Immutable
public class ActionInfoModel {
    /**
     * Maximum allowed length of a {@link #details} string. It is used to
     * restrict values larger as they will not fit into DB tables.
     *
     * <p>It is the largest size of VARCHAR supported by old maridDB of old versions.
     */
    private static final int MAX_DETAILS_LENGTH = 255;
    /**
     * Maximum allowed length of strings inside {@link #additionalDetails}. It is used to
     * restrict values larger as they will not fit into DB tables.
     *
     * <p>This length of string is sufficient to hold everything that could be put into the
     * additional details as there are no strings in the serialized data (only primitive values),
     * i.e. we can guarantee that values are not exceeding this size.
     */
    private static final int MAX_ADDITIONAL_DETAILS_LENGTH = 200;
    private final ActionTypeCase actionType;
    private final long targetId;
    private final String details;
    private final Set<String> additionalDetails;

    /**
     * Constructs action info model from the parts.
     *
     * @param actionType action type
     * @param targetId action target entity OID
     * @param details action details (varied by action type). Must be shorter then
     *         {@link #MAX_DETAILS_LENGTH} characters.
     * @param additionalDetails additional details, to be used as a set. If this parameter
     *         is {@code null} this means that there is no additional details expected for this
     *         action type. If value is an empty set this means that additional details could
     *         occur for such an actions but this specific action does not have any.
     *         Every member of this set must be shorter then {@link #MAX_ADDITIONAL_DETAILS_LENGTH}
     *         characters.
     */
    public ActionInfoModel(@Nonnull ActionTypeCase actionType, long targetId,
            @Nullable String details, @Nullable Set<String> additionalDetails) {
        this.actionType = Objects.requireNonNull(actionType);
        this.targetId = targetId;
        this.details = fixDetails(details);
        this.additionalDetails = fixAdditionalDetails(additionalDetails);
    }

    @Nullable
    private String fixDetails(@Nullable String details) {
        if (details == null) {
            return null;
        } else {
            if (details.length() > MAX_DETAILS_LENGTH) {
                LogManager.getLogger(getClass()).error(
                        "Action details length exceeded maximum size of " + MAX_DETAILS_LENGTH
                                + " chars, found: \"" + details + "\" for action " + actionType
                                + " target " + targetId
                                + ". This can lead to losing OID data.");
                return details.substring(0, MAX_DETAILS_LENGTH);
            } else {
                return details;
            }
        }
    }

    @Nullable
    private Set<String> fixAdditionalDetails(@Nullable Set<String> additionalDetails) {
        if (additionalDetails == null) {
            return null;
        } else {
            final Set<String> additionalDetailsList = new HashSet<>(additionalDetails.size());
            for (String additionalDetail : additionalDetails) {
                final String additionalDetailFixed;
                if (additionalDetail.length() > MAX_ADDITIONAL_DETAILS_LENGTH) {
                    LogManager.getLogger(getClass()).error(
                            "Additional action details length exceeded maximum size of "
                                    + MAX_ADDITIONAL_DETAILS_LENGTH + " chars, found: \""
                                    + additionalDetail + "\" for action " + actionType
                                    + " detail: \"" + details + "\"" + " target " + targetId
                                    + ". This can lead to losing OID data.");
                    additionalDetailFixed = additionalDetail.substring(0,
                            MAX_ADDITIONAL_DETAILS_LENGTH);
                } else {
                    additionalDetailFixed = additionalDetail;
                }
                additionalDetailsList.add(additionalDetailFixed);
            }
            return Collections.unmodifiableSet(additionalDetailsList);
        }
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

    @Nonnull
    public Optional<Set<String>> getAdditionalDetails() {
        return Optional.ofNullable(additionalDetails);
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
                && Objects.equals(details, that.details)
                && Objects.equals(additionalDetails, that.additionalDetails);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actionType, targetId, details, additionalDetails);
    }

    @Override
    public String toString() {
        return actionType + ":" + targetId;
    }
}
