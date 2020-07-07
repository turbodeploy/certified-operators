package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Contains information about rejected action.
 */
@Immutable
public class RejectedActionInfo {
    private final long recommendationId;
    private final String rejectedBy;
    private final LocalDateTime rejectedTime;
    private final String rejectingUserType;
    private final List<Long> relatedPolicies;

    /**
     * Constructor of {@link RejectedActionInfo}.
     *
     * @param recommendationId the recommendationId of rejected action
     * @param rejectedBy the action acceptor
     * @param rejectedTime the time when action was rejected
     * @param rejectingUserType the type of acceptor (e.g. turbo or orchestrator)
     * @param relatedPolicies policies associated with action
     */
    public RejectedActionInfo(long recommendationId, @Nonnull String rejectedBy,
            @Nonnull LocalDateTime rejectedTime, @Nonnull String rejectingUserType,
            @Nonnull Collection<Long> relatedPolicies) {
        this.recommendationId = recommendationId;
        this.rejectedBy = Objects.requireNonNull(rejectedBy);
        this.rejectedTime = Objects.requireNonNull(rejectedTime);
        this.rejectingUserType = Objects.requireNonNull(rejectingUserType);
        this.relatedPolicies = Collections.unmodifiableList(
                new ArrayList<>(Objects.requireNonNull(relatedPolicies)));
    }

    /**
     * Return rejected action recommendationId.
     *
     * @return the recommendation id
     */
    public long getRecommendationId() {
        return recommendationId;
    }

    /**
     * Return the name of user who rejected the action.
     *
     * @return the name of acceptor
     */
    @Nonnull
    public String getRejectedBy() {
        return rejectedBy;
    }

    /**
     * Return the time when action was rejected.
     *
     * @return the time when action was rejected.
     */
    @Nonnull
    public LocalDateTime getRejectedTime() {
        return rejectedTime;
    }

    /**
     * Return the type of rejecting user.
     *
     * @return the type of rejecting user.
     */
    @Nonnull
    public String getRejectingUserType() {
        return rejectingUserType;
    }

    /**
     * Return list of policies associated with rejected action.
     *
     * @return list of associated policies
     */
    @Nonnull
    public Collection<Long> getRelatedPolicies() {
        return relatedPolicies;
    }
}
