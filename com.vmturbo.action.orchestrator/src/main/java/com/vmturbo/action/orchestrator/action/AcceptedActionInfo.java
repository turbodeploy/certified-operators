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
 * Contains information about accepted action.
 */
@Immutable
public class AcceptedActionInfo {
    private final long recommendationId;
    private final LocalDateTime latestRecommendationTime;
    private final String acceptedBy;
    private final LocalDateTime acceptedTime;
    private final String acceptorType;
    private final List<Long> relatedPolicies;

    /**
     * Constructor of {@link AcceptedActionInfo}.
     *
     * @param recommendationId the recommendationId of accepted action
     * @param latestRecommendationTime the latest time when action was recommended
     * @param acceptedBy the action acceptor
     * @param acceptedTime the time when action was accepted
     * @param acceptorType the type of acceptor (e.g. turbo or orchestrator)
     * @param relatedPolicies policies associated with action
     */
    public AcceptedActionInfo(long recommendationId,
            @Nonnull LocalDateTime latestRecommendationTime, @Nonnull String acceptedBy,
            @Nonnull LocalDateTime acceptedTime, @Nonnull String acceptorType,
            @Nonnull Collection<Long> relatedPolicies) {
        this.recommendationId = recommendationId;
        this.latestRecommendationTime = Objects.requireNonNull(latestRecommendationTime);
        this.acceptedBy = Objects.requireNonNull(acceptedBy);
        this.acceptedTime = Objects.requireNonNull(acceptedTime);
        this.acceptorType = Objects.requireNonNull(acceptorType);
        this.relatedPolicies = Collections.unmodifiableList(
                new ArrayList<>(Objects.requireNonNull(relatedPolicies)));
    }

    /**
     * Return accepted action recommendationId.
     *
     * @return the recommendation id
     */
    public long getRecommendationId() {
        return recommendationId;
    }

    /**
     * Return the latest time when action was recommended by market.
     *
     * @return the latest recommendation time
     */
    @Nonnull
    public LocalDateTime getLatestRecommendationTime() {
        return latestRecommendationTime;
    }

    /**
     * Return the name of user who accepted the action.
     *
     * @return the name of acceptor
     */
    @Nonnull
    public String getAcceptedBy() {
        return acceptedBy;
    }

    /**
     * Return the time when action was accepted.
     *
     * @return the time when action was accepted.
     */
    @Nonnull
    public LocalDateTime getAcceptedTime() {
        return acceptedTime;
    }

    /**
     * Return the type of acceptor (e.g. TURBO_USER or orchestrator probe/target name if action was
     * approved by orchestrator).
     *
     * @return the acceptor type
     */
    @Nonnull
    public String getAcceptorType() {
        return acceptorType;
    }

    /**
     * Return list of policies associated with accepted action.
     *
     * @return list of associated policies
     */
    @Nonnull
    public Collection<Long> getRelatedPolicies() {
        return relatedPolicies;
    }
}
