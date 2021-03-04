package com.vmturbo.action.orchestrator.action;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * Contains information about actions (recommended by market or missing less then
 * AuditCommunicationConfig.minsClearedActionsCriteria minutes) that Action Orchestrator has sent
 * for auditing.
 */
public class AuditedActionInfo {

    private final long recommendationId;
    private final long workflowId;
    private final long targetEntityId;
    @Nonnull
    private final String settingName;
    @Nonnull
    private final Optional<Long> clearedTimestamp;

    /**
     * Constructor of {@link AuditedActionInfo}.
     *
     * @param recommendationId action identifier
     * @param workflowId workflow identifier
     * @param targetEntityId the entity id of the target of the action
     * @param settingName the setting name the workflow applies to only in the context of this action
     * @param clearedTimestamp time when Action Orchestrator first notices that the action is no
     *                         longer recommended. Will be Optional.empty() when action is generated
     *                         by the market, or when the action has been missing for
     *                         AuditCommunicationConfig.minsClearedActionsCriteria minutes.
     */
    public AuditedActionInfo(
            long recommendationId,
            long workflowId,
            long targetEntityId,
            @Nonnull String settingName,
            @Nonnull Optional<Long> clearedTimestamp) {
        this.recommendationId = recommendationId;
        this.workflowId = workflowId;
        this.targetEntityId = targetEntityId;
        this.settingName = settingName;
        this.clearedTimestamp = clearedTimestamp;
    }

    /**
     * Returns stable identifier of the action that was sent for audit.
     *
     * @return action stable id
     */
    public long getRecommendationId() {
        return recommendationId;
    }

    /**
     * Returns identifier ot the workflow.
     *
     * @return workflow id associated with action that we sent for audit
     */
    public long getWorkflowId() {
        return workflowId;
    }

    /**
     * Returns the identifier ot the target entity of the action.
     *
     * @return the identifier ot the target entity of the action.
     */
    public long getTargetEntityId() {
        return targetEntityId;
    }

    /**
     * Returns the setting name the workflow applies to only in the context of this action.
     *
     * @return the setting name the workflow applies to only in the context of this action.
     */
    @Nonnull
    public String getSettingName() {
        return settingName;
    }

    /**
     * The time when Action Orchestrator first notices that the action is no longer recommended.
     * Will be Optional.empty() when action is first generated, action returns from cleared, or
     * expired.
     *
     * @return time when Action Orchestrator first notices that the action is no longer recommended.
     * Will be Optional.empty() when action is first generated, action returns from cleared, or expired.
     */
    @Nonnull
    public Optional<Long> getClearedTimestamp() {
        return clearedTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AuditedActionInfo that = (AuditedActionInfo)o;
        return recommendationId == that.recommendationId
            && workflowId == that.workflowId
            && targetEntityId == that.targetEntityId
            && Objects.equals(settingName, that.settingName)
            && Objects.equals(clearedTimestamp, that.clearedTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            recommendationId,
            workflowId,
            targetEntityId,
            settingName,
            clearedTimestamp);
    }

    @Override
    public String toString() {
        return "AuditedActionInfo{"
            + "recommendationId=" + recommendationId
            + ", workflowId=" + workflowId
            + ", targetEntityId=" + targetEntityId
            + ", settingName=" + settingName
            + ", clearedTimestamp=" + clearedTimestamp.orElse(null)
            + '}';
    }
}
