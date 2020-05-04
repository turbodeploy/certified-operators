package com.vmturbo.integrations.intersight.targetsync;

import java.time.OffsetDateTime;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.cisco.intersight.client.model.AssetService;
import com.cisco.intersight.client.model.AssetService.StatusEnum;
import com.cisco.intersight.client.model.AssetTarget;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.topology.processor.api.TargetInfo;

/**
 * A wrapper combining both representations of the same target: the {@link AssetTarget} discovered
 * from Intersight and the {@link TargetInfo} from the topology processor.
 */
public class WrappedTarget {
    private final AssetTarget intersightAssetTarget;
    private final TargetInfo tpTargetInfo;

    /**
     * Construct a {@link WrappedTarget}.  The input {@link AssetTarget} and the
     * {@link TargetInfo} represent the same target.
     *
     * @param intersightAssetTarget the {@link AssetTarget} discovered from Intersight
     * @param tpTargetInfo the {@link TargetInfo} from topology processor
     */
    public WrappedTarget(@Nonnull final AssetTarget intersightAssetTarget,
                         @Nonnull final TargetInfo tpTargetInfo) {
        this.intersightAssetTarget = Objects.requireNonNull(intersightAssetTarget);
        this.tpTargetInfo = Objects.requireNonNull(tpTargetInfo);
    }

    /**
     * Return the Intersight portion of the target info.
     * @return the Intersight portion of the target info
     */
    public AssetTarget intersight() {
        return intersightAssetTarget;
    }

    /**
     * Return the topology processor portion of the target info.
     * @return the topology processor portion of the target info
     */
    public TargetInfo tp() {
        return tpTargetInfo;
    }

    /**
     * Return true if the Intersight target is "recently" created and is still in the
     * "CLAIMINPROGRESS" state (the state when it is first created).
     *
     * @param recentInSeconds created within this number of seconds deems to be recently
     * @return true if the Intersight target is recently created; false otherwise.
     */
    public boolean ifRecentlyCreated(final long recentInSeconds) {
        return intersightAssetTarget.getStatus() == AssetTarget.StatusEnum.CLAIMINPROGRESS &&
                intersightAssetTarget.getCreateTime() != null && intersightAssetTarget.getCreateTime()
                .isAfter(OffsetDateTime.now().minusSeconds(recentInSeconds));
    }

    /**
     * Return true if the Intersight target is "recently" modified and is still in the "EMPTY"
     * state (means the state is cleared out when it is modified).
     *
     * @param recentInSeconds modified within this number of seconds deems to be recently
     * @return true if the Intersight target is recently modified"; false otherwise.
     */
    public boolean ifRecentlyModified(final long recentInSeconds) {
        return intersightAssetTarget.getStatus() == AssetTarget.StatusEnum.EMPTY &&
                intersightAssetTarget.getModTime() != null && intersightAssetTarget.getModTime()
                .isAfter(OffsetDateTime.now().minusSeconds(recentInSeconds));
    }

    /**
     * Return if the current status from topology processor indicates that the target is validated.
     *
     * @return whether the status indicates the target is validated
     */
    public boolean isValidated() {
        return StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_SUCCESS.equals(tpTargetInfo.getStatus());
    }

    /**
     * Return true if the current status from topology processor indicates that the target is
     * in progress of either discovery or validation.
     *
     * @return whether the status indicates the target is in progress of either discovery or
     * validation
     */
    public boolean isInProgress() {
        return StringConstants.TOPOLOGY_PROCESSOR_VALIDATION_IN_PROGRESS.equals(tpTargetInfo.getStatus())
                || StringConstants.TOPOLOGY_PROCESSOR_DISCOVERY_IN_PROGRESS.equals(tpTargetInfo.getStatus());
    }

    /**
     * Return true if the given service's status needs update, under the following condition:
     * 1) if the new status enum is different; OR
     * 2) if the new status string is different.
     *
     * @param service the AssetService discovered from Intersight which contains the previously
     *                reported status
     * @return true if status update is needed
     */
    public boolean needsUpdate(@Nonnull final AssetService service) {
        Objects.requireNonNull(service);
        return !Objects.equals(getNewStatusEnum(service), service.getStatus())
                || !Objects.equals(getNewStatusErrorReason(service), service.getStatusErrorReason());
    }

    /**
     * Compute the new target {@link StatusEnum} to report back to Intersight, based on the
     * following rules:
     * 1) If the target is validated, return CONNECTED;
     * 2) If the target is discovery in progress, return the old value in the service;
     * 3) Else return NOTCONNECTED.
     *
     * @param service the AssetService discovered from Intersight which contains the previously
     *                reported status enum
     * @return the newly computed {@link StatusEnum}
     */
    public StatusEnum getNewStatusEnum(@Nonnull final AssetService service) {
        return isValidated() ? StatusEnum.CONNECTED : isInProgress() ?
                Objects.requireNonNull(service).getStatus() : StatusEnum.NOTCONNECTED;
    }

    /**
     * Return the new target status string if there is an error, or an empty string if no error.
     *
     * @param service the AssetService discovered from Intersight which contains the previously
     *                reported status enum
     * @return the newly computed status string if there is an error, or an empty string if no error
     */
    public String getNewStatusErrorReason(@Nonnull final AssetService service) {
        return StatusEnum.CONNECTED.equals(getNewStatusEnum(Objects.requireNonNull(service)))
                ? "" : tpTargetInfo.getStatus();
    }
}
