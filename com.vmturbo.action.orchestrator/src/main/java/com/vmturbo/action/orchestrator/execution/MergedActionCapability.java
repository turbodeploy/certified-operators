package com.vmturbo.action.orchestrator.execution;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapability;
import com.vmturbo.common.protobuf.topology.Probe.ProbeActionCapability.ActionCapabilityElement;

/**
 * Resulting action capability calculated based on all action capability elements sent by the
 * probe for the given combination of entity and action types.
 */
@Immutable
@ThreadSafe
public class MergedActionCapability {
    private final SupportLevel supportLevel;
    private final Boolean disruptive;
    private final Boolean reversible;

    private MergedActionCapability(
            @Nonnull final SupportLevel supportLevel,
            @Nullable final Boolean disruptive,
            @Nullable final Boolean reversible) {
        this.supportLevel = supportLevel;
        this.disruptive = disruptive;
        this.reversible = reversible;
    }

    private MergedActionCapability(@Nonnull final SupportLevel supportLevel) {
        this(supportLevel, null, null);
    }

    /**
     * Create {@code MergedActionCapability} with support level set to
     * {@link SupportLevel#SUPPORTED}.
     *
     * @return New instance of {@code MergedActionCapability}.
     */
    @Nonnull
    public static MergedActionCapability createSupported() {
        return new MergedActionCapability(SupportLevel.SUPPORTED);
    }

    /**
     * Create {@code MergedActionCapability} with support level set to
     * {@link SupportLevel#UNSUPPORTED}.
     *
     * @return New instance of {@code MergedActionCapability}.
     */
    @Nonnull
    public static MergedActionCapability createNotSupported() {
        return new MergedActionCapability(SupportLevel.UNSUPPORTED);
    }

    /**
     * Create {@code MergedActionCapability} with support level set to
     * {@link SupportLevel#SHOW_ONLY}.
     *
     * @return New instance of {@code MergedActionCapability}.
     */
    @Nonnull
    public static MergedActionCapability createShowOnly() {
        return new MergedActionCapability(SupportLevel.SHOW_ONLY);
    }

    /**
     * Create {@code MergedActionCapability} from {@link ActionCapabilityElement} sent by the
     * probe.
     *
     * @param actionCapabilityElement {@link ActionCapabilityElement} sent by the probe.
     * @return New instance of {@code MergedActionCapability}.
     */
    @Nonnull
    public static MergedActionCapability from(
            @Nonnull final ActionCapabilityElement actionCapabilityElement) {
        final SupportLevel supportLevel = convertToSupportLevel(
                actionCapabilityElement.getActionCapability());
        final Boolean disruptive = actionCapabilityElement.hasDisruptive()
                ? actionCapabilityElement.getDisruptive() : null;
        final Boolean reversible = actionCapabilityElement.hasReversible()
                ? actionCapabilityElement.getReversible() : null;
        return new MergedActionCapability(supportLevel, disruptive, reversible);
    }

    /**
     * Gets action support level.
     *
     * @return Action support level.
     */
    @Nonnull
    public SupportLevel getSupportLevel() {
        return supportLevel;
    }

    /**
     * Checks if action is disruptive.
     *
     * @return True for disruptive action. {@code null} return value means that probe didn't
     * send any information about action disruptiveness.
     */
    @Nullable
    public Boolean getDisruptive() {
        return disruptive;
    }

    /**
     * Checks if action is reversible.
     *
     * @return True for reversible action. {@code null} return value means that probe didn't
     * send any information about action reversibility.
     */
    @Nullable
    public Boolean getReversible() {
        return reversible;
    }

    @Nonnull
    private static ActionDTO.Action.SupportLevel convertToSupportLevel(@Nonnull ActionCapability capability) {
        switch (capability) {
            case NOT_SUPPORTED:
                return SupportLevel.UNSUPPORTED;
            case NOT_EXECUTABLE:
                return SupportLevel.SHOW_ONLY;
            default:
                return SupportLevel.SUPPORTED;
        }
    }
}
