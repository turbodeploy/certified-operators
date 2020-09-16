package com.vmturbo.action.orchestrator.action;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;

/**
 * Composite or multi-step actions may have multiple executable steps.
 */
@NotThreadSafe
public class ExecutableStep {
    public static final String INITIAL_QUEUED_DESCRIPTION = "Queued for execution.";
    public static final String INITIAL_EXECUTION_DESCRIPTION = "Initiating execution.";
    public static final String SUCCESS_DESCRIPTION = "Completed successfully.";
    public static final String FAILURE_DESCRIPTION = "Failed to complete.";

    private final ExecutionStep.Builder executionStepBuilder;

    /**
     * Create a new Execution step. Initializes start time to {@link LocalDateTime#now()},
     * errors to empty, and status to QUEUED.
     *
     * @param targetId The ID of the target where this step of the action will be executed.
     */
    public ExecutableStep(long targetId) {
        executionStepBuilder = ExecutionStep.newBuilder()
            .setEnqueueTime(System.currentTimeMillis())
            .setStatus(Status.QUEUED)
            .setTargetId(targetId)
            .setProgressDescription(INITIAL_QUEUED_DESCRIPTION);
    }

    /**
     * Private helper to restore from a built execution step.
     *
     * @param step The step from which to restore state.
     */
    private ExecutableStep(@Nonnull final ExecutionStep step) {
        executionStepBuilder = step.toBuilder();
    }

    /**
     * Get the target ID for the execution step.
     *
     * @return the target ID for the execution step.
     */
    public long getTargetId() {
        return executionStepBuilder.getTargetId();
    }

    /**
     * Get the execution step backing this executable step.
     *
     * @return The execution step backing this executable step.
     */
    public ExecutionStep getExecutionStep() {
        return executionStepBuilder.build();
    }

    /**
     * Create an {@link ExecutableStep} from a built {@link ExecutionStep}.
     *
     * @param step The step from which to build.
     * @return A mutable {@link ExecutableStep} wrapping the mutable input {@link ExecutionStep}.
     */
    public static ExecutableStep fromExecutionStep(@Nullable ExecutionStep step) {
        return (step == null) ? null : new ExecutableStep(step);

    }

    /**
     * Set the status of the action step to be IN_PROGRESS.
     * As a side-effect, the start time will be set to now().
     *
     * @return A reference to {@link this} to support method chaining.
     */
    @Nonnull
    public ExecutableStep execute() {
        if (getStatus() != Status.QUEUED) {
            throw new IllegalStateException("Illegal transition from " + getStatus() + " to " + Status.IN_PROGRESS);
        }

        executionStepBuilder
            .setStatus(Status.IN_PROGRESS)
            .setStartTime(System.currentTimeMillis())
            .setProgressPercentage(0)
            .setProgressDescription(INITIAL_EXECUTION_DESCRIPTION);

        return this;
    }

    /**
     * Set the status of the action step to be successful.
     * As a side-effect, the completion time will be set to now().
     *
     * @return A reference to {@link this} to support method chaining.
     */
    @Nonnull
    public ExecutableStep success() {
        if (getStatus() == Status.FAILED) {
            throw new IllegalStateException("Illegal transition from " + getStatus() + " to " + Status.SUCCESS);
        }

        executionStepBuilder
            .setStatus(Status.SUCCESS)
            .setCompletionTime(System.currentTimeMillis())
            .setProgressPercentage(100)
            .setProgressDescription(SUCCESS_DESCRIPTION);

        return this;
    }

    /**
     * Set the status of the operation to be failure.
     * As a side-effect, the completion time will be set to now().
     *
     * @return A reference to {@link this} to support method chaining.
     */
    @Nonnull
    public ExecutableStep fail() {
        if (getStatus() == Status.SUCCESS) {
            throw new IllegalStateException("Illegal transition from " + getStatus() + " to " + Status.FAILED);
        }

        executionStepBuilder
            .setStatus(Status.FAILED)
            .setCompletionTime(System.currentTimeMillis())
            .setProgressDescription(FAILURE_DESCRIPTION);

        return this;
    }

    /**
     * Add an error.
     * Note that Status is NOT automatically transitioned to FAILED when errors are added.
     * The fail() method must be called independently to set status to FAILED if the error
     * has caused the action to fail.
     *
     * @param error The error to add.
     * @return A reference to {@link this} to support method chaining.
     */
    @Nonnull
    public ExecutableStep addError(@Nonnull final String error) {
        executionStepBuilder.addErrors(error);
        return this;
    }

    /**
     * Add a list of errors. The errors will be translated to human readable strings from the DTOs.
     * Note that Status is NOT automatically transitioned to FAILED when errors are added.
     * The fail() method must be called independently to set status to FAILED if the error
     * has caused the operation to fail.
     *
     * @param errors The errors to add.
     * @return A reference to {@link this} to support method chaining
     */
    @Nonnull
    public ExecutableStep addErrors(@Nonnull final List<String> errors) {
        executionStepBuilder.addAllErrors(errors);
        return this;
    }

    @Nonnull
    public List<String> getErrors() {
        return executionStepBuilder.getErrorsList();
    }

    /**
     * Get the status of the operation.
     *
     * @return The status of the operation.
     */
    @Nonnull
    public Status getStatus() {
        return executionStepBuilder.getStatus();
    }

    @Nonnull
    public Optional<LocalDateTime> getEnqueueTime() {
        return toLocalDateTime(executionStepBuilder.hasEnqueueTime(), executionStepBuilder.getEnqueueTime());
    }

    @Nonnull
    public Optional<LocalDateTime> getStartTime() {
        return toLocalDateTime(executionStepBuilder.hasStartTime(), executionStepBuilder.getStartTime());
    }

    @Nonnull
    public Optional<LocalDateTime> getCompletionTime() {
        return toLocalDateTime(executionStepBuilder.hasCompletionTime(), executionStepBuilder.getCompletionTime());
    }

    public Optional<Integer> getProgressPercentage() {
        return executionStepBuilder.hasProgressPercentage() ?
            Optional.of(executionStepBuilder.getProgressPercentage()) :
            Optional.empty();
    }

    public void updateProgress(int progressPercentage, @Nonnull final String progressDescription) {
        setProgressPercentage(progressPercentage);
        setProgressDescription(progressDescription);
    }

    /**
     * Set the progress percentage.
     *
     * @param progressPercentage The percentage complete. Should be a value between 0 and 100 inclusive.
     * @throws IllegalArgumentException if progressPercentage < 0 || progressPercentage > 100.
     * @throws IllegalStateException if the action is already complete.
     */
    public void setProgressPercentage(int progressPercentage) {
        if (progressPercentage < 0 || progressPercentage > 100) {
            throw new IllegalArgumentException("Invalid progressPercentage: " + progressPercentage);
        }
        if (getStatus() != Status.IN_PROGRESS) {
            throw new IllegalStateException(
                "Illegal attempt to update progress when ExecutableStep is in state " + getStatus() + "."
            );
        }

        executionStepBuilder.setProgressPercentage(progressPercentage);
    }

    @Nonnull
    Optional<LocalDateTime> toLocalDateTime(boolean hasField, long epochMillis) {
        return hasField ?
            Optional.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC)) :
            Optional.empty();
    }

    public Optional<String> getProgressDescription() {
        return executionStepBuilder.hasProgressDescription() ?
            Optional.of(executionStepBuilder.getProgressDescription()) :
            Optional.empty();
    }

    public void setProgressDescription(@Nullable final String progressDescription) {
        executionStepBuilder.setProgressDescription(progressDescription);
    }

    /**
     * Compute the execution time for this step. If the step is incomplete, will return {@link Optional#empty()}
     *
     * @return The execution time for this step.
     *         If the step is incomplete, will return {@link Optional#empty()}
     */
    public Optional<Long> getExecutionTimeSeconds() {
        return getCompletionTime().flatMap(completionTime ->
            getEnqueueTime().map(enqueueTime ->
                ChronoUnit.SECONDS.between(completionTime, enqueueTime)));
    }

    @Override
    public String toString() {
        return "ExecutableStep{" + executionStepBuilder.getStatus() + ": "
                + executionStepBuilder.getProgressDescription() + "}";
    }
}
