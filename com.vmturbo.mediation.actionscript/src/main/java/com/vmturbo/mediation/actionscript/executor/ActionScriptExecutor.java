package com.vmturbo.mediation.actionscript.executor;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.mediation.actionscript.ActionScriptProbeAccount;
import com.vmturbo.mediation.actionscript.SshUtils;
import com.vmturbo.mediation.actionscript.exception.RemoteExecutionException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.sdk.probe.IProgressTracker;

/**
 * Execute an ActionScript workflow.
 *
 * <p>The Workflow object includes an absolute path to the script executable. We invoke the script
 * with no command line arguments, using SSH, on the execution host specified in the associated
 * target. Any parameter values configured into the workflow object are are supplied via environment
 * variables. Workflow "property" objects are ignored.</p>
 *
 * <p>Features of the execution include:</p>
 * <li>Execution is limited to a maximum duration (elapsed time) configured for the script.
 * If none is specified, a global default is used. If a negative limit is provided, the script
 * execution is not limited.
 * </li>
 * <li>A script that exceeds its execution time limit causes the following to occur:
 * <ul>
 * <li>
 * The script is sent a SIGTERM signal, which will terminate the execution unless the
 * script executable has been designed to trap SIGTERM. In that case the script should
 * terminate at the earliest safe moment.
 * </li>
 * <li>The script will be given a short grace period in which to terminate.</li>
 * <li>
 *     If the script does not terminate by the end of the grace period, it is currently left
 *     running; in the future we will probably provide alternatives for this situation.
 *     <p>
 *         Note that per the above, a script that does not terminate will prevent the probe's
 *         {@link #execute()} action from returning, and that will keep the action thread used for the
 *         script execution tied up until the script is terminated in some other way (e.g. manual
 *         intervention on the execution server).
 *     </p>
 * </li>
 * <li>
 *     An {@link IProgressTracker} is updated with progress during execution. Currently there is no
 *     provision for meaningful progress metrics; for now, we set progress to 50% when the script
 *     begins execution, and 100% when it completes.
 *     * <p>During execution, the progress tracker is periodically updated so that its text field
 *     contains the most recent output sent to stdout by the script</p>
 * </li>
 * </li>
 * </ul>
 * </ul>
 **/
public class ActionScriptExecutor {
    private static final Logger logger = LogManager.getLogger(ActionScriptExecutor.class);

    // Following fields come in pairs, some with a package-private getter method. The constants
    // serve as defaults, and there's currently no means of deviating from them (other than the
    // timeout value) in normal execution. However, unit tests with default values would run for
    // hours, so we have corresponding non-final fields that are initialized to the defaults but
    // can be altered by tests. Getters are provided for fields needed by other classes. Setters
    // and other methods intended for unit tests are consolidated in the enclosed TestAccess class.

    // maximum time a script is permited to run before an attempt is made to terminate it by sending
    // it a SIGTERM signal
    private static final int DEFAULT_TIMEOUT_SECONDS_DEFAULT = 30 * 60;

    int getDefaultTimeoutSeconds() {
        return DEFAULT_TIMEOUT_SECONDS_DEFAULT;
    }

    // short period of time that we wait for a script to actually terminate after sending it
    // SIGTERM. We currently take no further action after the grace period if the script is still
    // going, but this way a compliant script can be reported as completed.
    private static final int GRACE_PERIOD_SECONDS_DEFAULT = 10;
    private int gracePeriodSeconds = GRACE_PERIOD_SECONDS_DEFAULT;

    int getGracePeriodSeconds() {
        return gracePeriodSeconds;
    }

    // maximum number of output lines (stdout or stderr) to retain for an action script execution.
    // new output ejects older output irretrievably.
    private static final int MAX_OUTPUT_LINES_DEFAULT = 10;
    private int maxOutputLines = MAX_OUTPUT_LINES_DEFAULT;

    int getMaxOutputLines() {
        return maxOutputLines;
    }

    // minimum interval between progress updates that only report new output
    private static final int PROGRESS_OUTPUT_UPDATE_INTERVAL_SECONDS_DEFAULT = 60;
    private int progressOutputUpdateIntervalSeconds = PROGRESS_OUTPUT_UPDATE_INTERVAL_SECONDS_DEFAULT;

    int getProgressOutputUpdateIntervalSeconds() {
        return progressOutputUpdateIntervalSeconds;
    }

    // ActionScriptProbeAccount value from target that defined the executing action script
    private final ActionScriptProbeAccount accountValues;
    // Action for which this action script is executing
    private final ActionExecutionDTO actionExecution;
    // progress tracker for reporting progress
    private final IProgressTracker progressTracker;

    IProgressTracker getProgressTracker() {
        return progressTracker;
    }

    // results of execution - remains null until the action script has terminated
    private CompletionInfo completionInfo = null;

    /**
     * Create a new {@link ActionScriptExecutor} instance; does not cause script to be scheduled for execution.
     *
     * @param accountValues   the {@link ActionScriptProbeAccount} from target configuration
     * @param actionExecution the {@link ActionExecutionDTO} containing the {@link Workflow} to be executed
     * @param progressTracker the {@link IProgressTracker} to which progress updates are made
     */
    public ActionScriptExecutor(final @Nonnull ActionScriptProbeAccount accountValues,
                                final @Nonnull ActionExecutionDTO actionExecution,
                                final @Nonnull IProgressTracker progressTracker) {
        this.accountValues = Objects.requireNonNull(accountValues);
        this.actionExecution = Objects.requireNonNull(actionExecution);
        this.progressTracker = Objects.requireNonNull(progressTracker);
    }

    /**
     * Create a new dummy instance that cannot be executed; this is only used by some unit tests.
     */
    @VisibleForTesting
    ActionScriptExecutor() {
        // dummy non-null values
        this.accountValues = new ActionScriptProbeAccount();
        this.actionExecution = ActionExecutionDTO.newBuilder()
            .setActionType(ActionType.MOVE)
            .build();
        this.progressTracker = (actionResponseState, s, i) -> {
        };
    }

    /**
     * Run the script and report its final status
     *
     * @return final {@link ActionScriptExecutionStatus} value
     */
    public ActionScriptExecutionStatus execute() {
        final CompletionInfo completionInfo = runScript();
        return computeStatus();
    }

    /**
     * Run the script on the execution host using SSH.
     *
     * @return the {@link CompletionInfo} that indicates how the execution finished
     */
    private CompletionInfo runScript() {
        try {
            // special support for unit tests, which may (via TestAccess class) inject an excpetion that should be thrown during
            // script execution. We can't easily construct a scenario that causes such an exception without an intrusion like this,
            // and this one has the benefit of being quite obvious and focused. This will never happen during normal use
            if (testAccess.sabotageException != null) {
                throw new RemoteExecutionException("Execution sabotaged for testing", testAccess.sabotageException);
            }
            // for the duration of the execution we'll report 50% cuz we don't yet have a way to know better
            progressTracker.updateActionProgress(ActionResponseState.IN_PROGRESS, "ActionScript execution in progress", 50);
            // start and run an SSH channel to execute the script, and capture its completionInfo information
            this.completionInfo = SshUtils.runInSshSession(accountValues, actionExecution, new SshScriptExecutor(this));
        } catch (Exception e) {
            // here if we weren't able to execute the script (or if we were sabotaged in a unit test)
            this.completionInfo = new CompletionInfo();
            completionInfo.setException(e);
            // following is required so computeCurrentState works correctly in this case
        }
        // script has finished... send a final progress update
        updateFinalProgress();
        return completionInfo;
    }

    /**
     * Compute current status of the script execution
     *
     * @return current {@link ActionScriptExecutionStatus} value
     */
    private ActionScriptExecutionStatus computeStatus() {
        if (completionInfo != null) {
            // a completed task is considered successful if the script terminated with a zero
            // exit status. If it task was terminated by an exception, we report ERROR
            // status. In all other cases, we report FAILED.
            if (completionInfo.getException() != null) {
                return ActionScriptExecutionStatus.ERROR;
            } else if (completionInfo.getExitSignal() != null || (completionInfo.getExitStatus() != null && completionInfo.getExitStatus() != 0)) {
                return ActionScriptExecutionStatus.FAILED;
            } else {
                return ActionScriptExecutionStatus.COMPLETE;
            }
        } else {
            // execution has started but not yet completed
            return ActionScriptExecutionStatus.RUNNING;
        }
    }

    /**
     * Here we send a final progress update when the script has finished. This will include a
     * response state code based on the outcome, as well as the final tail of either stdout or stderr,
     * depending on whether the script was or was not successful.
     *
     */
    private void updateFinalProgress() {
        ActionResponseState finalState;
        String description;
        switch (computeStatus()) {
            case COMPLETE:
                // successful execution
                finalState = ActionResponseState.SUCCEEDED;
                description = computeFinalDescription("Action Script completed execution", false, completionInfo.getStdout());
                break;
            case CANCELED:
            case CANCELED_FROM_QUEUE:
                // execution canceled, either before or after it started executing.
                // (Actually, we currently don't support canceling any scripts, so this will never
                // be reported until support is added
                finalState = ActionResponseState.FAILED;
                description = computeFinalDescription("Action Script was canceled", true, completionInfo.getStdout());
                break;
            case FAILED:
            case ERROR:
                // either the script failed, or we failed to launch it
                finalState = ActionResponseState.FAILED;
                description = computeFinalDescription("Action Script execution failed", true, completionInfo.getStderr());
                break;
            default:
                // all other status values are non-terminal, and we should not be here!
                throw new IllegalStateException("Cannot compute final status for an action script that has not completed");
        }
        progressTracker.updateActionProgress(finalState, description, 100);
    }

    /**
     * Here we construct a "description" string for the final progress update.
     *
     * @param message        Beginning of constucted text; we append the supplied output if it's not empty
     * @param outputIsStderr true of the supplied output is stdout; else it must be stderr
     * @param output         output to append the message
     * @return constructed text for the progress update
     */
    private String computeFinalDescription(final @Nonnull String message,
                                           final boolean outputIsStderr,
                                           final @Nonnull String output) {
        if (output != null && !output.isEmpty()) {
            String messageTail = String.format("; tail of %s:\n", outputIsStderr ? "error output" : "output");
            return message + messageTail + output;
        } else {
            return message;
        }
    }


    /**
     * POJO to capture completionInfo information after a script has terminated
     */
    public static class CompletionInfo {
        // exit status reported by SSH, if the script terminated voluntarily
        private Integer exitStatus = null;
        // name of signal reported by SSH, if the script terminated because it received an untrapped
        // signal
        private String exitSignal = null;
        // exception that prevented the script from being executed, or prevented us from monitoring
        // the execution after it started (i.e. some error condition in our execution framework, not
        // an error encountered by the script itself)
        private Throwable exception = null;
        // most recent output sent ot stdout by the script
        private String stdout = null;
        // most recent output sent to stderr by the script
        private String stderr = null;

        /**
         * Return the exit status reported by the remote script execution
         *
         * @return exit status, or null if the script terminated in some other way
         */
        Integer getExitStatus() {
            return exitStatus;
        }

        void setExitStatus(final Integer exitStatus) {
            this.exitStatus = exitStatus;
        }

        /**
         * Return the signal that caused the script to terminate, if that's what happened.
         *
         * @return signal name (minus the initial "SIG" prefix) that caused the script to terminate, or null if that's not what happened
         */
        String getExitSignal() {
            return exitSignal;
        }

        void setExitSignal(final String exitSignal) {
            this.exitSignal = exitSignal;
        }

        /**
         * Return the exception that caused script execution to fail, if that's what happened.
         *
         * <p>This is an exception thrown by the action script machinery while attempting to manage
         * execution of the remote script. It is NOT an exception experienced by the remote script
         * (even if that happens to be a Java program). In this case, the final execution status of
         * the script is unknowable.</p>
         *
         * @return exception that terminated our attempt to run the script, or null if that's not what happened
         */
        public Throwable getException() {
            return exception;
        }

        void setException(final Throwable exception) {
            this.exception = exception;
        }

        /**
         * Return the most recent complete lines of output sent to stdout by the script
         *
         * @return most recent output
         */
        String getStdout() {
            return stdout;
        }

        void setStdout(final String stdout) {
            this.stdout = stdout;
        }

        /**
         * Return the most recent complete lines of output sent to stderr by the script
         *
         * @return most recent error output
         */
        String getStderr() {
            return stderr;
        }

        void setStderr(final String stderr) {
            this.stderr = stderr;
        }
    }

    // Object that unit tests can use to manipulate the executor in ways that are not available in
    // normal use
    private TestAccess testAccess = this.new TestAccess();

    /**
     * Obtain a TestAccess object to manipulate this {@link ActionScriptExecutor} instance
     *
     * @return test access instance
     */
    @VisibleForTesting
    TestAccess getTestAccess() {
        return testAccess;
    }

    /**
     * This class encapsulates a number of "hooks" used by unit tests to control various parameters
     * of the executor that are not normally configurable.
     *
     * <p>This mostly involves timing-related parameters, since with the defaults, the unit tests would
     * run for hours.</p>
     *
     * <p>A handful of other getters are provided so unit tests can peek at internal state.</p>
     */
    @VisibleForTesting
    class TestAccess {

        private ActionScriptExecutor executor = ActionScriptExecutor.this;

        Optional<CompletionInfo> getCompletionInfo() {
            return Optional.ofNullable(ActionScriptExecutor.this.completionInfo);
        }

        /**
         * Compute the current {@link ActionScriptExecutionStatus} for this execution
         *
         * @return current status
         */
        ActionScriptExecutionStatus getCurrentStatus() {
            return computeStatus();
        }

        /**
         * Set a non-default grace period for scripts that exceed their time limit.
         *
         * @param gracePeriodSeconds grace period in seconds
         */
        void setGracePeriodSeconds(final int gracePeriodSeconds) {
            executor.gracePeriodSeconds = gracePeriodSeconds;
        }

        /**
         * Set a non-default minimum interval between progress updates triggered by new output
         *
         * @param updateIntervalSeconds minimum report interval
         */
        void setProgressOutputUpdateIntervalSeconds(final int updateIntervalSeconds) {
            executor.progressOutputUpdateIntervalSeconds = updateIntervalSeconds;
        }

        /**
         * Set a non-defaul limit on the number of lines of output to be retained for stdout and stderr.
         *
         * @param maxOutputLines max number of retained lines
         */
        void setMaxOutputLines(final int maxOutputLines) {
            executor.maxOutputLines = maxOutputLines;
        }

        /**
         * Get the display name of the action script
         *
         * @return script name
         */
        String getScriptName() {
            return actionExecution.getWorkflow().getDisplayName();
        }

        private Throwable sabotageException = null;

        /**
         * Supply an exception to be thrown during what would normally be the attempted execution of
         * the action script.
         *
         * @param failureException exception to throw
         */
        void setSabotageExecution(final Throwable failureException) {
            this.sabotageException = failureException;
        }
    }
}
