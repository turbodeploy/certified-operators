package com.vmturbo.mediation.actionscript.executor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.protobuf.util.JsonFormat;

import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.session.ConnectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vmturbo.mediation.actionscript.ActionScriptProbeAccount;
import com.vmturbo.mediation.actionscript.SshUtils.RemoteCommand;
import com.vmturbo.mediation.actionscript.executor.ActionScriptExecutor.CompletionInfo;
import com.vmturbo.mediation.actionscript.parameter.ActionScriptParameterMapper;
import com.vmturbo.mediation.actionscript.parameter.ActionScriptParameterMapper.ActionScriptParameter;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;

/**
 * Class to manage execution of a script in an established SSH session.
 *
 * <p>We launch the remote command, handle timeouts and grace periods, keep track of output, and
 * post progress updates as output is collected. (We don't currently have a way to post percentage
 * completed progress updates.)</p>
 *
 * <p>We make use of {@link SignalingSshChannelExec}, which is an extension of
 * {@link org.apache.sshd.client.channel.ChannelExec} that can send signals to the remote
 * process.</p>
 */
class SshScriptExecutor implements RemoteCommand<CompletionInfo> {
    public static final String STDOUT_NAME = "STDOUT";
    public static final String STDERR_NAME = "STDERR";
    private static Logger logger = LoggerFactory.getLogger(SshScriptExecutor.class);

    private ActionScriptExecutor actionScriptExecutor;

    SshScriptExecutor(final ActionScriptExecutor actionScriptExecutor) {
        this.actionScriptExecutor = actionScriptExecutor;
    }

    private static final String ENV_VAR_FORMAT = "%s=%s";
    private static final Pattern ENV_VAR_NAME_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");

    /**
     * Execute an action script via SSH.
     *
     * @param accountValues   {@link ActionScriptProbeAccount} values from the target
     * @param session         {@link ClientSession} over which the code will be executed
     * @param actionExecution {@link ActionExecutionDTO} defining the action being performed.
     * @return the resulting {@link CompletionInfo} object
     */
    public CompletionInfo execute(final @Nonnull ActionScriptProbeAccount accountValues,
                                  final @Nonnull ClientSession session,
                                  final @Nonnull ActionExecutionDTO actionExecution) {
        Workflow workflow = Objects.requireNonNull(actionExecution).getWorkflow();
        CompletionInfo completion = new CompletionInfo();

        // make sure the parameter names will be accepable as environment variable naems
        Set<ActionScriptParameter> params = ActionScriptParameterMapper.mapParameterValues(actionExecution, workflow.getParamList());
        final String badParamNames = params.stream().map(p -> p.getName())
            .filter(name -> !ENV_VAR_NAME_PATTERN.matcher(name).matches())
            .collect(Collectors.joining(" "));
        if (!badParamNames.isEmpty()) {
            logger.warn("Action script remote execution failed due to invalid parameter names: " + badParamNames);
            completion.setException(new IllegalArgumentException(
                    "Invalid parameter names for action script execution: " + badParamNames));
        } else {
            // We can't reliably use the setEnv() method of the SSH channel object that will manage the
            // execution, because most sshd servers are configured to discard environment variables
            // defined via the SSH channel protocol (with the exception of TERM). So instead, we preface
            // the command line with VAR=VALUE pairs that should be understood by an underlying shell
            // on the remote end.
            // N.B. This will not work if the remote shell does not understand this syntax - e.g. csh
            // and variants like tcsh, and zsh. The Bourne and Bourne-again shells (sh and bash) will
            // work.
            final List<String> envSettings = params.stream()
                .map(p -> String.format(ENV_VAR_FORMAT, p.getName(), quoteForSsh(p.getValue())))
                .collect(Collectors.toList());
            final String quotedScriptPath = quoteForSsh(workflow.getScriptPath());
            final String commandString = String.join(" ", envSettings) + " " + quotedScriptPath;
            final String actionExecutionDTOJson = convertToCompactJson(actionExecution);
            try (
                // channel for command execution
                SignalingSshChannelExec channel = new SignalingSshChannelExec(commandString);
                // create pipe and handler for stdout
                PipedOutputStream stdout = new PipedOutputStream();
                OutputHandler stdoutHandler = new OutputHandler(stdout, actionScriptExecutor.getMaxOutputLines(), STDOUT_NAME);
                // and monitor it so we can update progress with latest output periodically
                ProgressOutputUpdater progressOutputUpdater = new ProgressOutputUpdater(
                    actionScriptExecutor.getProgressTracker(), stdoutHandler,
                    actionScriptExecutor.getProgressOutputUpdateIntervalSeconds());
                // create pipe and handler for stderr
                PipedOutputStream stderr = new PipedOutputStream();
                OutputHandler stderrHandler = new OutputHandler(stderr, actionScriptExecutor.getMaxOutputLines(), STDERR_NAME);
                // use stdin to pass the whole ActionExecutionDTO json string (rather than using
                // command line) to avoid the MAX_ARG_STRLEN limited by action script server
                InputStream stdin = new ByteArrayInputStream(actionExecutionDTOJson.getBytes())
            ) {
                // configure and start our channel
                session.getService(ConnectionService.class).registerChannel(channel);
                channel.setIn(stdin);
                channel.setOut(stdout);
                channel.setErr(stderr);
                // opening the channel is what actually starts the remote command execution
                channel.open().verify();
                handleChannelEvents(channel, workflow);
                // command has terminated; capture info and make sure we grab any in-flight output
                completion.setExitStatus(channel.getExitStatus());
                completion.setExitSignal(channel.getExitSignal());
                stdoutHandler.finish(stdout);
                completion.setStdout(stdoutHandler.assembleOutput(true));
                stderrHandler.finish(stderr);
                completion.setStderr(stderrHandler.assembleOutput(true));
            } catch (IOException e) {
                logger.warn("Action Script remote execution terminated with exception", e);
                completion.setException(e);
            }
        }
        return completion;
    }

    private static final String APOSTROPHE = "'";
    private static final String EMBEDDED_APOSTROPHE = "'\"'\"'";

    /**
     * Prepare a string for use in a command line sent to SSH. The overall string is quoted with
     * apostrophes. Embedded apostrophes are replaced with this funky looking chunk: '"'"'
     * That's an apostrophe that ties off the current single-quoted string, followed immediately
     * by a double-quoted apostrophe, and then finally an apostrophe to open a new single-quoted
     * string. All are concatenated by the shell command line processing on the SSH server.
     *
     * @param s string to be quoted
     * @return fully quoted string
     */
    private String quoteForSsh(@Nonnull String s) {
        return APOSTROPHE + s.replaceAll(APOSTROPHE, EMBEDDED_APOSTROPHE) + APOSTROPHE;
    }

    private static final long UNLIMITED_WAIT = -1;

    /**
     * Translate a time limit into a deadline for execution to complete. We'll compute timeouts
     * based on this and current time in the channel handler.
     *
     * @param workflow {@link Workflow} that defines the actions script
     * @return deadline (milliseconds since epoch) based on script's time limit and current time
     */
    private long getMaxEndTime(final @Nonnull Workflow workflow) {
        final long limitInSeconds = getWorkflowTimeLimitSecs(workflow);
        return limitInSeconds < 0L ? UNLIMITED_WAIT : System.currentTimeMillis() + limitInSeconds * 1000;
    }

    private long getWorkflowTimeLimitSecs(@Nonnull final Workflow workflow) {
        return workflow.hasTimeLimitSeconds()
                ? workflow.getTimeLimitSeconds()
                : actionScriptExecutor.getDefaultTimeoutSeconds();
    }


    /**
     * channel events that we'll be watching for in {@link #handleChannelEvents(SignalingSshChannelExec, Workflow)}
     *
     * <p>The "events" are not really events at all - they're persistent properties of the channel. E.g. if you include
     * the OPENED event in your waitFor list, then once the channel enters OPENED state, you'll never wait, and an
     * OPENED "event" will always be reported until the channel becomes closed.</p>
     *
     * <p>What we're really after is to know when the channel becomes closed, and that corresponds to the CLOSED "event".
     * We're not interested in intermediate transitions, like EXIT_STATUS that indicates that an exit status has been
     * set. Once it's set, we can retrieve thereafter from the channel, so we don't need to know when it actually
     * happens.</p>
     *
     * <p>The TIMEOUT value of ClientChannelEvent is the only value that actually acts like an event,
     * but it's not necessary to include it in the waitFor list - it'll be delivered if your wait
     * times out, regardless.</p>
     *
     * <p>That's why our waitFor list ends up only including CLOSED.</p>
     */
    private static final Collection<ClientChannelEvent> EVENTS_OF_INTEREST = Collections.singletonList(ClientChannelEvent.CLOSED);

    /**
     * Loop, handling events reported by the SSH channel, until a terminating event is
     * handled.
     *
     * @param channel  the channel whose events we're monitoring
     * @param workflow the workflow that's executing
     */
    private void handleChannelEvents(@Nonnull SignalingSshChannelExec channel,
            @Nonnull Workflow workflow) {
        // deadline for execution to complete
        long maxEndTime = getMaxEndTime(workflow);
        // true once we've gone past our deadline
        boolean timedOut = false;
        // true immediately after timeout, and until a subsequent grace period expires (waiting
        // for script to terminate after sending it a SIGTERM signal)
        boolean inGracePeriod = false;
        // true when the execution has terminated
        boolean done = false;
        while (!done) {
            long timeLimit = computeTimeLimit(maxEndTime, timedOut, inGracePeriod);
            if (timeLimit <= 0) {
                // this can happen for a few different reasons....
                if (inGracePeriod) {
                    // we were in grace period, but we've exceeded it
                    inGracePeriod = false;
                    timeLimit = UNLIMITED_WAIT; // no more timeouts after grace period
                    logger.warn("Action script {} did not terminate within grace period after " +
                                    "being sent SIGTERM signal; no further attempt will be made " +
                                    "to terminate the script",
                            workflow.getDisplayName());
                    // TODO Notify that we timed out and process did not terminate quickly
                } else if (!timedOut && maxEndTime > 0) {
                    // we've just hit our deadline - signal the process
                    try {
                        logger.warn("Action script {} exceeded time limit of {} seconds, " +
                                        "sending SIGTERM signal",
                                workflow.getDisplayName(), getWorkflowTimeLimitSecs(workflow));
                        channel.signal("TERM");
                    } catch (IOException e) {
                        logger.warn("Apparently failed to signal action script process with termination signal");
                        // not a big deal... will wait out the grace period and notify normally
                    }
                    timedOut = true;
                    inGracePeriod = true;
                    // recompute time limit for grace period
                    timeLimit = computeTimeLimit(maxEndTime, timedOut, inGracePeriod);
                } else {
                    // either we're timed out and beyond grace period, or our script was
                    // configured for no time limit, so don't time out from now on
                    timeLimit = UNLIMITED_WAIT;
                }
            }

            final Collection<ClientChannelEvent> fired = channel.waitFor(EVENTS_OF_INTEREST, timeLimit);
            // Note that there are ClientChannelEvent values defined for stdout and stderr activity, but
            // they do not appear to be sent for ChannelExec (from which ours is derived). Instead,
            // we handle output by monitoring pipes that we have established (see OutputHandler class)
            for (ClientChannelEvent event : fired) {
                switch (event) {
                    case CLOSED:
                        done = true;
                        break;
                    case TIMEOUT:
                        // don't need to do anything here - new timeLimit will be negative,
                        // and logic above will do needed state transitions
                        break;
                    default:
                        // nothing else interests us
                        break;
                }
            }
        }
    }

    /**
     * Compute timeout for next event wait.
     *
     * @param maxEndTime    deadline for script termination
     * @param timedOut      whether we've already hit that deadline
     * @param inGracePeriod whether we're still in grace period after hitting deadline
     * @return appropriate time limit for next cycle
     */
    private long computeTimeLimit(long maxEndTime, boolean timedOut, boolean inGracePeriod) {
        if (timedOut) {
            return inGracePeriod
                // grace period extends past initial max end time
                ? maxEndTime + actionScriptExecutor.getGracePeriodSeconds() * 1000 - System.currentTimeMillis()
                // once we're beyond grace period we just wait for other events without timing out
                : -1;
        } else {
            return maxEndTime - System.currentTimeMillis();
        }
    }

    /**
     * Convert the given {@link ActionExecutionDTO} to compacted json string.
     *
     * @param actionExecutionDTO the ActionExecutionDTO to convert
     * @return compacted json string, or empty string if any error happens
     */
    public static String convertToCompactJson(@Nonnull ActionExecutionDTO actionExecutionDTO) {
        try {
            return JsonFormat.printer().omittingInsignificantWhitespace().print(actionExecutionDTO);
        } catch (Exception e) {
            // if any error happens, return an empty string
            logger.error("Failed to convert ActionExecutionDTO to json: {}", actionExecutionDTO, e);
            return "";
        }
    }
}
