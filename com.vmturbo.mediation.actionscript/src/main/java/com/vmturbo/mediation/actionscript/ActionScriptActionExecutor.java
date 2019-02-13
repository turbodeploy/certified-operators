package com.vmturbo.mediation.actionscript;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.bouncycastle.util.io.TeeOutputStream;

import com.vmturbo.mediation.actionscript.exception.KeyValidationException;
import com.vmturbo.mediation.actionscript.exception.RemoteExecutionException;
import com.vmturbo.mediation.actionscript.parameter.ActionScriptParameterMapper;
import com.vmturbo.mediation.actionscript.parameter.ActionScriptParameterMapper.ActionScriptParameter;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.Parameter;
import com.vmturbo.platform.sdk.probe.ActionResult;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.IProgressTracker;

/**
 * Execute an ActionScript when requested. The execution will name a workflow, and the name of the
 * workflow specifies a unix command line script to be run from the base folder.
 * <p/>
 * The return code from the script execution will indicate success (0) or failure (!0).
 * <p/>
 * We will need to decide what progress messages will be returned.
 *
 * TODO: implement the script execution, status update, and 'destroy()' functionality.
 **/
public class ActionScriptActionExecutor implements AutoCloseable  {

    private static final Logger logger = LogManager.getLogger();

    private static final Pattern VALID_BASH_VARIABLE_NAME_REGEX = Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]*");

    private final IProbeContext probeContext;

    public ActionScriptActionExecutor(final IProbeContext probeContext) {
        this.probeContext = probeContext;
    }

    @Override
    public void close() {
        // TODO: implement close for this action - may be in progress
    }

    @Nonnull
    public ActionResult executeAction(@Nonnull final ActionExecutionDTO actionExecutionDTO,
                                      @Nonnull final ActionScriptProbeAccount actionScriptProbeAccount,
                                      @Nonnull final IProgressTracker iProgressTracker)
        throws InterruptedException {

        if (actionExecutionDTO.hasWorkflow()) {
            executeWorkflow(actionExecutionDTO, actionScriptProbeAccount);

            // TODO: implement the script execution with an async status update and 'wait for completion'
            // for now just indicate that the operation succeeded
            iProgressTracker.updateActionProgress(ActionResponseState.SUCCEEDED, "Done", 100);

            return new ActionResult(ActionResponseState.SUCCEEDED, "Done");
        } else {
            final String errorMessage = "ActionExecutionDTO does not have a workflow associated";
            logger.error(errorMessage + " - not done: {}");

            return new ActionResult(ActionResponseState.FAILED, errorMessage);
        }
    }

    private static ActionResult executeWorkflow(@Nonnull final ActionExecutionDTO actionExecutionDTO,
            @Nonnull final ActionScriptProbeAccount actionScriptProbeAccount) {
        // TODO: Convert method to execute asynchronously
//        FutureTask actionFutureTask = new FutureTask(() -> {
//            return new ActionResult(ActionResponseState.SUCCEEDED, "Action completed.");
//        });
        try {
            final boolean scriptSucceeded = SshUtils.executeRemoteCommand(actionScriptProbeAccount,
                ActionScriptActionExecutor::invokeActionScript,
                actionExecutionDTO);
            final ActionResponseState responseState = scriptSucceeded ?
                ActionResponseState.SUCCEEDED :
                ActionResponseState.FAILED;
            return new ActionResult(responseState, "ActionScript execution completed.");
        } catch (KeyValidationException | RemoteExecutionException e) {
            final String message = String.format("Action %s failed: %s ",
                actionExecutionDTO.getActionOid(),
                e.getMessage());
            logger.error(message, e);
            return new ActionResult(ActionResponseState.FAILED, message);
        }
    }

    private static boolean invokeActionScript(@Nonnull final ActionScriptProbeAccount accountValues,
                                              @Nonnull final ClientSession session,
                                              @Nonnull final ActionExecutionDTO actionExecutionDTO)
            throws RemoteExecutionException {
        // Get an SSH client channel to send/receive data over SSH
        try (ClientChannel channel = session.createShellChannel();
             ByteArrayOutputStream sent = new ByteArrayOutputStream();
             PipedOutputStream pipedIn = new PipedOutputStream();
             PipedInputStream pipedOut = new PipedInputStream(pipedIn)) {
            channel.setIn(pipedOut);

            try (OutputStream teeOut = new TeeOutputStream(sent, pipedIn);
                 ByteArrayOutputStream out = new ByteArrayOutputStream();
                 ByteArrayOutputStream err = new ByteArrayOutputStream()) {

                channel.setOut(out);
                channel.setErr(err);
                channel.open();

                final NonMarketEntityDTO workflow = actionExecutionDTO.getWorkflow();

                // Load the ActionScript parameters into BASH environment variables
                final List<Parameter> paramList = workflow.getWorkflowData().getParamList();
                final Set<ActionScriptParameter> actionScriptParameters =
                    ActionScriptParameterMapper.mapParameterValues(actionExecutionDTO, paramList);
                for (ActionScriptParameter parameter : actionScriptParameters) {
                    final String parameterName = parameter.getName();
                    final String parameterValue = parameter.getValue();
                    // Only include valid name/value pairs.
                    if (isValidName(parameterName) && isValidValue(parameterValue)) {
                        executeRemoteShellCommand("export "
                                + parameterName
                                + "='"
                                + parameterValue
                                + "'\n",
                            teeOut,
                            channel);
                    } else {
                        logger.warn("Skipping invalid parameter name/value pair. Name: "
                            + parameterName
                            + ", Value: "
                            + parameterValue);
                    }
                }

                // TODO: Replace this logic after the ActionScript manifest is done (OM-42083)
                // This is a terrible hack, but we are fixing it!
                final String actionCommand = accountValues.scriptPath
                    + "/"
                    + workflow.getDisplayName()
                    + "\n";

                executeRemoteShellCommand("echo 'Beginning execution of ActionScript.'\n", teeOut, channel);

                executeRemoteShellCommand(actionCommand, teeOut, channel);

                executeRemoteShellCommand("exit\n", teeOut, channel);

                // Wait for the connection to exit
                Collection<ClientChannelEvent> result = channel.waitFor(
                    EnumSet.of(ClientChannelEvent.CLOSED), TimeUnit.SECONDS.toMillis(15L));

                // Log a recap of the session
                logger.info("Sent: " + sent);
                logger.info("Exit status: " + channel.getExitStatus());
                logger.info("Out: " + out);
                logger.info("Err: " + err);

                channel.close(false);
            }
        } catch (IOException e) {
           throw new RemoteExecutionException(e.getMessage(), e);
        }
        return true;
    }

    private static boolean isValidName(@Nonnull final String name) {
        Matcher m = VALID_BASH_VARIABLE_NAME_REGEX.matcher(name);
        return m.find();
    }

    private static boolean isValidValue(@Nonnull final String value) {
        //TODO: Determine if any further validation is needed for bash variables
        // Maybe we should consider escaping double-quotes?
        return value != null;
    }

    public static int executeRemoteShellCommand(@Nonnull final String shellCommand,
                                                @Nonnull final OutputStream outputStream,
                                                @Nonnull final ClientChannel channel)
            throws IOException {
        outputStream.write(shellCommand.getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
        // Wait for the command to complete
//        Collection<ClientChannelEvent> result = channel.waitFor(
//            EnumSet.of(ClientChannelEvent.EXIT_SIGNAL), TimeUnit.SECONDS.toMillis(15L));
//        return channel.getExitStatus();
        return 0;
    }
}
