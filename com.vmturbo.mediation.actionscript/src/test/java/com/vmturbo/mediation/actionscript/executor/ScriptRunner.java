package com.vmturbo.mediation.actionscript.executor;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import com.vmturbo.mediation.actionscript.ActionScriptProbeAccount;
import com.vmturbo.mediation.actionscript.exception.RemoteExecutionException;
import com.vmturbo.mediation.actionscript.executor.ActionScriptExecutor.CompletionInfo;
import com.vmturbo.mediation.actionscript.executor.ActionScriptExecutor.TestAccess;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Parameter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * This class provides a fluent API for creating unit tests for {@link ActionScriptExecutor}
 */
class ScriptRunner {
    private static Logger logger = LogManager.getLogger(ScriptRunner.class);

    private Thread thread = null;

    private final String scriptPath;
    private final String scriptName;
    private ActionScriptProbeAccount accountValues;
    private ActionScriptExecutor executor;
    private ActionScriptExecutionStatus initialStatus;
    private TestProgressTracker progressTracker;
    private Integer timeoutSeconds = null;
    private Integer gracePeriodSeconds = null;
    private Throwable sabotageException = null;
    private Integer progressUdpateInterval = null;
    private Integer maxOutputLines = null;
    private TestAccess testAccess = null;
    private ActionExecutionDTO actionExecutionDTO = null;

    ScriptRunner(final @Nonnull String scriptName, final @Nonnull ActionScriptProbeAccount accountValues) {
        this.accountValues = accountValues;
        this.scriptPath = new File(ActionScriptExecutorTest.tempFolder.getRoot(), scriptName).getAbsolutePath();
        this.scriptName = new File(scriptPath).getName();
    }

    ScriptRunner withTimeoutSeconds(final int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
        return this;
    }

    ScriptRunner withGracePeriodSeconds(final int gracePeriodSeconds) {
        this.gracePeriodSeconds = gracePeriodSeconds;
        return this;
    }

    ScriptRunner withMaxOutputLines(final int maxOutputLines) {
        this.maxOutputLines = maxOutputLines;
        return this;
    }

    ScriptRunner withProgressUpdateInterval(final int progressUpdateInterval) {
        this.progressUdpateInterval = progressUpdateInterval;
        return this;
    }

    ScriptRunner withSabotageException(final Throwable sabotageException) {
        this.sabotageException = sabotageException;
        return this;
    }

    ScriptRunner withHostKey(final String requiredHostKey) {
        this.accountValues = new ActionScriptProbeAccount(
            accountValues.getNameOrAddress(), accountValues.getUserid(), accountValues.getPrivateKeyString(),
            accountValues.getManifestPath(), accountValues.getPort(), requiredHostKey);
        return this;
    }

    ScriptRunner run() {
        logger.info("Running script {}", scriptName);
        this.progressTracker = new TestProgressTracker();
        this.actionExecutionDTO = createActionExecution(scriptPath, scriptName, timeoutSeconds);
        this.executor = new ActionScriptExecutor(accountValues, actionExecutionDTO, progressTracker);
        this.testAccess = executor.getTestAccess();
        // perform any customizations to the executor before running it
        if (gracePeriodSeconds != null) {
            testAccess.setGracePeriodSeconds(gracePeriodSeconds);
        }
        if (maxOutputLines != null) {
            testAccess.setMaxOutputLines(maxOutputLines);
        }
        if (progressUdpateInterval != null) {
            testAccess.setProgressOutputUpdateIntervalSeconds(progressUdpateInterval);
        }
        if (sabotageException != null) {
            testAccess.setSabotageExecution(sabotageException);
        }
        thread = new Thread(() -> executor.execute());
        thread.start();
        return this;
    }

    ScriptRunner waitForCompletion() throws ExecutionException {
        logger.info("Waiting for action script {} to terminate", testAccess.getScriptName());
        while (thread.isAlive()) {
            try {
                thread.join();
             } catch (InterruptedException e) {
                // keep waiting
                break;
            }
        }
        logger.info("Action script {} has terminated", testAccess.getScriptName());
        return this;
    }

    /**
     * Test that the current status of the executor is among those expected.
     *
     * @param expected
     * @return
     */
    ScriptRunner checkStatus(final @Nonnull ActionScriptExecutionStatus... expected) {
        ActionScriptExecutionStatus actual = testAccess.getCurrentStatus();
        String msg = String.format("Unexpected status '%s', expected one of: %s", actual, Stream.of(expected)
            .map(Enum::name).collect(Collectors.joining(", ")));
        Assert.assertTrue(msg, Arrays.asList(expected).contains(actual));
        return this;
    }

    /**
     * Check that the completion info for this script is as expected. Only one of the three parameters should be non-null,
     * as they are mutually exclusive in a {@link CompletionInfo}
     * @param exitStatus
     * @param exitSignal
     * @param exceptionClass
     * @return
     */
    ScriptRunner checkExit(final @Nullable Integer exitStatus, final @Nullable String exitSignal, final @Nullable Class<? extends Throwable> exceptionClass) {
        CompletionInfo comp = testAccess.getCompletionInfo().orElse(null);
        assertNotNull(comp);
        Throwable actualException = comp.getException();
        if (actualException instanceof RemoteExecutionException && actualException.getCause() != null) {
            actualException = actualException.getCause();
        }
        Class<?> actualExceptionClass = actualException != null ? actualException.getClass() : null;
        Assert.assertEquals("Incorrect exit [status, signal, exception]",
            Arrays.asList(exitStatus, exitSignal, exceptionClass),
            Arrays.asList(comp.getExitStatus(), comp.getExitSignal(), actualExceptionClass));
        return this;
    }

    /**
     * Test that the collected stdout output is as expected.
     * <p>If either expected or actual value lacks a final newline, it is added prior to the test.</p>
     * <p>This test only applies after completion of the script.</p>
     * @param expected
     * @return
     */
    ScriptRunner checkStdout(final @Nonnull String expected) {
        checkOutput(expected, testAccess.getCompletionInfo().map(CompletionInfo::getStdout).orElse(null), "Unexpected stdout output");
        return this;
    }

    /**
     * Test that the collectd stdout output contains the given string as a complete line.
     * @param expected
     * @return
     */
    ScriptRunner checkStdoutContainsLine(final @Nonnull String expected) {
        checkOutputContainsLine(expected, testAccess.getCompletionInfo().map(CompletionInfo::getStdout).orElse(null), "Stdout does not contain the line: ");
        return this;
    }

    /**
     * Like {@link #checkStdout(String)} but for stderr
     * @param expected
     * @return
     */
    public ScriptRunner checkStderr(final @Nonnull String expected) {
        checkOutput(expected, testAccess.getCompletionInfo().map(CompletionInfo::getStderr).orElse(null), "Unexpected stderr output");
        return this;
    }

    /**
     * like {@link #checkStdoutContainsLine(String)} but for stderr
     * @param expected
     * @return
     */
    public ScriptRunner checkStderrContainsLine(final @Nonnull String expected) {
        checkOutputContainsLine(expected, testAccess.getCompletionInfo().map(CompletionInfo::getStderr).orElse(null), "Stdout does not contain the line: ");
        return this;
    }

    private void checkOutput(@Nonnull String expected, @Nonnull String actual, final  String message) {
        if (!expected.endsWith("\n")) {
            expected = expected + "\n";
        }
        if (!actual.endsWith("\n")) {
            actual = actual + "\n";
        }
        Assert.assertEquals(message, expected, actual);
    }

    private void checkOutputContainsLine(final @Nonnull String expected, final @Nonnull String actual, String message) {
        boolean contains = actual != null && Arrays.asList(actual.split("\\r?\\n")).contains(expected);
        Assert.assertTrue(message + " " + expected, contains);
    }

    /**
     * Test that the values in the most recent progress update. Description is not tested if the
     * expected value is null
     *
     * @param expectedState
     * @param expectedDescription
     * @param expectedPercentage
     * @return
     */
    ScriptRunner checkProgress(final @Nonnull ActionResponseState expectedState, final @Nullable String expectedDescription, final int expectedPercentage) {
        Assert.assertEquals("Unexpected state in progress tracker", expectedState, progressTracker.getState());
        if (expectedDescription != null) {
            Assert.assertEquals("Unexpected description in progress tracker", expectedDescription, progressTracker.getDescription());
        }
        Assert.assertEquals("Unexpected percentage in progress tracker", expectedPercentage, progressTracker.getPercentage());
        return this;
    }

    /**
     * Tests that the description value in the most recent progerss update contains the expected
     * string as a complete line.
     * @param line
     * @return
     */
    ScriptRunner checkProgressDescriptionContainsLine(final @Nonnull String line) {
        boolean found = Arrays.asList(progressTracker.getDescription().split("\\r?\\n")).contains(line);
        Assert.assertTrue("Progress description does not contain expected line '" + line + "'", found);
        return this;
    }

    /**
     * Opposite of {@link #checkProgressDescriptionContainsLine(String)}
     * @param line
     * @return
     */
    ScriptRunner checkProgressDescriptionDoesNotContainLine(final @Nonnull String line) {
        boolean found = Arrays.asList(progressTracker.getDescription().split("\\r?\\n")).contains(line);
        Assert.assertFalse("Progress description unexpectedly contains line '" + line + "'", found);
        return this;
    }

    /**
     * Chill for a while (waiting for the script to do something that will then be tested)
     * @param seconds
     * @return
     */
    ScriptRunner sleep(final int seconds) {
        logger.info("Script {} pausing for {} seconds", testAccess.getScriptName(), seconds);
        long waketime = System.currentTimeMillis() + seconds * 1000;
        while (System.currentTimeMillis() < waketime) {
            try {
                Thread.sleep(waketime - System.currentTimeMillis());
            } catch (InterruptedException e) {
                // OK to be interrupted here, we'll just hit snooze
            }
        }
        return this;
    }

    /**
     * Create an {@link ActionExecutionDTO} instance to use in testing. All of this is canned except
     * the script path, the display name, and the time limit.
     * <p>The instance is constructed so that it will set a value for the VMT_TARGET_NAME parameter,
     * which is checked by one of the unit tests.</p>
     * @param scriptPath
     * @param displayName
     * @param timeoutSeconds
     * @return
     */
    private ActionExecutionDTO createActionExecution(final @Nonnull String scriptPath, final @Nonnull String displayName, final @Nullable Integer timeoutSeconds) {
        Workflow.Builder workFlowBuilder = Workflow.newBuilder()
            .setId("test")
            .setDisplayName(new File(scriptPath).getName())
            .setScriptPath(scriptPath)
            .addParam(Parameter.newBuilder()
                .setName("VMT_TARGET_NAME")
                .setType("text")
                .build()
            );
        if (timeoutSeconds != null) {
            workFlowBuilder.setTimeLimitSeconds(timeoutSeconds);
        }
        return ActionExecutionDTO.newBuilder()
            .setActionType(ActionType.MOVE)
            .addActionItem(ActionItemDTO.newBuilder()
                .setUuid("UnnecessaryMove321")
                .setActionType(ActionType.MOVE)
                .setTargetSE(EntityDTO.newBuilder()
                    .setEntityType(EntityType.VIRTUAL_MACHINE)
                    .setId("vm12")
                    .setDisplayName(displayName))
                .setCurrentSE(EntityDTO.newBuilder()
                    .setEntityType(EntityType.PHYSICAL_MACHINE)
                    .setId("pm202")
                    .setDisplayName("Perfectly Good PM"))
                .setNewSE(EntityDTO.newBuilder()
                    .setEntityType(EntityType.PHYSICAL_MACHINE)
                    .setId("pm205")
                    .setDisplayName("PM With Greener Grass")))
            .setWorkflow(workFlowBuilder.build())
            .build();
    }

    /**
     * Get the ActionExecutionDTO for this script runner.
     *
     * @return {@link ActionExecutionDTO}
     */
    public ActionExecutionDTO getActionExecutionDTO() {
        return actionExecutionDTO;
    }
}
