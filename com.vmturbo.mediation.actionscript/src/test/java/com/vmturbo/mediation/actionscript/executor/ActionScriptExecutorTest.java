package com.vmturbo.mediation.actionscript.executor;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.apache.sshd.common.SshException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Charsets;

import com.vmturbo.mediation.actionscript.ActionScriptProbeAccount;
import com.vmturbo.mediation.actionscript.ActionScriptTestBase;
import com.vmturbo.platform.common.dto.ActionExecution.ActionResponseState;

public class ActionScriptExecutorTest extends ActionScriptTestBase {
    // TODO add tests with bogus parameter names - hard to do because ActionScriptParameterMapper rejects unexpected param names
    // TODO unit test for valid but incorrect public host key string in account values - when host key validation is fully implemented
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    private static ActionScriptProbeAccount accountValues;

    @BeforeClass
    public static void setup() throws IOException {
        // We're a little chatty just so developers know to wait
        System.out.println("ActionScriptExecorTest normally takes a minute or two, with gaps of apparent inactivity");
        // we launch a real sshd process on the test host, configured to run with non-root access and some other
        // non-default configuration so we can use our canned keys
        final int testPort = startSystemSshd(getResource("/ssh/id_rsa.pub"));
        // these are the scripts we'll be invoking in unit tests
        copyResouresToFileTree("testScripts", ActionScriptExecutorTest.class.getPackage().getName(), tempFolder.getRoot());
        // make sure they're all executable
        setAllExecutable(tempFolder.getRoot(), Pattern.compile(".*[.]sh"));
        // set up to use our canned credentials with the current user name on the current machine
        accountValues = new ActionScriptProbeAccount("localhost",
            System.getProperty("user.name"),
            getResource("/ssh/id_rsa"),
            "",
            Integer.toString(testPort), null);
    }

    @AfterClass
    public static void tearDown() {
        stopSystemSshd();
    }

    /**
     * Load text from a resource
     * @param resourcePath path to the resource
     * @return content of resource
     * @throws IOException
     */
    private static String getResource(final @Nonnull String resourcePath) throws IOException {
        return IOUtils.toString(ActionScriptExecutorTest.class.getResourceAsStream(resourcePath), Charsets.UTF_8);
    }

    @Test
    public void testSuccessfulScript() throws ExecutionException {
        makeScriptRunner("noop-succeed.sh")
            .run()
            .waitForCompletion()
            .checkExit(0, null, null)
            .checkStatus(ActionScriptExecutionStatus.COMPLETE)
            .checkProgress(ActionResponseState.SUCCEEDED, "Action Script completed execution", 100);
    }

    @Ignore // TODO until host key feature is fully implemented
    @Test
    public void testWithProvidedHostKey() throws ExecutionException, IOException {
        makeScriptRunner("noop-succeed.sh")
            .withHostKey(getResource("../sshd-host-key.pub"))
            .run()
            .waitForCompletion()
            .checkExit(0, null, null);
    }

    @Ignore // TODO until host key feature is fully implemented
    @Test
    public void testWithBadProvidedHostKey() throws ExecutionException, IOException {
        makeScriptRunner("noop-succeed.sh")
            .withHostKey("bogus host key")
            .run()
            .waitForCompletion()
            .checkExit(null, null, SshException.class);
    }

    @Test
    public void testFailingScript() throws ExecutionException {
        makeScriptRunner("noop-fail.sh")
            .run()
            .waitForCompletion()
            .checkExit(1, null, null)
            .checkStatus(ActionScriptExecutionStatus.FAILED)
            .checkProgress(ActionResponseState.FAILED, "Action Script execution failed", 100);
    }

    @Test
    public void testLongVulnerable() throws ExecutionException {
        makeScriptRunner("long-nontrapping.sh")
            .withTimeoutSeconds(10)
            .run()
            .sleep(5)
            .checkStatus(ActionScriptExecutionStatus.RUNNING)
            .waitForCompletion()
            .checkExit(null, "TERM", null)
            .checkStatus(ActionScriptExecutionStatus.FAILED)
            .checkProgress(ActionResponseState.FAILED, null, 100);
    }

    @Test
    public void testLongWillingScript() throws ExecutionException {
        makeScriptRunner("long-trapping.sh")
            .withTimeoutSeconds(10)
            .withGracePeriodSeconds(5)
            .run()
            .sleep(5)
            .checkStatus(ActionScriptExecutionStatus.RUNNING)
            .waitForCompletion()
            .checkExit(0, null, null)
            .checkStatus(ActionScriptExecutionStatus.COMPLETE)
            .checkProgress(ActionResponseState.SUCCEEDED, null, 100);
    }

    @Test
    public void testLongStubbornScript() throws ExecutionException {
        long start = System.currentTimeMillis();
        makeScriptRunner("long-stubborn.sh")
            .withTimeoutSeconds(10)
            .withGracePeriodSeconds(5)
            .run()
            .sleep(5)
            .checkStatus(ActionScriptExecutionStatus.RUNNING)
            .waitForCompletion()
            .checkExit(0, null, null)
            .checkStatus(ActionScriptExecutionStatus.COMPLETE)
            .checkProgress(ActionResponseState.SUCCEEDED, null, 100);
        assertTrue("Long-running stubborn script appears to have exited too quickly", (System.currentTimeMillis() - start) > 29 * 1000);
    }

    @Test
    public void testSabotagedScript() throws ExecutionException {
        makeScriptRunner("noop-succeed.sh")
            .withSabotageException(new IOException("sabotaged test"))
            .run()
            .waitForCompletion()
            .checkExit(null, null, IOException.class)
            .checkStatus(ActionScriptExecutionStatus.ERROR)
            .checkProgress(ActionResponseState.FAILED, null, 100);
    }

    @Test
    public void testStdoutAndStderr() throws ExecutionException {
        makeScriptRunner("stdout-and-stderr.sh")
            .run()
            .waitForCompletion()
            .checkExit(0, null, null)
            .checkStatus(ActionScriptExecutionStatus.COMPLETE)
            .checkStdout(IntStream.rangeClosed(9991, 10000)
                .mapToObj(i -> String.format("stdout line %s", i))
                .collect(Collectors.joining("\n")))
            .checkStderr(IntStream.rangeClosed(9991, 10000)
                .mapToObj(i -> String.format("stderr line %s", i))
                .collect(Collectors.joining("\n")))
            .checkProgress(ActionResponseState.SUCCEEDED,
                "Action Script completed execution; tail of output:\n" +
                    IntStream.rangeClosed(9991, 10000).mapToObj(i -> String.format("stdout line %s", i))
                        .collect(Collectors.joining("\n")), 100);
    }

    @Test
    public void testOutputWithPartialLine() throws ExecutionException {
        makeScriptRunner("stdout-with-partial.sh")
            .run()
            .waitForCompletion()
            .checkExit(0, null, null)
            .checkStatus(ActionScriptExecutionStatus.COMPLETE)
            .checkStdout(IntStream.rangeClosed(9992, 10000)
                .mapToObj(i -> String.format("stdout line %s", i))
                .collect(Collectors.joining("\n")) + "\nFinal line without line terminator")
            .checkProgress(ActionResponseState.SUCCEEDED, null, 100);
    }

    @Test
    public void testParameterReceived() throws ExecutionException {
        makeScriptRunner("echo-script-name.sh")
            .run()
            .waitForCompletion()
            .checkExit(0, null, null)
            .checkStatus(ActionScriptExecutionStatus.COMPLETE)
            .checkStdoutContainsLine("echo-script-name.sh")
            .checkProgress(ActionResponseState.SUCCEEDED, null, 100);
    }

    @Test
    public void testQuoting() throws ExecutionException {
        // this tests quoting of both the script path and parameter values in the command string sent to ssh
        makeScriptRunner("echo' script%name.sh")
            .run()
            .waitForCompletion()
            .checkExit(0, null, null)
            .checkStatus(ActionScriptExecutionStatus.COMPLETE)
            .checkStdoutContainsLine("echo' script%name.sh")
            .checkProgress(ActionResponseState.SUCCEEDED, null, 100);
    }

    @Test
    public void testScriptWithNonAsciiName() throws ExecutionException {
        // this tests non-ascii chars both in script name and parameters in command line sent to ssh
        makeScriptRunner("happy-new-year-新年快乐.sh")
            .run()
            .waitForCompletion()
            .checkExit(0, null, null)
            .checkStatus(ActionScriptExecutionStatus.COMPLETE)
            .checkStdoutContainsLine("happy-new-year-新年快乐.sh")
            .checkProgress(ActionResponseState.SUCCEEDED, null, 100);

    }
    @Test
    public void testStaggeredOutputInProgress() throws ExecutionException {
        makeScriptRunner("staggered-output.sh")
            .withProgressUpdateInterval(1)
            .withMaxOutputLines(10000)
            .run()
            .sleep(5)
            .checkProgress(ActionResponseState.IN_PROGRESS, null, 50)
            .checkProgressDescriptionContainsLine("Action Script execution is in progress; recent output:")
            .checkProgressDescriptionContainsLine("Line 100")
            .checkProgressDescriptionDoesNotContainLine("Line 1001")
            .sleep(10)
            .checkProgress(ActionResponseState.IN_PROGRESS, null, 50)
            .checkProgressDescriptionContainsLine("Line 1100")
            .checkProgressDescriptionDoesNotContainLine("Line 2001")
            .waitForCompletion()
            .checkStatus(ActionScriptExecutionStatus.COMPLETE)
            .checkProgress(ActionResponseState.SUCCEEDED, null, 100)
            .checkProgressDescriptionContainsLine("Line 2100");
    }

    /**
     * Create a new {@link ScriptRunner} to run a test
     * @param scriptName script name configured into the action script workflow
     * @return new ScriptRunner instance
     */
    private ScriptRunner makeScriptRunner(final @Nonnull String scriptName) {
        return new ScriptRunner(scriptName, accountValues);
    }
}
