package com.vmturbo.mediation.actionscript;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sshd.common.Factory;
import org.apache.sshd.common.keyprovider.ClassLoadableResourceKeyPairProvider;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.apache.sshd.server.SshServer;
import org.apache.sshd.server.auth.pubkey.KeySetPublickeyAuthenticator;
import org.apache.sshd.server.command.Command;
import org.apache.sshd.server.subsystem.sftp.SftpSubsystemFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.util.SocketUtils;

import com.google.common.io.Resources;

import com.vmturbo.mediation.actionscript.exception.KeyValidationException;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;

/**
 * Class to test the path validation for the Action Script folder.
 * This folder is where the user will place the executable files (scripts)
 * which the ActionScript probe will invoke in the Executor.
 */
public class ActionScriptPathValidatorTest  {

    private static Logger logger = LogManager.getLogger(ActionScriptPathValidatorTest.class);

    private SshServer sshd;
    // must be > 8000 for non-privileged process to create it
    private int testPort;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws IOException, KeyValidationException {
        testPort = SocketUtils.findAvailableTcpPort();
        logger.info("TCP Test Port for SSH: {}", testPort);

        sshd = SshServer.setUpDefaultServer();
        sshd.setPort(testPort);

        // load the private key id_rsa from the src/test/resources/ssh folder
        final ClassLoadableResourceKeyPairProvider keyPairProvider
            = new ClassLoadableResourceKeyPairProvider("ssh/id_rsa");
        sshd.setKeyPairProvider(keyPairProvider);

        // fetch the private key info for this test from the resource
        final String privateKeyString = Resources.toString(Resources.getResource("ssh/id_rsa"),
            Charset.defaultCharset());

        // extract a keypair from the key for this test
        final KeyPair keyPair = SshUtils.extractKeyPair(privateKeyString);

        // prepare the authenticator to recognize the public key of this pair
        sshd.setPublickeyAuthenticator(new KeySetPublickeyAuthenticator(
            Collections.singleton(keyPair.getPublic())));

        // create a fake Shell factory instead of running real shell commands
        sshd.setShellFactory(new TestShellFactory());

        // add a "real" SFTP factor for handling file system requests
        SftpSubsystemFactory factory = new SftpSubsystemFactory.Builder()
            .build();
        sshd.setSubsystemFactories(Collections.singletonList(factory));

        sshd.start();

    }

    @After
    public void takeDown() throws IOException {
        if (sshd != null) {
            sshd.stop();
        }
    }

    /**
     * Create a temporary folder path and check that the path is found.
     *
     * @throws IOException if there is an error creating the folder or accessing the private key file
     */
    @Test
    public void testValidateActionScriptPathExists() throws IOException {
        // arrange
        final ActionScriptPathValidator validator = new ActionScriptPathValidator();
        final String hostname = "localhost";
        final String userid = "test_user";
        final String port = Integer.toString(testPort);
        final String privateKeyString = Resources.toString(Resources.getResource("ssh/id_rsa"),
            Charset.defaultCharset());
        final String scriptPath = tempFolder.getRoot().getAbsolutePath();
        tempFolder.newFile("test1");
        tempFolder.newFile("test2");
        final ActionScriptProbeAccount accountValues = new ActionScriptProbeAccount(hostname,
            userid, privateKeyString, scriptPath, port);
        // act
        final ValidationResponse response = validator.validateActionScriptPath(accountValues);
        // assert
        assertTrue(response.getErrorDTOList().isEmpty());
    }

    /**
     * Test that a file path that does not exist reports the error
     *
     * @throws IOException if there is an error accessing the private key file
     */
    @Test
    public void testValidateActionScriptPathDoesntExist() throws IOException {
        // arrange
        final String nonExistingPath = "this/path/doest/exist";
        final ActionScriptPathValidator validator = new ActionScriptPathValidator();
        final String hostname = "localhost";
        final String port = Integer.toString(testPort);
        final String userid = "test_user";
        final String privateKeyString = Resources.toString(Resources.getResource("ssh/id_rsa"),
            Charset.defaultCharset());
        final ActionScriptProbeAccount accountValues = new ActionScriptProbeAccount(hostname,
            userid,
            privateKeyString,
            nonExistingPath,
            port);
        // act
        final ValidationResponse response = validator.validateActionScriptPath(accountValues);
        // assert
        final List<ErrorDTO> errors = response.getErrorDTOList();
        assertEquals(1, errors.size());
        String message = errors.iterator().next().getDescription();
        assertThat(message, containsString("No such file or directory"));
    }

    /**
     * Test that a path to a file is rejected as an invalid actionScriptPath.
     *
     * @throws IOException if there is an error creating the test file or accessing the private key file
     */
    @Test
    public void testValidateActionScriptNotFolder() throws IOException {
        // arrange
        final File existingFolder = tempFolder.newFolder("a", "b");
        // create a/b/c as a file, not a folder
        final Path filePath = Paths.get(existingFolder.getAbsolutePath(), "c");
        final File cFile = filePath.toFile();
        // ignore value since we're calling for side-effect: create parent directories if necessary
        //noinspection ResultOfMethodCallIgnored
        cFile.getParentFile().mkdirs();
        cFile.createNewFile();
        // test the path to the file as the actionScriptPath
        final Path testExistingPath = cFile.toPath();
        final ActionScriptPathValidator validator = new ActionScriptPathValidator();
        // create credentials for the connection
        final String hostname = "localhost";
        final String port = Integer.toString(testPort);
        final String userid = "test_user";
        final String privateKeyString = Resources.toString(Resources.getResource("ssh/id_rsa"),
            Charset.defaultCharset());
        final ActionScriptProbeAccount accountValues = new ActionScriptProbeAccount(hostname,
            userid,
            privateKeyString,
            testExistingPath.toString(),
            port);
        // act
        final ValidationResponse response = validator.validateActionScriptPath(accountValues);
        List<ErrorDTO> errors = response.getErrorDTOList();
        // assert
        assertEquals(1, errors.size());
        String message = errors.iterator().next().getDescription();
        assertThat(message, containsString("is not a directory"));
    }

    /**
     * test that a file path that does not exist reports the error
     */
    @Ignore //TODO: Determine if this test still makes sense after implementing manifest file (OM-42083)
    @Test
    public void testValidateActionScriptPathNotExecutable() throws IOException {
        final File existingFolder = tempFolder.newFolder("a", "b", "c");
        final Path testExistingPath = existingFolder.toPath();
        testExistingPath.toFile().setExecutable(false);
        final ActionScriptPathValidator validator = new ActionScriptPathValidator();
        // create credentials for the connection
        final String hostname = "localhost";
        final String port = Integer.toString(testPort);
        final String userid = "test_user";
        final String privateKeyString = Resources.toString(Resources.getResource("ssh/id_rsa"),
            Charset.defaultCharset());
        final ActionScriptProbeAccount accountValues = new ActionScriptProbeAccount(hostname,
            userid,
            privateKeyString,
            testExistingPath.toString(),
            port);
        // act
        final ValidationResponse response = validator.validateActionScriptPath(accountValues);
        List<ErrorDTO> errors = response.getErrorDTOList();
        // assert
        assertEquals(1, errors.size());
        String message = errors.iterator().next().getDescription();
        assertThat(message, containsString("is not executable"));
    }

    /**
     * A fake Shell factory, to be used instead of running real shell commands
     */
    private class TestShellFactory implements Factory<Command> {
        @Override
        public Command create() {
            return new Command() {

                @Override
                public void start(final Environment environment) throws IOException {

                }

                @Override
                public void destroy() throws Exception {

                }

                @Override
                public void setInputStream(final InputStream inputStream) {

                }

                @Override
                public void setOutputStream(final OutputStream outputStream) {

                }

                @Override
                public void setErrorStream(final OutputStream outputStream) {

                }

                @Override
                public void setExitCallback(final ExitCallback exitCallback) {

                }
            };
        }
    }
}