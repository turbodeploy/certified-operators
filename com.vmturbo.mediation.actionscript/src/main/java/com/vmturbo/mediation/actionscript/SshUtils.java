package com.vmturbo.mediation.actionscript;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.keyverifier.AcceptAllServerKeyVerifier;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.client.subsystem.sftp.SftpClient;
import org.apache.sshd.client.subsystem.sftp.SftpClient.Attributes;
import org.apache.sshd.client.subsystem.sftp.SftpClientFactory;
import org.apache.sshd.common.util.security.SecurityUtils;

import sun.misc.IOUtils;

import com.vmturbo.mediation.actionscript.exception.KeyValidationException;
import com.vmturbo.mediation.actionscript.exception.RemoteExecutionException;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;

/**
 * Utilities for creating SSH and SFTP connections.
 **/
public class SshUtils {

    private static final Logger logger = LogManager.getLogger();

    private static final long SESSION_CONNECT_TIMEOUT_SECS = 10L;
    private static final long AUTH_TIMEOUT_SECS = 15L;

    private static final boolean isPasswordAuthAllowed = getPasswordConfig();

    private static boolean getPasswordConfig() {
        Properties config = new Properties();
        try {
            config.load(SshUtils.class.getResourceAsStream("config.properties"));
            return Boolean.parseBoolean(config.getProperty("ssh-allow-password-auth"));
        } catch (IOException e) {
            logger.warn("Resource config.properties is missing or malformed; continuing with default property values");
            return false;
        }
    }

    /**
     * An interface for defining methods to be executed in the context of an SSH session
     */
    @FunctionalInterface
    public interface RemoteCommand<T> {
        /**
         * Encapsulates code to be executed within an established SSH session.
         *
         * @param accountValues   {@link ActionScriptProbeAccount} values from the target
         * @param session         {@link ClientSession} over which the code will be executed
         * @param actionExecution {@link ActionExecutionDTO} defining the action being performed.
         *                        This can be null if there is no action being performed (e.g. during discovery)
         * @return result computed by code (of whatever type is appropriate)
         * @throws RemoteExecutionException if there's a problem with the execution
         */
        T execute(ActionScriptProbeAccount accountValues,
                  ClientSession session,
                  @Nullable ActionExecutionDTO actionExecution) throws RemoteExecutionException;
    }

    /**
     * Run code in an SSH session and return a result.
     *
     * @param accountValues   to use for creating the connection
     * @param remoteCommand   the command to execute once the connection is established
     * @param actionExecution the {@link ActionExecutionDTO} defining the action being performed,
     *                        or null if this is not part an action execution.
     * @param <T>             a generic return type, allowing this method to return whatever remoteCommand returns
     * @return the result of executing the remote command
     * @throws KeyValidationException   if a valid SSH private key cannot be retrieved from accountValues
     * @throws RemoteExecutionException if an IO Exception occurs while executing the remote command
     */
    public static <T> T runInSshSession(@Nonnull final ActionScriptProbeAccount accountValues,
                                        @Nonnull final RemoteCommand<? extends T> remoteCommand,
                                        @Nullable final ActionExecutionDTO actionExecution)
        throws KeyValidationException, RemoteExecutionException {

        final String host = accountValues.getNameOrAddress();
        final String userid = accountValues.getUserid();
        final int port = Integer.valueOf(accountValues.getPort());
        final String privateKeyString = accountValues.getPrivateKeyString();
        KeyPair loginKeyPair;
        String loginPassword;
        try {
            // try to use provided key as a private key first
            loginKeyPair = SshUtils.extractKeyPair(privateKeyString);
            loginPassword = null;
        } catch (GeneralSecurityException | IOException e) {
            if (isPasswordAuthAllowed) {
                // fall back to using it as a password if that's permitted in this build
                loginPassword = privateKeyString;
                loginKeyPair = null;
            } else {
                throw new KeyValidationException("Cannot recover public key from string: "
                    + privateKeyString, e);
            }
        }

        try (SshClient client = SshClient.setUpDefaultClient()) {
            client.setServerKeyVerifier(AcceptAllServerKeyVerifier.INSTANCE);
            client.start();

            try (final ClientSession session = client.connect(userid, host, port)
                .verify(SESSION_CONNECT_TIMEOUT_SECS, TimeUnit.SECONDS)
                .getSession()) {
                session.setUsername(userid);
                if (loginKeyPair != null) {
                    session.addPublicKeyIdentity(loginKeyPair);
                } else {
                    session.addPasswordIdentity(loginPassword);
                }
                session.auth().verify(AUTH_TIMEOUT_SECS, TimeUnit.SECONDS);
                logger.debug("session authenticated to " + host);
                return remoteCommand.execute(accountValues, session, actionExecution);
            } finally {
                client.stop();
            }
        } catch (IOException e) {
            throw new RemoteExecutionException("IO Exception while using the SSH client "
                + host + ": " + e.getMessage(), e);
        }
    }

    public static KeyPair extractKeyPair(String privateKeyString)
        throws GeneralSecurityException, IOException {

        InputStream privateKeyStream = new ByteArrayInputStream(privateKeyString.getBytes());

        return SecurityUtils.loadKeyPairIdentity(null, privateKeyStream, null);
    }

    /**
     * Retrieve the content of a text file from the execution server.
     *
     * @param path            absolute path of the file on the execution server
     * @param accountValues   {@link ActionScriptProbeAccount} values for the target
     * @param actionExecution {@link ActionExecutionDTO} defining the currently executing action, or
     *                        null if this is not part of an action execution.
     * @return file content as text
     * @throws RemoteExecutionException if there's a problem obtaining the content
     * @throws KeyValidationException   if the key provided in accountValues is not valid
     */
    public static String getRemoteFileContent(final String path,
                                              ActionScriptProbeAccount accountValues,
                                              @Nullable ActionExecutionDTO actionExecution) throws RemoteExecutionException, KeyValidationException {
        RemoteCommand<String> cmd = (a, session, ae) -> {
            try {
                SftpClient sftp = null;
                sftp = SftpClientFactory.instance().createSftpClient(session);

                return new String(IOUtils.readFully(sftp.read(path),
                    Integer.MAX_VALUE, // read to end of stream
                    true)); // this arg is ignored when prior is -1 or MAX_VALUE
            } catch (IOException e) {
                throw new RemoteExecutionException("Failed to fetch remote file", e);
            }

        };
        return runInSshSession(accountValues, cmd, actionExecution);
    }

    /**
     * Perform an lstat on a file that resides on the execution server
     *
     * @param path            absolute path of the file on the server
     * @param accountValues   {@link ActionScriptProbeAccount} values for the target
     * @param actionExecution {@link ActionExecutionDTO} defining the currently executing action,
     *                        or null if this is not part of an action execution.
     *                        * @return
     * @throws RemoteExecutionException if there's a problem obtaining the file attributes
     * @throws KeyValidationException if the key provided in the accountValues is not valid
     * */
    public static Attributes getRemoteFileAttributes(final String path,
                                                     ActionScriptProbeAccount accountValues,
                                                     @Nullable ActionExecutionDTO actionExecution) throws RemoteExecutionException, KeyValidationException {
        RemoteCommand<Attributes> cmd = (a, session, ae) -> {
            SftpClient sftp = null;
            try {
                sftp = SftpClientFactory.instance().createSftpClient(session);
                return sftp.lstat(path);
            } catch (IOException e) {
                throw new RemoteExecutionException("Failed to obtain file attributes", e);
            }
        };
        return runInSshSession(accountValues, cmd, actionExecution);
    }
}
