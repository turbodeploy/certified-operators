package com.vmturbo.mediation.actionscript;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sshd.client.ClientFactoryManager;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.config.hosts.HostConfigEntryResolver;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.client.subsystem.sftp.SftpClient;
import org.apache.sshd.client.subsystem.sftp.SftpClient.Attributes;
import org.apache.sshd.client.subsystem.sftp.SftpClientFactory;
import org.apache.sshd.common.PropertyResolverUtils;
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
    private static final long HEARTBEAT_SECS = 30L;


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

    public static class SshRunner implements AutoCloseable {
        private final ActionScriptProbeAccount accountValues;
        private final ActionExecutionDTO actionExecutionDTO;
        private final SshClient client;
        private final ClientSession session;

        public SshRunner(@Nonnull final ActionScriptProbeAccount accountValues, @Nullable final ActionExecutionDTO actionExecutionDTO) throws KeyValidationException, RemoteExecutionException {
            this.accountValues = accountValues;
            this.actionExecutionDTO = actionExecutionDTO;

            final String host = accountValues.getNameOrAddress();
            final String userid = accountValues.getUserid();
            final int port = Integer.valueOf(accountValues.getPort());
            final String privateKeyString = accountValues.getPrivateKeyString();
            KeyPair loginKeyPair;
            try {
                loginKeyPair = SshUtils.extractKeyPair(privateKeyString);
            } catch (GeneralSecurityException | IOException e) {
                throw new KeyValidationException(String.format("Cannot recover public key from string for action script target %s", accountValues.getNameOrAddress()), e);
            }
            // set up our server key to accept either the key specified in the target or any key if none
            // is given. In latter case, we'll remember it and require it in future connections
            final AcceptAnyOrGivenServerKeyVerifier serverKeyVerifier = new AcceptAnyOrGivenServerKeyVerifier(accountValues.getHostKey(), host);

            this.client = SshClient.setUpDefaultClient();
            client.setServerKeyVerifier(serverKeyVerifier);
            // there should not be an ssh-config file in the turbo installation, and if there is one
            // we don't want to process it.
            client.setHostConfigEntryResolver(HostConfigEntryResolver.EMPTY);
            client.start();

            try {
                PropertyResolverUtils.updateProperty(client, ClientFactoryManager.HEARTBEAT_INTERVAL, TimeUnit.SECONDS.toMillis(HEARTBEAT_SECS));
                this.session = client.connect(userid, host, port)
                    .verify(SESSION_CONNECT_TIMEOUT_SECS, TimeUnit.SECONDS)
                    .getSession();
                session.setUsername(userid);
                session.addPublicKeyIdentity(loginKeyPair);
                session.auth().verify(AUTH_TIMEOUT_SECS, TimeUnit.SECONDS);
                logger.debug("session authenticated to " + host);
            } catch (IOException e) {
                throw new RemoteExecutionException("IO Exception while using the SSH client "
                    + host + ": " + e.getMessage(), e);
            }
        }

        public <T> T run(RemoteCommand<T> remoteCommand) throws RemoteExecutionException {
            return remoteCommand.execute(accountValues, session, actionExecutionDTO);
        }

        public void close() {
            if (session != null) {
                try {
                    session.close();
                } catch (IOException e) {
                    logger.warn("Failed to close SSH client session", e);
                }
            }
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    logger.warn("Failed to close SSH client session", e);
                }
            }
        }
    }

    /**
     * Run code in an SSH session and return a result.
     *
     * @param <T> a generic return type, allowing this method to return whatever remoteCommand returns
     * @param accountValues to use for creating the connection
     * @param actionExecution the action on whose behalf this execution is being performed, or null if none (e.g. commands executed during discovery)
     * @param remoteCommand the command to execute once the connection is established
     * @return the result of executing the remote command
     * @throws KeyValidationException   if a valid SSH private key cannot be retrieved from accountValues
     * @throws RemoteExecutionException if an IO Exception occurs while executing the remote command
     */
    public static <T> T runInSshSession(@Nonnull final ActionScriptProbeAccount accountValues,
                                        @Nullable final ActionExecutionDTO actionExecution,
                                        @Nonnull final RemoteCommand<? extends T> remoteCommand)
        throws KeyValidationException, RemoteExecutionException {

        try (SshRunner runner = new SshRunner(accountValues, actionExecution)) {
            return runner.run(remoteCommand);
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
     * @param path absolute path of the file on the execution server
     * @param runner SshRunner with a live session
     *
     * @return file content as text
     * @throws RemoteExecutionException if there's a problem obtaining the content
     * @throws KeyValidationException   if the key provided in accountValues is not valid
     */
    public static String getRemoteFileContent(final String path, @Nonnull SshRunner runner) throws RemoteExecutionException, KeyValidationException {
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
        return runner.run(cmd);
    }

    /**
     * Perform an lstat on a file or directory that resides on the execution server
     *
     * @param path absolute path of the file on the server
     * @param runner SshRunner with a live session

     * @return file attributes fo remote file
     * @throws RemoteExecutionException if there's a problem obtaining the file attributes
     * @throws KeyValidationException if the key provided in the accountValues is not valid
     * */
    public static Attributes getRemoteFileAttributes(final String path, @Nonnull final SshRunner runner) throws RemoteExecutionException, KeyValidationException {
        RemoteCommand<Attributes> cmd = (a, session, ae) -> {
            SftpClient sftp = null;
            try {
                sftp = SftpClientFactory.instance().createSftpClient(session);
                return sftp.lstat(path);
            } catch (IOException e) {
                throw new RemoteExecutionException("Failed to obtain file attributes", e);
            }
        };
        return runner.run(cmd);
    }
}
