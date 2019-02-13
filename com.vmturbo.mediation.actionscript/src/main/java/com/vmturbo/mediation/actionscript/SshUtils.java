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
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.keyverifier.AcceptAllServerKeyVerifier;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.util.security.SecurityUtils;

import com.google.common.annotations.VisibleForTesting;

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

    /**
     * An interface for defining remote commands that can be executed within an SSH session
     */
    @FunctionalInterface
    public interface RemoteCommand<T> {

        /**
         * Execute the remote command
         *
         * @param accountValues that were used to create the connection
         * @param session holds the state of the connection to the remote host
         * @param actionExecutionDTO the action being executed, or null during validation and discovery
         * @return the result of executing the remote command
         * @throws RemoteExecutionException if an error occurs while executing the remote command
         */
        T execute(@Nonnull ActionScriptProbeAccount accountValues,
                  @Nonnull ClientSession session,
                  @Nullable ActionExecutionDTO actionExecutionDTO) throws RemoteExecutionException;
    }

    /**
     * Execute a remote command and return the result
     *
     * @param accountValues to use for creating the connection
     * @param remoteCommand the command to execute once the connection is established
     * @param <T> a generic return type, allowing this method to return whatever remoteCommand returns
     * @return the result of executing the remote command
     * @throws KeyValidationException if a valid SSH private key cannot be retrieved from accountValues
     * @throws RemoteExecutionException if an IO Exception occurs while executing the remote command
     */
    public static <T> T executeRemoteCommand(@Nonnull final ActionScriptProbeAccount accountValues,
                                             @Nonnull final RemoteCommand<? extends T> remoteCommand)
        throws KeyValidationException, RemoteExecutionException {
        return executeRemoteCommand(accountValues, remoteCommand, null);
    }

    /**
     * Execute a remote command as part of action execution and return the result
     *
     * @param accountValues to use for creating the connection
     * @param remoteCommand the command to execute once the connection is established
     * @param actionExecutionDTO the action being executed
     * @param <T> a generic return type, allowing this method to return whatever remoteCommand returns
     * @return the result of executing the remote command
     * @throws KeyValidationException if a valid SSH private key cannot be retrieved from accountValues
     * @throws RemoteExecutionException if an IO Exception occurs while executing the remote command
     */
    public static <T> T executeRemoteCommand(@Nonnull final ActionScriptProbeAccount accountValues,
                                             @Nonnull final RemoteCommand<? extends T> remoteCommand,
                                             @Nullable final ActionExecutionDTO actionExecutionDTO)
            throws KeyValidationException, RemoteExecutionException {
        final String host = accountValues.getNameOrAddress();
        final String userid = accountValues.getUserid();
        final int port = Integer.valueOf(accountValues.getPort());
        final String privateKeyString = accountValues.getPrivateKeyString();
        final KeyPair loginKeyPair = extractKeyPair(privateKeyString);

        try (SshClient client = SshClient.setUpDefaultClient()) {
            client.setServerKeyVerifier(AcceptAllServerKeyVerifier.INSTANCE);
            client.start();

            try (final ClientSession session = client.connect(userid, host, port)
                .verify(SESSION_CONNECT_TIMEOUT_SECS, TimeUnit.SECONDS)
                .getSession()) {
                session.setUsername(userid);
                session.addPublicKeyIdentity(loginKeyPair);
                session.auth().verify(AUTH_TIMEOUT_SECS, TimeUnit.SECONDS);
                logger.debug("session authenticated to " + host);
                return remoteCommand.execute(accountValues, session, actionExecutionDTO);
            } finally {
                client.stop();
            }
        } catch (RemoteExecutionException e) {
            // Do not wrap a RemoteExecutionException
            throw e;
        } catch (IOException | RuntimeException e) {
            // Wrap Runtime and IOExceptions as a custom checked exception
            throw new RemoteExecutionException("Exception encountered while using the SSH client "
                + host + ": " + e.getMessage(), e);
        }
    }

    @VisibleForTesting
    static KeyPair extractKeyPair(String privateKeyString) throws KeyValidationException {
        InputStream privateKeyStream = new ByteArrayInputStream(privateKeyString.getBytes());
        try {
            return SecurityUtils.loadKeyPairIdentity(null, privateKeyStream, null);
        }
        catch (GeneralSecurityException | IOException e) {
                throw new KeyValidationException("Cannot recover public key from string: "
                    + privateKeyString, e);
            }
    }

}
