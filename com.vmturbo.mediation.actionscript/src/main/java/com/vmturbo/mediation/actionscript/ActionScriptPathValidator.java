package com.vmturbo.mediation.actionscript;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.keyverifier.AcceptAllServerKeyVerifier;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.client.subsystem.sftp.SftpClient;
import org.apache.sshd.client.subsystem.sftp.SftpClient.DirEntry;
import org.apache.sshd.client.subsystem.sftp.SftpClientFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.vmturbo.mediation.actionscript.exception.KeyValidationException;
import com.vmturbo.mediation.actionscript.exception.RemoteExecutionException;
import com.vmturbo.mediation.actionscript.parameter.ActionScriptParameterDefinition;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.NonMarketEntityType;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.Parameter;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.WorkflowData;
import com.vmturbo.platform.common.dto.NonMarketDTO.NonMarketEntityDTO.WorkflowData.Builder;

/**
 * Class to validate and discover the ActionScript folder.
 *
 * In validation, we test that the path points to a directory and that the directory contents can
 * be listed (i.e. the directory is searchable).
 *
 * In discovery, we search for all files within the provided path that match our naming convention,
 * Discovered scripts will be returned in the discovery response as NonMarketEntityDTOs of type
 * Workflow.
 */
public class ActionScriptPathValidator {

    private static final Logger logger = LogManager.getLogger();

    private static final long SESSION_CONNECT_TIMEOUT_SECS = 10L;
    private static final long AUTH_TIMEOUT_SECS = 15L;
    private static final String WORKFLOW_PREFIX = "AS-WF-";
    @VisibleForTesting
    public static final String WORKFLOW_PARAMETER_TYPE = "text";

    public ActionScriptPathValidator() {

    }

    /**
     * Validate the ActionScript folder at 'actionScriptsRootPath'. Check that the path denotes
     * a directory where the directory contents can be listed (i.e. the directory is searchable.
     *
     * @param accountValues values for this account - address, userid, login token, and path
     * @return a ValidationResponse, containing any validation errors
     */
    public ValidationResponse validateActionScriptPath(@Nonnull final ActionScriptProbeAccount accountValues) {
        try {
            List<ErrorDTO> resultingErrors = executeRemoteCommand(accountValues, this::listRemoteDirectories);
            return ValidationResponse.newBuilder()
                    .addAllErrorDTO(resultingErrors).build();
        } catch (Exception e) {
            return ValidationResponse.newBuilder()
                    .addAllErrorDTO(logError(e.getMessage(), e))
                    .build();
        }
    }

    /**
     * Discovers all action scripts matching our naming convention that are found within the
     * ActionScript folder at 'actionScriptsRootPath'. Discovered scripts will be returned in the
     * discovery response as NonMarketEntityDTOs of type Workflow.
     *
     * @param accountValues values for this account - address, userid, login token, and path
     * @return a DiscoveryResponse, containings discovered scripts as NonMarketEntityDTOs of type
     *     Workflow.
     */
    public DiscoveryResponse discoverActionScripts(@Nonnull final ActionScriptProbeAccount accountValues) {
        try {
            return executeRemoteCommand(accountValues, this::findAllScripts);
        } catch (KeyValidationException | RemoteExecutionException e) {
            return DiscoveryResponse.newBuilder()
                    .addAllErrorDTO(logError(e.getMessage(), e))
                    .build();
        }
    }

    /**
     * An interface for defining remote commands that can be executed within an SSH session
     */
    @FunctionalInterface
    private interface RemoteCommand<T> {
        T execute(ActionScriptProbeAccount accountValues,
                  ClientSession session);
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
    private <T> T executeRemoteCommand(@Nonnull final ActionScriptProbeAccount accountValues,
                                       @Nonnull final RemoteCommand<? extends T> remoteCommand)
            throws KeyValidationException, RemoteExecutionException {
        final String host = accountValues.getNameOrAddress();
        final String userid = accountValues.getUserid();
        final int port = Integer.valueOf(accountValues.getPort());
        final String privateKeyString = accountValues.getPrivateKeyString();
        final KeyPair loginKeyPair;
        try {
            loginKeyPair = SshUtils.extractKeyPair(privateKeyString);
        } catch (GeneralSecurityException | IOException e) {
            throw new KeyValidationException("Cannot recover public key from string: "
                    + privateKeyString, e);
        }

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
                return remoteCommand.execute(accountValues, session);
            } finally {
                client.stop();
            }
        } catch (IOException e) {
            throw new RemoteExecutionException("IO Exception while using the SSH client "
                    + host + ": " + e.getMessage(), e);
        }
    }

    private List<ErrorDTO> listRemoteDirectories(@Nonnull final ActionScriptProbeAccount accountValues,
                                                 @Nonnull final ClientSession session) {
        final SftpClientFactory factory = SftpClientFactory.instance();
        try (SftpClient sftp = factory.createSftpClient(session)) {
            final String actionScriptsRootPath = accountValues.getScriptPath();
            for (DirEntry dirEntry : sftp.readDir(actionScriptsRootPath)) {
                // TODO: this would validate the files / folders, not just list them
                logger.trace("dirEntry {}", dirEntry);
            }
        } catch (IOException e) {
            return logError("IO Exception opening Sftp Client session with " +
                    session.getConnectAddress() + ": " + e.getMessage(), e);
        }
        return Collections.emptyList();
    }

    private DiscoveryResponse findAllScripts(@Nonnull final ActionScriptProbeAccount accountValues,
                                             @Nonnull final ClientSession session) {
        final DiscoveryResponse.Builder discoveryResponseBuilder = DiscoveryResponse.newBuilder();
        final SftpClientFactory factory = SftpClientFactory.instance();
        try (SftpClient sftp = factory.createSftpClient(session)) {
            final String actionScriptsRootPath = accountValues.getScriptPath();
            for (DirEntry dirEntry : sftp.readDir(actionScriptsRootPath)) {
                final String filename = dirEntry.getFilename();
                if (matchesNamingConvention(filename)) {
                    logger.debug("Found script {}", filename);
                    discoveryResponseBuilder.addNonMarketEntityDTO(
                            NonMarketEntityDTO.newBuilder()
                                    .setId(generateWorkflowID(accountValues.nameOrAddress, filename))
                                    .setDisplayName(filename)
                                    .setDescription(dirEntry.getLongFilename())
                                    // TODO: Detect entity type (via naming convention, etc.)
                                    .setWorkflowData(generateWorkflowData(null))
                                    .setEntityType(NonMarketEntityType.WORKFLOW));
                }
            }
        } catch (IOException e) {
            discoveryResponseBuilder.addAllErrorDTO(
                    logError("IO Exception opening Sftp Client session with " +
                            session.getConnectAddress() + ": " + e.getMessage(), e));
        }
        return discoveryResponseBuilder.build();
    }

    private boolean matchesNamingConvention(String filename) {
        // TODO: Check if scripts match our naming convention
        // for now, return true for all files ending in ".sh"
        return filename.endsWith(".sh");
    }

    /**
     * Prepend workflow id with server address.
     *
     * @param targetName name or address of the ActionScript target
     * @param filename the filename of the script being called in this workflow
     * @return id prepended with "AS-WF-" and server name
     */
    private static String generateWorkflowID(@Nonnull String targetName, @Nonnull String filename) {
        return WORKFLOW_PREFIX + targetName + "-" + filename;
    }

    /**
     * Generate workflow data, representing the type of entity a script operates on as well as the
     * variables that can be passed to the script.
     *
     * @param entityType the type of entity the script operates on, or {@code null} if unknown
     * @return a {@code WorkflowData}, representing the type of entity a script operates on as well as the
     *      variables that can be passed to the script.
     */
    private static WorkflowData generateWorkflowData(@Nullable EntityType entityType) {
        final Builder workflowBuilder = WorkflowData.newBuilder();

        // Include the entity type, if it is known
        if (entityType != null) {
            workflowBuilder.setEntityType(entityType);
        }

        // Include the standard variables that are available to all scripts
        Arrays.stream(ActionScriptParameterDefinition.values())
            .forEach(parameter -> workflowBuilder.addParam(generateParameter(parameter.name())));

        return workflowBuilder.build();
    }

    private static Parameter generateParameter(@Nonnull String name) {
        return Parameter.newBuilder()
            .setName(name)
            .setType(WORKFLOW_PARAMETER_TYPE)
            .setMandatory(false)
            .build();
    }

    /**
     * Log an error string to the console and add a new {@link ErrorDTO} to the given errorList.
     *
     * @param errorMessage the message describing the error
     * @param originalException the Exception that triggered the error
     */
    private List<ErrorDTO> logError(String errorMessage, Exception originalException) {
        logger.error(errorMessage, originalException);
        return Lists.newArrayList(ErrorDTO.newBuilder()
            .setSeverity(ErrorSeverity.CRITICAL)
            .setDescription(errorMessage)
            .build());
    }
}