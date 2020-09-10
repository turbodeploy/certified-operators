package com.vmturbo.mediation.actionscript;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.sshd.client.subsystem.sftp.SftpClient.Attributes;

import com.vmturbo.mediation.actionscript.ActionScriptsManifest.ActionScriptDeclaration;
import com.vmturbo.mediation.actionscript.SshUtils.SshRunner;
import com.vmturbo.mediation.actionscript.exception.KeyValidationException;
import com.vmturbo.mediation.actionscript.exception.RemoteExecutionException;
import com.vmturbo.mediation.actionscript.parameter.ActionScriptParameterDefinition;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.Parameter;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;

/**
 * This class is responsible for loading the action scripts manifest and using it to discover action scripts.
 */

public class ActionScriptDiscovery {
    private static final Logger logger = LogManager.getLogger();

    private static final String WORKFLOW_PREFIX = "AS-WF";

    @VisibleForTesting
    static final String WORKFLOW_PARAMETER_TYPE = "text";

    private final ActionScriptProbeAccount accountValues;
    private ActionScriptsManifest manifest = null;

    /**
     * Create a new instance.
     *
     * @param accountValues probe configuration values, including location of manifest file and access credentials
     */
    public ActionScriptDiscovery(@Nonnull ActionScriptProbeAccount accountValues) {
        this.accountValues = Objects.requireNonNull(accountValues);
    }

    /**
     * Validate the manifest file, by loading and parsing it.
     *
     * <p>N.B. This does not validate all the action scripts declared in the manifest.</p>
     *
     * @return a ValidationResponse, containing any validation errors
     */
    public ValidationResponse validateManifestFile() {
        try (SshRunner runner = new SshRunner(accountValues, null)) {
            return validateManifestFile(runner);
        }
    }

    /**
     * Validate the manifest file, using an existing {@link SshRunner} object;
     *
     * @param runner live ssh runner we will use to execute remote commands
     * @return validation status
     */
    ValidationResponse validateManifestFile(SshRunner runner) {
        ErrorDTO errorDTO = null;
        final File manifestFile = new File(accountValues.getManifestPath());
        String extension = FilenameUtils.getExtension(accountValues.getManifestPath());
        if (extension == null) {
            extension = ""; // avoid NPE below
        }
        ObjectMapper mapper = null;
        if (!manifestFile.isAbsolute()) {
            errorDTO = createAndLogErrorDTO(null, "Action script Manifest file path must be an absolute path", null);
        } else {
            switch (extension.toLowerCase()) {
                case "json":
                    mapper = new ObjectMapper();
                    break;
                case "yaml":
                case "yml":
                    mapper = new YAMLMapper();
                    break;
                default:
                    errorDTO = createAndLogErrorDTO(null, "Action Scripts Manifest file name must end in .json or .yaml", null);
                    break;
            }
        }
        if (mapper != null) {
            try {
                String fileContent = SshUtils.getRemoteFileContent(manifestFile.toString(), runner);
                this.manifest = mapper.readValue(fileContent, ActionScriptsManifest.class);
                manifest.setManifestFilePath(accountValues.getManifestPath());
            } catch (RemoteExecutionException | KeyValidationException | IOException e) {
                errorDTO = createAndLogErrorDTO(null, "Failed to load Action Scripts Manifest file: " + e.getMessage(), e);
            }
        }
        final ValidationResponse.Builder response = ValidationResponse.newBuilder();
        if (errorDTO != null) {
            response.addErrorDTO(errorDTO);
        } else if (manifest.getScripts() == null || manifest.getScripts().isEmpty()) {
            response.addErrorDTO(
                createAndLogErrorDTO(null, "Action Scripts Manifest file defines no scripts", null));
        }
        return response.build();
    }

    /**
     * Discover all action declared in the manifest file.
     *
     * @return a DiscoveryResponse, containings discovered scripts as NonMarketEntityDTOs of type Workflow.
     */
    public DiscoveryResponse discoverActionScripts() {
        try (SshRunner runner = new SshRunner(accountValues, null)) {
            return discoverActionScripts(runner);
        }
    }

    /**
     * Discover all actions declared in the manifest file, using an existing {@link SshRunner} object.
     *
     * @param runner live ssh runner we can use to execute remote commands
     * @return discovery response
     */
    DiscoveryResponse discoverActionScripts(SshRunner runner) {
        DiscoveryResponse.Builder response = DiscoveryResponse.newBuilder();
        final ValidationResponse validation = validateManifestFile(runner);
        if (validation.getErrorDTOCount() == 0) {
            for (ActionScriptDeclaration script : manifest.getScripts()) {
                try {
                    response.addWorkflow(validateScriptDeclaration(script, runner));
                } catch (InvalidActionScriptException e) {
                    response.addErrorDTO(createAndLogErrorDTO(generateWorkflowID(script),
                            "Failed to validate action script " + script.getName() + "("
                                    + script.getScriptPath() + ")" + ": " + e.getLocalizedMessage(),
                            e));
                }
            }
        } else {
            response.addAllErrorDTO(validation.getErrorDTOList());
        }
        return response.build();
    }

    private Workflow validateScriptDeclaration(ActionScriptDeclaration script, SshRunner runner) throws InvalidActionScriptException {
        if (script.getName() == null) {
            throw new InvalidActionScriptException("Action script display name is missing");
        }
        if (script.getScriptPath() == null) {
            throw new InvalidActionScriptException("Action script path is missing");
        }
        String fullPath = script.getScriptPath(manifest);
        try {
            if (isExecutableFile(fullPath, runner)) {
                return generateWorkflow(script);
            } else {
                throw new InvalidActionScriptException("Script path does not refer to an executable file on the execution server");
            }
        } catch (RemoteExecutionException | KeyValidationException e) {
            throw new InvalidActionScriptException("Unable to check executability of action script", e);
        }
    }

    private static final int OWNER_EXECUTABLE_MASK = 0100;
    private static final int ALL_USERS_EXECUTABLE_MASK = 0001;

    private boolean isExecutableFile(String path, SshRunner runner) throws RemoteExecutionException, KeyValidationException {
        Attributes attrs = SshUtils.getRemoteFileAttributes(path, runner);
        int perms = attrs.getPermissions();
        String owner = attrs.getOwner();
        boolean ownerExecutable = owner != null && owner.equals(accountValues.getUserid()) && (perms & OWNER_EXECUTABLE_MASK) != 0;
        boolean allUsersExecutable = (perms & ALL_USERS_EXECUTABLE_MASK) != 0;
        return attrs.isRegularFile() && (ownerExecutable || allUsersExecutable);
    }

    /**
     * Generate a workflow id for a declared script.
     *
     * @param script the script declaration from the manifest
     * @return id incorporating the execution host and the script's name and absolute path
     */
    private String generateWorkflowID(@Nonnull ActionScriptDeclaration script) {
        String host = accountValues.getNameOrAddress();
        String name = script.getName();
        String path = script.getScriptPath(manifest);
        return generateWorkflowID(host, name, path);
    }

    @VisibleForTesting
    static String generateWorkflowID(@Nonnull String host, @Nonnull String name, @Nonnull String path) {
        return String.join(":", WORKFLOW_PREFIX, host != null ? host : "", name != null ? name : "", path != null ? path : "");
    }

    /**
     * Generate workflow data for a declared script.
     *
     * @param script the script declaration from the manifest
     * @return a {@code WorkflowData} representing the script
     */
    private Workflow generateWorkflow(@Nonnull ActionScriptDeclaration script) {
        final Workflow.Builder workflowBuilder = Workflow.newBuilder()
            .setId(generateWorkflowID(script))
            .setDisplayName(script.getName())
            .setScriptPath(script.getScriptPath(manifest));
        // Include other fields if present
        if (script.getDescription() != null) {
            workflowBuilder.setDescription(script.getDescription());
        }
        if (script.getEntityType() != null) {
            workflowBuilder.setEntityType(script.getEntityType());
        }
        if (script.getActionType() != null) {
            workflowBuilder.setActionType(script.getActionType());
        }
        if (script.getActionPhase() != null) {
            workflowBuilder.setPhase(script.getActionPhase());
        }
        if (script.getTimeLimitSeconds() != null) {
            workflowBuilder.setTimeLimitSeconds(script.getTimeLimitSeconds());
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
     * Log an error string to the console and return a critical-severity ErrorDTO built from it
     *
     * @param errorMessage      the message describing the error
     * @param originalException the Exception that triggered the error, or null if none
     */
    private ErrorDTO createAndLogErrorDTO(String entityUuid, String errorMessage, Exception originalException) {
        logger.error(errorMessage, originalException);
        final ErrorDTO.Builder builder = ErrorDTO.newBuilder()
            .setSeverity(ErrorSeverity.CRITICAL)
            .setDescription(errorMessage);
        if (entityUuid != null) {
            builder.setEntityUuid(entityUuid);
        }
        return builder.build();
    }

    /**
     * Class to report script validation errors.
     */
    private static class InvalidActionScriptException extends RuntimeException {
        public InvalidActionScriptException(final String message) {
            super(message);
        }

        public InvalidActionScriptException(final String message, final Throwable cause) {
            super(message + (cause != null ? ":" + cause.getMessage() : ""), cause);
        }
    }
}
