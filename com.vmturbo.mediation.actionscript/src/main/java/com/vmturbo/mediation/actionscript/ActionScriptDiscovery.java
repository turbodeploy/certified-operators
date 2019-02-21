package com.vmturbo.mediation.actionscript;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.FileUtils;
import org.apache.sshd.client.subsystem.sftp.SftpClient.Attributes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.mediation.actionscript.ActionScriptsManifest.ActionScriptDeclaration;
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
        String extension = FileUtils.getFileExtension(new File(accountValues.getManifestPath()));
        ErrorDTO errorDTO = null;
        ObjectMapper mapper = null;
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
        if (mapper != null) {
            try {
                String fileContent = SshUtils.getRemoteFileContent(accountValues.getManifestPath(), accountValues, null);
                this.manifest = mapper.readValue(fileContent, ActionScriptsManifest.class);
                manifest.setManifestFilePath(accountValues.getManifestPath());
            } catch (RemoteExecutionException | KeyValidationException | IOException e) {
                errorDTO = createAndLogErrorDTO(null, "Failed to load Action Scripts Manifest file: " + e.getMessage(), e);
            }
        }
        final ValidationResponse.Builder response = ValidationResponse.newBuilder();
        if (errorDTO != null) {
            response.addErrorDTO(errorDTO);
        }
        return response.build();
    }

    /**
     * Discover all action declared in the manifest file.
     *
     * @return a DiscoveryResponse, containings discovered scripts as NonMarketEntityDTOs of type Workflow.
     */
    public DiscoveryResponse discoverActionScripts() {
        DiscoveryResponse.Builder response = DiscoveryResponse.newBuilder();
        if (manifest != null) {
            for (ActionScriptDeclaration script : manifest.getScripts()) {
                try {
                    response.addWorkflow(validateScriptDeclaration(script));
                } catch (Exception e) {
                    response.addErrorDTO(createAndLogErrorDTO(generateWorkflowID(script), "Failed to validate action script " + script.getName() + ": " + e.getLocalizedMessage(),
                        // some script validations cause a RuntimeException to be thrown, but we really don't want to include an
                        // underlying exception in this case; we just want the message.
                        e.getClass() != RuntimeException.class ? e : null));
                }
            }
        } else {
            response.addErrorDTO(createAndLogErrorDTO(null,"Action scripts manifest must be loaded prior to discovery.", null));
        }
        return response.build();
    }

    private Workflow validateScriptDeclaration(ActionScriptDeclaration script) throws RemoteExecutionException, KeyValidationException {
        if (script.getName() == null) {
            throw new RuntimeException("Action script display name is missing");
        }
        if (script.getScriptPath() == null) {
            throw new RuntimeException("Action script path is missing");
        }
        String path = script.getScriptPath(manifest);
        if (isExecutableFile(path)) {
            return generateWorkflow(script);
        } else {
            throw new RuntimeException("Script path does not refer to an executable file on the execution server");
        }
    }

    private boolean isExecutableFile(String path) throws RemoteExecutionException, KeyValidationException {
        Attributes attrs = SshUtils.getRemoteFileAttributes(path, accountValues, null);
        int perms = attrs.getPermissions();
        String owner = attrs.getOwner();
        boolean userEx = owner != null && owner.equals(accountValues.getUserid()) && (perms & 0100) != 0;
        boolean allEx = (perms & 0001) != 0;
        return attrs.isRegularFile() && (userEx || allEx);
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
    static String generateWorkflowID(@Nonnull String host, @Nonnull String name,  @Nonnull String path) {
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
}
