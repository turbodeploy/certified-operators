package com.vmturbo.mediation.actionscript;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO.ActionType;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow.ActionScriptPhase;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Representation of an action scripts manifest file.
 *
 * <p>This class is the target representation for JSON/YAML parsers loading the file</p>
 */

public class ActionScriptsManifest {

    private List<ActionScriptDeclaration> scripts;

    @JsonIgnore
    private String manifestFilePath = null;

    public List<ActionScriptDeclaration> getScripts() {
        return scripts;
    }

    public void setScripts(final List<ActionScriptDeclaration> scripts) {
        this.scripts = scripts;
    }

    public String getManifestFilePath() {
        return manifestFilePath;
    }

    public void setManifestFilePath(final String manifestFilePath) {
        this.manifestFilePath = manifestFilePath;
    }

    public File getScriptDefaultDir() {
        return manifestFilePath != null ? new File(manifestFilePath).getParentFile() : null;
    }

    public static class ActionScriptDeclaration {
        private String name;
        private String description;
        private String scriptPath;
        private EntityType entityType;
        private ActionType actionType;
        private ActionScriptPhase actionPhase;
        private Long timeLimitSeconds;
        private boolean apiMessageFormatEnabled = false;

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(final String description) {
            this.description = description;
        }

        public String getScriptPath() {
            return scriptPath;
        }

        public String getScriptPath(ActionScriptsManifest manifest) {
            if (scriptPath == null) {
                return null;
            }
            File file = new File(scriptPath);
            File base = manifest.getScriptDefaultDir();
            if (!file.isAbsolute() && base != null) {
                try {
                    return new File(base, scriptPath).getCanonicalPath();
                } catch (IOException e) {
                    // canonicalization failed... skip it
                    return new File(base, scriptPath).getAbsolutePath();
                }
            } else {
                return scriptPath;
            }
        }

        public void setScriptPath(final String scriptPath) {
            this.scriptPath = scriptPath;
        }


        public EntityType getEntityType() {
            return entityType;
        }

        public void setEntityType(final EntityType entityType) {
            this.entityType = entityType;
        }

        public ActionType getActionType() {
            return actionType;
        }

        public void setActionType(final ActionType actionType) {
            this.actionType = actionType;
        }

        public ActionScriptPhase getActionPhase() {
            return actionPhase;
        }

        public void setActionPhase(final ActionScriptPhase actionPhase) {
            this.actionPhase = actionPhase;
        }

        public Long getTimeLimitSeconds() {
            return timeLimitSeconds;
        }

        public void setTimeLimitSeconds(final Long timeLimitSeconds) {
            this.timeLimitSeconds = timeLimitSeconds;
        }

        /**
         * If set as true workflow sends the information about the action in API message format.
         *
         * @return should send the message in API message format.
         */
        public boolean isApiMessageFormatEnabled() {
            return apiMessageFormatEnabled;
        }

        /**
         * Sets if we should send the message to ActionScript in API message format.
         *
         * @param apiMessageFormatEnabled if we should sent the message in API message format.
         */
        public void setApiMessageFormatEnabled(Boolean apiMessageFormatEnabled) {
            this.apiMessageFormatEnabled = apiMessageFormatEnabled;
        }
    }
}
