package com.vmturbo.action.orchestrator.audit;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.action.orchestrator.action.AuditActionsPersistenceManager;
import com.vmturbo.action.orchestrator.action.AuditedActionInfo;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;

/**
 * Diagnostics dumper/imported for auditedActions.
 */
public class AuditActionsPersistenceDiagnostics implements DiagsRestorable<Void> {

    private static final String EXTERNAL_AUDITED_ACTIONS = "externalAuditedActions";
    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private final AuditActionsPersistenceManager store;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructs auditedActions diagnostics.
     *
     * @param store AuditActionsPersistence store to use to get and persist audited action book keeping.
     */
    public AuditActionsPersistenceDiagnostics(@Nonnull AuditActionsPersistenceManager store) {
        this.store = store;
    }

    @Override
    public void restoreDiags(@Nonnull List<String> diagEntries, @Nullable Void context) throws DiagnosticsException {
        logger.info("Restoring {} audited action book keeping from diags", diagEntries::size);
        final List<AuditedActionInfo> workflows = diagEntries.stream()
            .map(diagEntry -> GSON.fromJson(diagEntry, AuditedActionInfo.class))
            .collect(Collectors.toList());
        try {
            store.persistActions(workflows);
        } catch (ActionStoreOperationException e) {
            throw new DiagnosticsException("Unable to restore audited action book keeping due to a database issue.", e);
        }
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        final Collection<AuditedActionInfo> auditedActions;
        try {
            auditedActions = store.getActions();
        } catch (DataAccessException e) {
            throw new DiagnosticsException("Could not load audited actions from the database", e);
        }

        for (AuditedActionInfo auditedActionInfo : auditedActions) {
            appender.appendString(GSON.toJson(auditedActionInfo));
        }
        logger.info("Successfully exported {} auditedActions to diagnostics", auditedActions::size);
    }

    @Nonnull
    @Override
    public String getFileName() {
        return EXTERNAL_AUDITED_ACTIONS;
    }
}
