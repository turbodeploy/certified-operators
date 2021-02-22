package com.vmturbo.action.orchestrator.audit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.gson.Gson;

import org.jooq.exception.DataAccessException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.action.AuditActionsPersistenceManager;
import com.vmturbo.action.orchestrator.action.AuditedActionInfo;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;

/**
 * Verifies that AuditActionsPersistenceDiagnostics can save and load diagnostics.
 */
public class AuditActionsPersistenceDiagnosticsTest {

    private static final Gson GSON = ComponentGsonFactory.createGsonNoPrettyPrint();

    private static final Void UNUSED_CONTEXT = null;

    private static final long ACTION_OID1 = 1;
    private static final long ACTION_OID2 = 2;
    private static final long ACTION_OID3 = 3;
    private static final long WORKFLOW_OID1 = 4;
    private static final long WORKFLOW_OID2 = 5;
    private static final long WORKFLOW_OID3 = 6;
    private static final long CLEARED_TIMESTAMP = 1000L;
    private static final AuditedActionInfo SENT_RECORD1 =
        new AuditedActionInfo(ACTION_OID1, WORKFLOW_OID1, Optional.empty());
    private static final AuditedActionInfo SENT_RECORD2 =
        new AuditedActionInfo(ACTION_OID2, WORKFLOW_OID2, Optional.empty());
    private static final AuditedActionInfo CLEARED_RECORD1 =
        new AuditedActionInfo(ACTION_OID3, WORKFLOW_OID3, Optional.of(CLEARED_TIMESTAMP));

    /**
     * Input taken from 8.1.1 so that we know which future version breaks backwards compatibility.
     */
    private static final List<String> BACKWARDS_COMPATIBILITY_INPUT = Arrays.asList(
        "{\"recommendationId\":\"1\",\"workflowId\":\"4\",\"clearedTimestamp\":{}}",
        "{\"recommendationId\":\"2\",\"workflowId\":\"5\",\"clearedTimestamp\":{}}",
        "{\"recommendationId\":\"3\",\"workflowId\":\"6\",\"clearedTimestamp\":{\"value\":\"1000\"}}"
    );

    @Mock
    private AuditActionsPersistenceManager auditActionsPersistenceManager;
    @Mock
    private DiagnosticsAppender diagnosticsAppender;

    /**
     * Instance of AuditActionsPersistenceDiagnostics filled in with mock dependencies.
     */
    private AuditActionsPersistenceDiagnostics auditActionsPersistenceDiagnostics;

    /**
     * Initializes the mocks used by the tests.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        auditActionsPersistenceDiagnostics = new AuditActionsPersistenceDiagnostics(auditActionsPersistenceManager);
    }

    /**
     * When there's nothing to save, nothing should be persisted and there should be no failure.
     *
     * @throws DiagnosticsException should not be thrown.
     */
    @Test
    public void testEmptyCollectDiags() throws DiagnosticsException {
        when(auditActionsPersistenceManager.getActions()).thenReturn(Collections.emptyList());
        auditActionsPersistenceDiagnostics.collectDiags(diagnosticsAppender);
        verify(diagnosticsAppender, never()).appendString(anyString());
    }

    /**
     * Should convert each audited action into json.
     *
     * @throws DiagnosticsException should not be thrown.
     */
    @Test
    public void testCollectDiags() throws DiagnosticsException {
        when(auditActionsPersistenceManager.getActions()).thenReturn(Arrays.asList(
            SENT_RECORD1,
            SENT_RECORD2,
            CLEARED_RECORD1
        ));
        auditActionsPersistenceDiagnostics.collectDiags(diagnosticsAppender);
        // should not be called with anything else
        verify(diagnosticsAppender, times(3)).appendString(anyString());
        verify(diagnosticsAppender, times(1)).appendString(GSON.toJson(SENT_RECORD1));
        verify(diagnosticsAppender, times(1)).appendString(GSON.toJson(SENT_RECORD2));
        verify(diagnosticsAppender, times(1)).appendString(GSON.toJson(CLEARED_RECORD1));
    }

    /**
     * Should throw DiagnosticsException when there is a database issue.
     *
     * @throws DiagnosticsException should be thrown.
     */
    @Test(expected = DiagnosticsException.class)
    public void testCollectDiagsDatabaseIssue() throws DiagnosticsException {
        when(auditActionsPersistenceManager.getActions()).thenThrow(new DataAccessException("DB Unavailable"));
        auditActionsPersistenceDiagnostics.collectDiags(diagnosticsAppender);
    }

    /**
     * No exceptions should be thrown if there is no book keeping to restore.
     *
     * @throws DiagnosticsException should not be thrown.
     * @throws ActionStoreOperationException should not be thrown.
     */
    @Test
    public void testEmptyRestoreDiags() throws DiagnosticsException, ActionStoreOperationException {
        auditActionsPersistenceDiagnostics.restoreDiags(Collections.emptyList(), UNUSED_CONTEXT);
        verify(auditActionsPersistenceManager, times(1)).persistActions(eq(
            Collections.emptyList()
        ));
    }

    /**
     * Each diag entry should be restored to the database.
     *
     * @throws DiagnosticsException should not be thrown.
     * @throws ActionStoreOperationException should not be thrown.
     */
    @Test
    public void testRestoreDiags() throws DiagnosticsException, ActionStoreOperationException {
        final List<String> input = Arrays.asList(
            GSON.toJson(SENT_RECORD1),
            GSON.toJson(SENT_RECORD2),
            GSON.toJson(CLEARED_RECORD1)
        );
        auditActionsPersistenceDiagnostics.restoreDiags(input, UNUSED_CONTEXT);
        verify(auditActionsPersistenceManager).persistActions(eq(Arrays.asList(
            SENT_RECORD1,
            SENT_RECORD2,
            CLEARED_RECORD1
        )));
    }

    /**
     * Should throw DiagnosticsException when there is a database issue.
     *
     * @throws DiagnosticsException should be thrown.
     * @throws ActionStoreOperationException should not be thrown.
     */
    @Test(expected = DiagnosticsException.class)
    public void testRestoreDiagsDatabaseIssue() throws ActionStoreOperationException, DiagnosticsException {
        doThrow(new ActionStoreOperationException("DB Unavailable")).when(auditActionsPersistenceManager).persistActions(any());
        auditActionsPersistenceDiagnostics.restoreDiags(Arrays.asList(GSON.toJson(SENT_RECORD1, AuditedActionInfo.class)), null);
    }

    /**
     * Verifies that backwards compatibility with old diags is maintained. If it cannot be
     * maintained, please document which diags versions are compatibility with which XL versions.
     *
     * @throws DiagnosticsException should be thrown.
     * @throws ActionStoreOperationException should not be thrown.
     */
    @Test
    public void testBackwardsCompatibility() throws DiagnosticsException, ActionStoreOperationException {
        auditActionsPersistenceDiagnostics.restoreDiags(BACKWARDS_COMPATIBILITY_INPUT, UNUSED_CONTEXT);
        verify(auditActionsPersistenceManager).persistActions(eq(Arrays.asList(
            SENT_RECORD1,
            SENT_RECORD2,
            CLEARED_RECORD1
        )));
    }
}
