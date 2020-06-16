package com.vmturbo.action.orchestrator.action;

import static com.vmturbo.action.orchestrator.db.tables.ActionHistory.ACTION_HISTORY;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockConnection;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.action.orchestrator.db.tables.pojos.ActionHistory;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision.Reason;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;

/**
 * Ensures that ActionHistoryDaoImpl handles responses from jooq correctly.
 */
public class ActionHistoryDaoImplTest {

    /**
     * The unstable id of the action, not to be confused with the stable oid of the action.
     */
    private static final long UNSTALBE_ACTION_ID = 143877300103332L;
    private static final long REALTIME_CONTEXT_ID = 77777L;

    @Mock
    private ActionModeCalculator actionModeCalculator;

    /**
     * Setup the mocks.
     */
    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * The recommendation oid from the database is null when it was created in XL version 7.22.1
     * or earlier. We should be able to gracefully handle this situation without throwing an
     * exception.
     */
    @Test
    public void testNullRecommendationOid() {
        // Initialise your data provider (implementation further down):
        TestCaseJooqProvider provider = new TestCaseJooqProvider();
        MockConnection connection = new MockConnection(provider);

        // Pass the mock connection to a jOOQ DSLContext:
        DSLContext dslContext = DSL.using(connection, SQLDialect.MARIADB);

        ActionHistoryDaoImpl dao = new ActionHistoryDaoImpl(dslContext, actionModeCalculator);
        List<ActionView> actuals = dao.getActionHistoryByDate(LocalDateTime.now(), LocalDateTime.now());

        Assert.assertEquals(1, actuals.size());
        Assert.assertEquals(UNSTALBE_ACTION_ID, actuals.get(0).getId());
        Assert.assertEquals(UNSTALBE_ACTION_ID, actuals.get(0).getRecommendationOid());
    }

    /**
     * Mock provider uses for mock data returned by jooq.
     */
    private static class TestCaseJooqProvider implements MockDataProvider {

        /**
         * Sets up mock data to that the unit tests use.
         *
         * @param ctx not used.
         * @return the mocked data.
         * @throws SQLException should not be thrown.
         */
        @Override
        public MockResult[] execute(MockExecuteContext ctx) throws SQLException {
            DSLContext create = DSL.using(SQLDialect.MARIADB);

            ActionHistory actionHistory = new ActionHistory(
                0L,
                LocalDateTime.now(),
                LocalDateTime.now(),
                REALTIME_CONTEXT_ID,
                // This value was taken from a real instance reproducing the problem
                ActionDTO.Action.newBuilder()
                    .setId(UNSTALBE_ACTION_ID)
                    .setInfo(ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder()
                            .setTarget(ActionEntity.newBuilder()
                                .setId(73508539136057L)
                                .setType(10)
                                .setEnvironmentType(EnvironmentType.ON_PREM)
                                .build())
                            .setCommodityType(CommodityType.newBuilder()
                                .setType(53)
                                .build())
                            .build())
                        .build())
                    .setDeprecatedImportance(1.0)
                    .setExplanation(Explanation.newBuilder()
                        .setResize(ResizeExplanation.newBuilder()
                            .setDeprecatedStartUtilization(1.0f)
                            .setDeprecatedEndUtilization(0.75f)
                            .build())
                        .build())
                    .setExecutable(true)
                    .setSupportingLevel(SupportLevel.SUPPORTED)
                    .build(),
                LocalDateTime.now(),
                // This value was taken from a real instance reproducing the problem
                ActionDecision.newBuilder()
                    .setDecisionTime(1592258964838L)
                    .setExecutionDecision(ExecutionDecision.newBuilder()
                        .setReason(Reason.MANUALLY_ACCEPTED)
                        .setUserUuid("administrator(3139779544672)")
                        .build())
                    .build(),
                // This value was taken from a real instance reproducing the problem
                ExecutionStep.newBuilder()
                    .setEnqueueTime(1592258964835L)
                    .setStartTime(1592258964845L)
                    .setCompletionTime(1592258967425L)
                    .setStatus(Status.FAILED)
                    .addErrors("errors: \"RIGHT_SIZE stanislav_vm_scale_action\\'s Capacity to"
                        + " 4194304 failed: Task failed: Permission to perform this operation was"
                        + " denied.; nested exception is: \\n\\tMethodFault{dynamicType=\\'null\\',"
                        + " dynamicProperty=null, faultCause=null, faultMessage=null}\"\n")
                    .setTargetId(73508536134352L)
                    .setProgressPercentage(100)
                    .setProgressDescription("Failed to complete.")
                    .build(),
                ActionState.FAILED.getNumber(),
                "userNameThatExecutedAction",
                null,
                0L,
                0L,
                null);

            return new MockResult[]{
                new MockResult(create.newRecord(ACTION_HISTORY, actionHistory))
            };
        }
    }
}
