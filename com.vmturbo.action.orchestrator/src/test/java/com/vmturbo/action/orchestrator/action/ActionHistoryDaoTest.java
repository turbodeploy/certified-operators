package com.vmturbo.action.orchestrator.action;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.action.orchestrator.TestActionOrchestratorDbEndpointConfig;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision.ExecutionDecision.Reason;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutionStep.Status;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

/**
 * Integration tests related to the {@link ActionHistoryDaoImpl}.
 */
@RunWith(Parameterized.class)
public class ActionHistoryDaoTest extends MultiDbTestBase {

    private static final Clock CLOCK = Clock.systemUTC();
    private final DSLContext dsl;
    private final ActionModeCalculator actionModeCalculator = new ActionModeCalculator();
    ActionHistoryDao actionStore;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect DB dialect to use
     * @throws SQLException if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    public ActionHistoryDaoTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(com.vmturbo.action.orchestrator.db.Action.ACTION, configurableDbDialect, dialect,
                "action-orchestrator",
                TestActionOrchestratorDbEndpointConfig::actionOrchestratorEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    /**
     * Setup tests.
     */
    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        actionStore = new ActionHistoryDaoImpl(dsl, actionModeCalculator, CLOCK, 1000);
    }

    /**
     * Test `getActionHistoryByFilter`.
     */
    @Test
    public void testGetActionHistoryByFilter() {
        final int count = 10;
        for (int i = 0; i < count; i++) {
            actionStore.persistActionHistory(i, getRecommendation(), 777777,
                    LocalDateTime.now(CLOCK), getDecision(), getStep(), 0, "details".getBytes(), 1L,
                    1L, 1L);
        }
        final List<ActionView> actionViews = actionStore.getActionHistoryByFilter(
                getActionQueryFilterDatesBuilder().build());

        assertEquals(count, actionViews.size());
    }

    private ActionDTO.Action getRecommendation() {
        return ActionDTO.Action.newBuilder()
                .setId(1)
                .setInfo(ActionInfo.newBuilder().setResize(Resize.newBuilder()
                        .setTarget(ActionEntity.newBuilder()
                                .setId(73508539136057L)
                                .setType(10)
                                .setEnvironmentType(EnvironmentType.ON_PREM)
                                .build())
                        .setCommodityType(CommodityType.newBuilder().setType(53).build())
                        .build()).build())
                .setDeprecatedImportance(1.0)
                .setExplanation(Explanation.newBuilder()
                        .setResize(ResizeExplanation.newBuilder().setDeprecatedStartUtilization(
                                1.0f).setDeprecatedEndUtilization(0.75f).build())
                        .build())
                .setExecutable(true)
                .setSupportingLevel(SupportLevel.SUPPORTED)
                .build();
    }

    private ActionDecision getDecision() {
        return ActionDecision.newBuilder().setDecisionTime(11111L).setExecutionDecision(
                ExecutionDecision.newBuilder()
                        .setReason(Reason.MANUALLY_ACCEPTED)
                        .setUserUuid("administrator(3139779544672)")
                        .build()).build();
    }

    private ExecutionStep getStep() {
        return ExecutionStep.newBuilder().setEnqueueTime(System.currentTimeMillis()).setStatus(
                Status.QUEUED).setTargetId(1L).setProgressDescription("description").build();
    }

    private ActionQueryFilter.Builder getActionQueryFilterDatesBuilder() {
        return ActionQueryFilter.newBuilder().setStartDate(LocalDateTime.now(CLOCK)
                .minusDays(1)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli()).setEndDate(LocalDateTime.now(CLOCK)
                .plusDays(1)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli());
    }
}
