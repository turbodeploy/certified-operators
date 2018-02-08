package com.vmturbo.plan.orchestrator.plan;

import static com.vmturbo.plan.orchestrator.db.tables.PlanInstance.PLAN_INSTANCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.plan.orchestrator.project.PlanProjectDaoImpl;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Unit test for {@link PlanProjectDaoImpl}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class, PlanTestConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=plan"})
public class PlanDaoImplTest {

    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    @Autowired
    private PlanDao planDao;

    @Autowired
    private SettingServiceMole settingServer;

    private Flyway flyway;

    private DSLContext dsl;

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        prepareDatabase();
        dsl = dbConfig.dsl();
    }

    private void prepareDatabase() throws Exception {
        flyway = dbConfig.flyway();

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testQueueNextPlanInstance() throws Exception {
        deleteAllPlanInstances();

        Optional<PlanInstance> inst = planDao.queueNextPlanInstance();
        assertEquals(Optional.empty(), inst);

        long id1 = planDao.createPlanInstance(CreatePlanRequest.newBuilder()
                .setTopologyId(1L)
                .build()).getPlanId();

        inst = planDao.queueNextPlanInstance();
        assertEquals(true, inst.isPresent());
        assertEquals(PlanStatus.QUEUED, inst.get().getStatus());

        assertEquals(id1, inst.get().getPlanId());
    }

    @Test
    public void testGetNumberOfRunningPlanInstances() throws Exception {
        deleteAllPlanInstances();
        long id1 = planDao.createPlanInstance(CreatePlanRequest.newBuilder()
                .setTopologyId(1L)
                .build()).getPlanId();
        long id2 = planDao.createPlanInstance(CreatePlanRequest.newBuilder()
                .setTopologyId(1L)
                .build()).getPlanId();
        long id3 = planDao.createPlanInstance(CreatePlanRequest.newBuilder()
                .setTopologyId(1L)
                .build()).getPlanId();
        createHeadroomPlanInstance(PlanStatus.READY);
        createHeadroomPlanInstance(PlanStatus.CONSTRUCTING_TOPOLOGY); // <-- Running #1
        createHeadroomPlanInstance(PlanStatus.RUNNING_ANALYSIS); // <-- Running #2
        createHeadroomPlanInstance(PlanStatus.WAITING_FOR_RESULT); // <-- Running #3
        createHeadroomPlanInstance(PlanStatus.SUCCEEDED);


        dsl.update(PLAN_INSTANCE)
                .set(PLAN_INSTANCE.STATUS, PlanStatus.QUEUED.name())
                .where(PLAN_INSTANCE.ID.eq(id1))
                .execute();
        dsl.update(PLAN_INSTANCE)
                .set(PLAN_INSTANCE.STATUS, PlanStatus.SUCCEEDED.name())
                .where(PLAN_INSTANCE.ID.eq(id2))
                .execute();
        dsl.update(PLAN_INSTANCE)
                .set(PLAN_INSTANCE.STATUS, PlanStatus.RUNNING_ANALYSIS.name())
                .where(PLAN_INSTANCE.ID.eq(id3))
                .execute();
        int num = planDao.getNumberOfRunningPlanInstances();

        // Only number of headroom project instances should be counted
        assertEquals(3, num);
    }

    @Test
    public void testQueuePlanInstance() throws Exception {
        when(settingServer.getGlobalSetting(GetSingleGlobalSettingRequest.newBuilder()
                .setSettingSpecName(GlobalSettingSpecs.MaxConcurrentPlanInstances
                        .getSettingName())
                .build()))
                .thenReturn(Setting.newBuilder()
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(1)
                                .build())
                        .build());

        // create 3 Headroom plan instances, 2 running and 1 ready
        createHeadroomPlanInstance(PlanStatus.RUNNING_ANALYSIS);
        createHeadroomPlanInstance(PlanStatus.CONSTRUCTING_TOPOLOGY);
        PlanInstance headroomPlanInstance = createHeadroomPlanInstance(PlanStatus.READY);

        long id1 = planDao.createPlanInstance(CreatePlanRequest.newBuilder()
                .setTopologyId(1L)
                .build()).getPlanId();
        Optional<PlanDTO.PlanInstance> inst = planDao.queuePlanInstance(id1);
        assertEquals(true, inst.isPresent());
        assertEquals(PlanStatus.QUEUED, inst.get().getStatus());

        // throw exception if plan ID is invalid
        boolean exceptionThrown = false;
        try {
            inst = planDao.queuePlanInstance(1234);
        } catch (Exception e) {
            assertTrue(e instanceof NoSuchObjectException);
            exceptionThrown = true;
        }
        assertEquals(true, exceptionThrown);

        // User plan instance should always be queued.
        inst = planDao.queuePlanInstance(id1);
        assertEquals(true, inst.isPresent());
        assertEquals(PlanStatus.QUEUED, inst.get().getStatus());

        // queue the headroom plan that is in READY state.  Since there are two plan instances
        // running and max number is 1, this plan should stay in READY status.
        inst = planDao.queuePlanInstance(headroomPlanInstance.getPlanId());
        assertEquals(false, inst.isPresent());
        assertEquals(PlanStatus.READY, headroomPlanInstance.getStatus());
    }

    @Test
    public void testListeners() throws Exception {
        PlanInstanceQueue planInstanceQueue = Mockito.mock(PlanInstanceQueue.class);
        PlanInstanceCompletionListener listener = Mockito.spy(new PlanInstanceCompletionListener(planInstanceQueue));
        planDao.addStatusListener(listener);
        PlanDTO.PlanInstance inst = planDao.createPlanInstance(CreatePlanRequest.newBuilder()
                .setTopologyId(1L)
                .build());

        // Status of plan instance is changed to RUNNING_ANALYSIS.
        // The listener should be notified of the change, but since the status is not in one of the
        // completed statuses, the method to run the next plan instance should not be called.
        PlanDTO.PlanInstance updatedInst = PlanDTO.PlanInstance.newBuilder(inst)
                .setStatus(PlanStatus.RUNNING_ANALYSIS)
                .setStartTime(System.currentTimeMillis())
                .build();
        planDao.updatePlanInstance(updatedInst.getPlanId(),
                builder -> builder.setStatus(PlanStatus.RUNNING_ANALYSIS)
                        .setStartTime(System.currentTimeMillis()));
        verify(listener).onPlanStatusChanged(any(PlanDTO.PlanInstance.class));

        // verify planInstanceQueue.runNextPlanInstance is not called
        verify(planInstanceQueue, never()).runNextPlanInstance();

        // Status of plan instance is changed to SUCCEEDED. The method runNextPlanInstance should
        // be invoked to execute the next plan instance if one is available.
        updatedInst = PlanDTO.PlanInstance.newBuilder(inst)
                .setStatus(PlanStatus.SUCCEEDED)
                .setStartTime(System.currentTimeMillis())
                .build();
        planDao.updatePlanInstance(updatedInst.getPlanId(),
                builder -> builder.setStatus(PlanStatus.SUCCEEDED)
                        .setStartTime(System.currentTimeMillis()));
        verify(listener, times(2)).onPlanStatusChanged(any(PlanDTO.PlanInstance.class));

        // verify planInstanceQueue.runNextPlanInstance is called
        verify(planInstanceQueue).runNextPlanInstance();
    }

    /**
     * Delete all records of the plan_instance table.
     *
     * @throws Exception
     */
    private void deleteAllPlanInstances() throws Exception {
        dsl.delete(PLAN_INSTANCE);
    }

    private PlanDTO.PlanInstance createHeadroomPlanInstance(@Nonnull PlanStatus planStatus)
            throws IntegrityException {
        final PlanDTO.PlanInstance.Builder builder = PlanDTO.PlanInstance.newBuilder();
        builder.setTopologyId(1L);
        builder.setPlanId(IdentityGenerator.next());
        builder.setStatus(planStatus);
        builder.setProjectType(PlanProjectType.CLUSTER_HEADROOM);
        final PlanDTO.PlanInstance plan = builder.build();

        final LocalDateTime curTime = LocalDateTime.now();
        final com.vmturbo.plan.orchestrator.db.tables.pojos.PlanInstance dbRecord =
                new com.vmturbo.plan.orchestrator.db.tables.pojos.PlanInstance(
                        plan.getPlanId(), curTime, curTime, plan,
                        PlanProjectType.CLUSTER_HEADROOM.name(), planStatus.name());
        dsl.newRecord(PLAN_INSTANCE, dbRecord).store();
        return plan;
    }
}
