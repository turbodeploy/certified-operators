package com.vmturbo.plan.orchestrator.plan;

import static com.vmturbo.plan.orchestrator.db.tables.PlanInstance.PLAN_INSTANCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.plan.orchestrator.project.PlanProjectDaoImpl;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

/**
 * Unit test for {@link PlanProjectDaoImpl}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {PlanTestConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=plan"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class PlanDaoImplTest {

    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    @Autowired
    private PlanDao planDao;

    @Autowired
    private SettingServiceMole settingServer;

    private DSLContext dsl;

    @Before
    public void setup() throws Exception {
        dsl = dbConfig.dsl();
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

    /**
     * Verified cleaning up time outed plan instances.
     * @throws Exception
     */
    @Test
    public void testCleanUpTimeOutedPlans() throws Exception {
        deleteAllPlanInstances();

        Optional<PlanInstance> inst = planDao.queueNextPlanInstance();
        assertEquals(Optional.empty(), inst);
        // the default time out value is set in factoryInstalledComponents.yml, if it's updated
        // we need to change the defaultTimeOut value here too
        final int defaultTimeOutHour = 6;
        final LocalDateTime createdTime = LocalDateTime.now().minusHours(defaultTimeOutHour + 1);
        // create 6 time outed plan instance.
        createHeadroomPlanInstance(PlanStatus.RUNNING_ANALYSIS, createdTime);
        createHeadroomPlanInstance(PlanStatus.CONSTRUCTING_TOPOLOGY, createdTime);
        createHeadroomPlanInstance(PlanStatus.RUNNING_ANALYSIS, createdTime);
        createHeadroomPlanInstance(PlanStatus.CONSTRUCTING_TOPOLOGY, createdTime);
        createHeadroomPlanInstance(PlanStatus.RUNNING_ANALYSIS, createdTime);
        createHeadroomPlanInstance(PlanStatus.CONSTRUCTING_TOPOLOGY, createdTime);

        int num = planDao.getNumberOfRunningPlanInstances();

        assertEquals(6, num);
        long id1 = planDao.createPlanInstance(CreatePlanRequest.newBuilder()
                .setTopologyId(1L)
                .build()).getPlanId();

        // it will indirectly call PlanDaoImpl#cleanUpFailedInstance, so the previous time outed
        // instance will be clean up (set to FAILED state).
        inst = planDao.queueNextPlanInstance();
        num = planDao.getNumberOfRunningPlanInstances();
        // verify all time outed plan instances have updated status
        assertEquals(0, num);


        Result re = dsl.selectFrom(PLAN_INSTANCE).fetch();
        long failedInstncesCount = re.stream()
                .filter(result -> result.toString().contains(PlanStatus.FAILED.name()))
                .count();
        // verify there are 6 plan instance with 'FAILED' state.
        assertEquals(6L, failedInstncesCount);

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
        createHeadroomPlanInstance(PlanStatus.READY, null);
        createHeadroomPlanInstance(PlanStatus.CONSTRUCTING_TOPOLOGY, null); // <-- Running #1
        createHeadroomPlanInstance(PlanStatus.RUNNING_ANALYSIS, null); // <-- Running #2
        createHeadroomPlanInstance(PlanStatus.WAITING_FOR_RESULT, null); // <-- Running #3
        createHeadroomPlanInstance(PlanStatus.SUCCEEDED, null);


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
                .thenReturn(GetGlobalSettingResponse.newBuilder()
                        .setSetting(Setting.newBuilder()
                                .setNumericSettingValue(NumericSettingValue.newBuilder()
                                        .setValue(1)
                                        .build()))
                        .build());

        // create 3 Headroom plan instances, 2 running and 1 ready
        createHeadroomPlanInstance(PlanStatus.RUNNING_ANALYSIS, null);
        createHeadroomPlanInstance(PlanStatus.CONSTRUCTING_TOPOLOGY, null);
        PlanInstance headroomPlanInstance = createHeadroomPlanInstance(PlanStatus.READY, null);

        PlanInstance inst1 = planDao.createPlanInstance(CreatePlanRequest.newBuilder()
                .setTopologyId(1L)
                .build());
        PlanInstance inst2 = planDao.createPlanInstance(Scenario.getDefaultInstance(),
                PlanProjectType.INITAL_PLACEMENT);

        // User plan instance should always be queued.
        Optional<PlanDTO.PlanInstance> inst = planDao.queuePlanInstance(inst1);
        assertEquals(true, inst.isPresent());
        assertEquals(PlanStatus.QUEUED, inst.get().getStatus());
        Optional<PlanDTO.PlanInstance> initialPlacementPlan = planDao.queuePlanInstance(inst2);
        assertEquals(true, initialPlacementPlan.isPresent());
        assertEquals(PlanStatus.QUEUED, initialPlacementPlan.get().getStatus());

        // When trying to queue an instance that has already been queued, Optional.empty() will be returned.
        inst = planDao.queuePlanInstance(inst1);
        assertEquals(false, inst.isPresent());

        // queue the headroom plan that is in READY state.  Since there are two plan instances
        // running and max number is 1, this plan should stay in READY status.
        inst = planDao.queuePlanInstance(headroomPlanInstance);
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

    @Test
    public void testCollectDiags() throws Exception {

        final PlanInstance first =
            planDao.createPlanInstance(CreatePlanRequest.newBuilder().setTopologyId(1).build());
        final PlanInstance second =
            planDao.createPlanInstance(CreatePlanRequest.newBuilder().setTopologyId(2).build());
        final List<PlanInstance> expected = Arrays.asList(first, second);

        final List<String> result = planDao.collectDiags();
        assertEquals(2, result.size());
        assertTrue(result.stream().map(string -> PlanDaoImpl.GSON.fromJson(string, PlanInstance.class))
            .allMatch(expected::contains));

    }

    @Test
    public void testRestoreDiags() throws Exception {

        final PlanInstance preexisting =
            planDao.createPlanInstance(CreatePlanRequest.newBuilder().setTopologyId(1).build());

        final String first = "{\"planId\":\"1992305997952\",\"topologyId\":\"212\",\"status\":" +
            "\"READY\",\"projectType\":\"USER\"}";
        final String second = "{\"planId\":\"1992305997760\",\"topologyId\":\"646\",\"status\":" +
            "\"READY\",\"projectType\":\"USER\"}";

        try {
            planDao.restoreDiags(Arrays.asList(first, second));
            fail();
        } catch (DiagnosticsException e) {
            assertTrue(e.hasErrors());
            assertEquals(1, e.getErrors().size());
            assertTrue(e.getErrors().get(0).contains("preexisting plan instances"));
        }

        final Set<PlanInstance> result = planDao.getAllPlanInstances();

        assertEquals(2, result.size());
        assertFalse(result.contains(preexisting));
        assertTrue(result.contains(PlanDaoImpl.GSON.fromJson(first, PlanInstance.class)));
        assertTrue(result.contains(PlanDaoImpl.GSON.fromJson(second, PlanInstance.class)));

    }

    /**
     * Delete all records of the plan_instance table.
     *
     * @throws Exception
     */
    private void deleteAllPlanInstances() throws Exception {
        dsl.delete(PLAN_INSTANCE);
    }

    private PlanDTO.PlanInstance createHeadroomPlanInstance(@Nonnull PlanStatus planStatus, LocalDateTime createdTime)
            throws IntegrityException {
        final PlanDTO.PlanInstance.Builder builder = PlanDTO.PlanInstance.newBuilder();
        builder.setTopologyId(1L);
        builder.setPlanId(IdentityGenerator.next());
        builder.setStatus(planStatus);
        builder.setProjectType(PlanProjectType.CLUSTER_HEADROOM);
        final PlanDTO.PlanInstance plan = builder.build();

        final LocalDateTime curTime = createdTime == null ? LocalDateTime.now() : createdTime;
        final com.vmturbo.plan.orchestrator.db.tables.pojos.PlanInstance dbRecord =
                new com.vmturbo.plan.orchestrator.db.tables.pojos.PlanInstance(
                        plan.getPlanId(), curTime, curTime, plan,
                        PlanProjectType.CLUSTER_HEADROOM.name(), planStatus.name());
        dsl.newRecord(PLAN_INSTANCE, dbRecord).store();
        return plan;
    }
}
