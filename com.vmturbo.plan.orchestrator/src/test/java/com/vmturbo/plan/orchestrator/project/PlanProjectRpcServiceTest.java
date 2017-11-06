package com.vmturbo.plan.orchestrator.project;

import static com.vmturbo.plan.orchestrator.db.tables.PlanProject.PLAN_PROJECT;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.DeletePlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.DeletePlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.Recurrence;
import com.vmturbo.common.protobuf.plan.PlanDTO.Recurrence.Daily;
import com.vmturbo.common.protobuf.plan.PlanDTO.Recurrence.Schedule;
import com.vmturbo.common.protobuf.plan.PlanDTO.Recurrence.TimeOfRun;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.plan.orchestrator.db.tables.records.PlanProjectRecord;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanProject;

/**
 * Unit test for {@link PlanProjectRpcService}
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=plan"})
public class PlanProjectRpcServiceTest {

    private static final long GET_PLAN_PROJECT_ID = 100L;
    private static final long DELETE_PLAN_PROJECT_ID = 101L;

    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    private Flyway flyway;

    private PlanProjectRpcService planProjectRpcService;

    private PlanProjectDao planProjectDao;

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        planProjectDao = new PlanProjectDaoImpl(dbConfig.dsl(),new IdentityInitializer(0));
        prepareDatabase();
        planProjectRpcService = new PlanProjectRpcService(planProjectDao);
    }

    private void prepareDatabase() throws Exception {
        flyway = dbConfig.flyway();

        // Clean the database and bring it up to the production configuration before running test
        flyway.clean();
        flyway.migrate();

        prePopulatePlanProjectTestData();
    }

    @After
    public void teardown() {
        flyway.clean();
    }

    @Test
    public void testCreatePlanProject() throws Exception {
        Recurrence recurrence = Recurrence.newBuilder()
                .setSchedule(Schedule.newBuilder().setDaily(Daily.newBuilder()))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(5))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setPlanProjectInfoName("Plan Project")
                .setPlanProjectInfoRecurrence(recurrence)
                .build();

        final StreamObserver<PlanDTO.PlanProject> mockObserver =
                Mockito.mock(StreamObserver.class);

        planProjectRpcService.createPlanProject(toCreate, mockObserver);

        verify(mockObserver).onNext(PlanDTO.PlanProject.newBuilder()
                .setPlanProjectId(anyInt())
                .setPlanProjectInfo(toCreate)
                .build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testDeletePlanProject() throws Exception {
        final StreamObserver<DeletePlanProjectResponse> mockObserver = mock(StreamObserver.class);
        DeletePlanProjectResponse response = DeletePlanProjectResponse.newBuilder()
                .setProjectId(DELETE_PLAN_PROJECT_ID) // created with prePopulatePlanProjectTestData()
                .build();
        planProjectRpcService.deletePlanProject(
                DeletePlanProjectRequest.newBuilder().setProjectId(DELETE_PLAN_PROJECT_ID).build(), mockObserver);
        verify(mockObserver).onNext(response);
    }

    @Test
    public void testDeleteNonExistingPlanProject() throws Exception {
        final StreamObserver<DeletePlanProjectResponse> mockObserver = mock(StreamObserver.class);
        planProjectRpcService.deletePlanProject(
                DeletePlanProjectRequest.newBuilder().setProjectId(1).build(), mockObserver);
        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.NOT_FOUND)
                .descriptionContains("1"));
    }


    @Test
    public void testGetPlanProject() throws Exception {
        // created with prePopulatePlanProjectTestData()
        Optional<PlanDTO.PlanProject> project = getProject(dbConfig.dsl(), GET_PLAN_PROJECT_ID);
        final GetPlanProjectResponse expectedResponse = GetPlanProjectResponse.newBuilder()
                .setProject(project.orElse(null)).build();
        final StreamObserver<GetPlanProjectResponse> mockObserver = mock(StreamObserver.class);

        planProjectRpcService.getPlanProject(GetPlanProjectRequest.newBuilder()
                .setProjectId(GET_PLAN_PROJECT_ID)
                .build(), mockObserver);
        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testGetNonExistingPlanProject() throws Exception {
        final GetPlanProjectResponse expectedResponse = GetPlanProjectResponse.newBuilder().build();
        final StreamObserver<GetPlanProjectResponse> mockObserver = mock(StreamObserver.class);

        planProjectRpcService.getPlanProject(GetPlanProjectRequest.newBuilder()
                .setProjectId(1234) // not existed id
                .build(), mockObserver);
        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
    }

    private Optional<PlanDTO.PlanProject> getProject(DSLContext context, final long planProjectId) {
        Optional<PlanProjectRecord> loadedPlanProject = Optional.ofNullable(
                context.selectFrom(PLAN_PROJECT)
                        .where(PLAN_PROJECT.ID.eq(planProjectId))
                        .fetchAny());
        return loadedPlanProject
                .map(record -> toPlanProjectDTO(record.into(
                        PlanProject.class)));
    }

    private PlanDTO.PlanProject toPlanProjectDTO(
            @Nonnull final PlanProject planProject) {
        return PlanDTO.PlanProject.newBuilder()
                .setPlanProjectId(planProject.getId())
                .setPlanProjectInfo(planProject.getProjectInfo())
                .build();
    }

    private void prePopulatePlanProjectTestData() {
        LocalDateTime curTime = LocalDateTime.now();

        Recurrence dailyRecurrence = Recurrence.newBuilder()
                .setSchedule(Recurrence.Schedule.newBuilder().setDaily(Daily.newBuilder()))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(19))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setPlanProjectInfoName("Plan Project")
                .setPlanProjectInfoRecurrence(dailyRecurrence)
                .build();
        // For testing get
        PlanProject getPlanProject = new
                PlanProject(GET_PLAN_PROJECT_ID, curTime, curTime, toCreate);
        dbConfig.dsl().newRecord(PLAN_PROJECT, getPlanProject).store();
        // For testing delete
        PlanProject deletePlanProject = new
                PlanProject(DELETE_PLAN_PROJECT_ID, curTime, curTime, toCreate);
        dbConfig.dsl().newRecord(PLAN_PROJECT, deletePlanProject).store();
    }
}