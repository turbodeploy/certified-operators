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

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.DeletePlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.DeletePlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetPlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetPlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.Daily;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.Schedule;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.Recurrence.TimeOfRun;
import com.vmturbo.common.protobuf.plan.PlanProjectREST.PlanProject.PlanProjectStatus;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.db.tables.pojos.PlanProject;
import com.vmturbo.plan.orchestrator.db.tables.records.PlanProjectRecord;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Unit test for {@link PlanProjectRpcService}
 */
public class PlanProjectRpcServiceTest {
    /**
     * Rule to create the DB schema and migrate it.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Plan.PLAN);

    /**
     * Rule to automatically cleanup DB data before each test.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    private static final long GET_PLAN_PROJECT_ID = 100L;
    private static final long DELETE_PLAN_PROJECT_ID = 101L;

    private PlanProjectRpcService planProjectRpcService;

    private PlanProjectDao planProjectDao;

    private PlanProjectExecutor planProjectExecutor = mock(PlanProjectExecutor.class);

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        planProjectDao = new PlanProjectDaoImpl(dbConfig.getDslContext(), new IdentityInitializer(0));
        prepareDatabase();
        planProjectRpcService = new PlanProjectRpcService(planProjectDao, planProjectExecutor);
    }

    private void prepareDatabase() throws Exception {
        prePopulatePlanProjectTestData();
    }

    @Test
    public void testCreatePlanProject() throws Exception {
        Recurrence recurrence = Recurrence.newBuilder()
                .setSchedule(Schedule.newBuilder().setDaily(Daily.newBuilder()))
                .setTimeOfRun(TimeOfRun.newBuilder().setHour(5))
                .build();
        PlanProjectInfo toCreate = PlanProjectInfo.newBuilder()
                .setName("Plan Project")
                .setRecurrence(recurrence)
                .build();

        final StreamObserver<PlanProjectOuterClass.PlanProject> mockObserver =
                Mockito.mock(StreamObserver.class);

        planProjectRpcService.createPlanProject(toCreate, mockObserver);

        verify(mockObserver).onNext(PlanProjectOuterClass.PlanProject.newBuilder()
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
        Optional<PlanProjectOuterClass.PlanProject> project = getProject(dbConfig.getDslContext(), GET_PLAN_PROJECT_ID);
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

    private Optional<PlanProjectOuterClass.PlanProject> getProject(DSLContext context, final long planProjectId) {
        Optional<PlanProjectRecord> loadedPlanProject = Optional.ofNullable(
                context.selectFrom(PLAN_PROJECT)
                        .where(PLAN_PROJECT.ID.eq(planProjectId))
                        .fetchAny());
        return loadedPlanProject
                .map(record -> toPlanProjectDTO(record.into(
                        PlanProject.class)));
    }

    private PlanProjectOuterClass.PlanProject toPlanProjectDTO(
            @Nonnull final PlanProject planProject) {
        return PlanProjectOuterClass.PlanProject.newBuilder()
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
                .setName("Plan Project")
                .setRecurrence(dailyRecurrence)
                .build();
        // For testing get
        PlanProject getPlanProject = new
                PlanProject(GET_PLAN_PROJECT_ID, curTime, curTime, toCreate,
                PlanProjectType.CLUSTER_HEADROOM.name(), PlanProjectStatus.UNAVAILABLE.name());
        dbConfig.getDslContext().newRecord(PLAN_PROJECT, getPlanProject).store();
        // For testing delete
        PlanProject deletePlanProject = new
                PlanProject(DELETE_PLAN_PROJECT_ID, curTime, curTime, toCreate,
                PlanProjectType.CLUSTER_HEADROOM.name(), PlanProjectStatus.UNAVAILABLE.name());
        dbConfig.getDslContext().newRecord(PLAN_PROJECT, deletePlanProject).store();
    }
}