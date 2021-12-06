package com.vmturbo.plan.orchestrator.plan.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.GetPlanDestinationResponse;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.GetPlanDestinationsRequest;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestination;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestinationCriteria;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanDestinationID;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportRequest;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportResponse;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.PlanExportStatus.PlanExportState;
import com.vmturbo.common.protobuf.plan.PlanExportDTO.StoreDiscoveredPlanDestinationsResponse;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.DestinationEntityType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.MigrationReference;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.topology.PlanExport.PlanExportToTargetRequest;
import com.vmturbo.common.protobuf.topology.PlanExport.PlanExportToTargetResponse;
import com.vmturbo.common.protobuf.topology.PlanExportMoles.PlanExportToTargetServiceMole;
import com.vmturbo.common.protobuf.topology.PlanExportToTargetServiceGrpc;
import com.vmturbo.common.protobuf.topology.PlanExportToTargetServiceGrpc.PlanExportToTargetServiceBlockingStub;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.commons.idgen.IdentityInitializer;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.plan.orchestrator.db.Plan;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileDaoImplTest.TestPlanOrchestratorDBEndpointConfig;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit test for {@link PlanExportRpcService}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestPlanOrchestratorDBEndpointConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"sqlDialect=MARIADB"})
public class PlanExportRpcServiceTest {

    @Autowired(required = false)
    private TestPlanOrchestratorDBEndpointConfig dbEndpointConfig;

    private static final long UNKNOWN_DESTINATION_ID = 707;
    private static final long IN_PROGRESS_DESTINATION_ID = 717;
    private static final long VALID_DESTINATION_ID = 727;
    private static final long EXCEPTION_DESTINATION_ID = 737;
    private static final long VALID_MARKET_ID_1 = 747;
    private static final long VALID_MARKET_ID_2 = 757;
    private static final long OTHER_MARKET_ID = 767;
    private static final long UNKNOWN_MARKET_ID = 777;

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

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @Rule
    public DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("tp");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    /**
     * Rule to enable Mockito features/annotations in this unit test.
     */
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    /**
     * PlanExportRpcService object.
     */
    private PlanExportRpcService planExportRpcService;

    /**
     * Mock DAOs.
     */
    private PlanDestinationDao planDestinationDaoMock;
    private PlanDao planDaoMock;

    /**
     * Real DAO.
     */
    private PlanDestinationDao planDestinationDao;

    private TestPlanExportToTargetService tpPlanExportRpcService;
    private PlanExportToTargetServiceBlockingStub tpPlanExportService;
    private ExecutorService exportExecutor;
    private PlanExportNotificationSender planExportNotificationSender;

    @Captor
    ArgumentCaptor<PlanExportResponse> responseCaptor;

    @Captor
    ArgumentCaptor<PlanExportStatus> statusCaptor;

    @Captor
    ArgumentCaptor<PlanDestination> destinationCaptor;

    private DSLContext dsl;

    // Even if it's an {@link ExternalResource} we need to start and close it manually because with
    // the FeatureFlags.POSTGRES_PRIMARY_DB it needs to be recreated for every flag value combination
    // This is also because some tests are counting how many methods are accessed on that instance
    private GrpcTestServer grpcTestServer;

    /**
     * Setup method.
     *
     * @throws Exception any exception on setup.
     */
    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        planDestinationDaoMock = mock(PlanDestinationDao.class);
        planDaoMock = mock(PlanDao.class);

        tpPlanExportRpcService = spy(TestPlanExportToTargetService.class);
        grpcTestServer = GrpcTestServer.newServer(tpPlanExportRpcService);
        grpcTestServer.start();

        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("plan-export-starter-%d")
            .build();
        exportExecutor = Executors.newFixedThreadPool(1, threadFactory);

        tpPlanExportService =
            PlanExportToTargetServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        planExportNotificationSender = mock(PlanExportNotificationSender.class);

        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbEndpointConfig.planEndpoint());
            dsl = dbEndpointConfig.planEndpoint().dslContext();
        } else {
            dsl = dbConfig.getDslContext();
        }
    }

    /**
     * Tears down the test env/setup.
     *
     * @throws Exception any exception on teardown.
     */
    @After
    public void teardown() throws Exception {
        grpcTestServer.close();
    }

    /**
     * Test getPlanDestination(final PlanDestinationID planDestinationID, ...).
     * When the corresponding Oid is not found in DB.
     */
    @Test
    public void testGetPlanDestinationWithOIdNotFound() {
        // Given
        final long oid = 777777L;
        final PlanDestinationID planDestinationID = PlanDestinationID.newBuilder().setId(oid).build();
        final StreamObserver<GetPlanDestinationResponse> mockObserver = mock(StreamObserver.class);

        when(planDestinationDaoMock.getPlanDestination(anyLong())).thenReturn(Optional.empty());

        planExportRpcService = exportRpcService();
        planExportRpcService.getPlanDestination(planDestinationID, mockObserver);

        // Verify
        final GetPlanDestinationResponse expectedResponse = GetPlanDestinationResponse.newBuilder().build();

        verify(planDestinationDaoMock, times(1)).getPlanDestination(anyLong());
        verifyNoMoreInteractions(planDestinationDaoMock);
        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
        verifyNoMoreInteractions(mockObserver);
    }

    /**
     * Test getPlanDestination(final PlanDestinationID planDestinationID, ...).
     * When the corresponding Oid is found in DB.
     */
    @Test
    public void testGetPlanDestinationWithOIdFound() {
        // Given
        final long oid = 888888L;
        final long targetId = 1234567L;
        final long accountId = 666666L;
        final long marketId = 777777L;

        PlanDestination planDestination = PlanDestination.newBuilder()
            .setOid(oid)
            .setDisplayName("DisplayName")
            .setExternalId("externalid")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId).build())
            .build();

        when(planDestinationDaoMock.getPlanDestination(anyLong())).thenReturn(Optional.of(planDestination));

        planExportRpcService = exportRpcService();
        final PlanDestinationID planDestinationID = PlanDestinationID.newBuilder().setId(oid).build();
        final StreamObserver<GetPlanDestinationResponse> mockObserver = mock(StreamObserver.class);
        planExportRpcService.getPlanDestination(planDestinationID, mockObserver);

        // Verify
        final GetPlanDestinationResponse expectedResponse = GetPlanDestinationResponse.newBuilder()
            .setDestination(planDestination)
            .build();

        verify(planDestinationDaoMock, times(1)).getPlanDestination(anyLong());
        verifyNoMoreInteractions(planDestinationDaoMock);

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, times(1)).onNext(expectedResponse);
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);
    }

    /**
     * Test getPlanDestination(final PlanDestinationID planDestinationID, ...).
     * When the corresponding Oid is not found in DB. Using real DAO.
     */
    @Test
    public void testGetPlanDestinationWithOIdNotFoundWithRealDao() {
        planDestinationDao = new PlanDestinationDaoImpl(dsl, new IdentityInitializer(0));

        // Given
        final long oid = 777777L;
        final PlanDestinationID planDestinationID = PlanDestinationID.newBuilder().setId(oid).build();
        final StreamObserver<GetPlanDestinationResponse> mockObserver = mock(StreamObserver.class);

        planExportRpcService = exportRpcService(planDestinationDao);
        planExportRpcService.getPlanDestination(planDestinationID, mockObserver);

        // Verify
        final GetPlanDestinationResponse expectedResponse = GetPlanDestinationResponse.newBuilder().build();

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver).onNext(expectedResponse);
        verify(mockObserver).onCompleted();
        verifyNoMoreInteractions(mockObserver);
    }

    /**
     * Test getPlanDestination(final PlanDestinationID planDestinationID, ...).
     * When the corresponding Oid is found in DB.
     *
     * @throws IntegrityException any DB issue from DAO.
     */
    @Test
    public void testGetPlanDestinationWithOIdFoundWithRealDao() throws IntegrityException {
        planDestinationDao = new PlanDestinationDaoImpl(dsl, new IdentityInitializer(0));

        // Given
        final long targetId = 1234567L;
        final long accountId = 666666L;
        final long marketId = 777777L;

        final StreamObserver<GetPlanDestinationResponse> mockObserver = mock(StreamObserver.class);

        PlanDestination planDestination = PlanDestination.newBuilder()
            .setDisplayName("DisplayName")
            .setExternalId("externalid")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId).build())
            .build();
        PlanDestination createdPlanDestination = planDestinationDao.createPlanDestination(planDestination);

        final PlanDestinationID planDestinationID = PlanDestinationID.newBuilder().setId(createdPlanDestination.getOid()).build();
        planExportRpcService = exportRpcService(planDestinationDao);
        planExportRpcService.getPlanDestination(planDestinationID, mockObserver);

        // Verify
        final GetPlanDestinationResponse expectedResponse = GetPlanDestinationResponse.newBuilder()
            .setDestination(createdPlanDestination)
            .build();

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, times(1)).onNext(expectedResponse);
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);
    }

    /**
     * Test getPlanDestinations() with no query param.  DB has no data.
     */
    @Test
    public void testGetPlanDestinationsWithNoParamGivenNoDestinationInDB() {
        when(planDestinationDaoMock.getPlanDestination(anyLong())).thenReturn(Optional.empty());

        planExportRpcService = exportRpcService();
        final StreamObserver<PlanDestination> mockObserver = mock(StreamObserver.class);
        final GetPlanDestinationsRequest request = GetPlanDestinationsRequest.newBuilder().build();
        planExportRpcService.getPlanDestinations(request, mockObserver);

        // Verify
        final PlanDestination expectedResponse = PlanDestination.newBuilder().build();

        verify(planDestinationDaoMock, times(1)).getAllPlanDestinations();
        verifyNoMoreInteractions(planDestinationDaoMock);

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, never()).onNext(any(PlanDestination.class));
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);
    }

    /**
     * Test getPlanDestinations() with no query param.  DB has data.
     */
    @Test
    public void testGetPlanDestinationsWithNoParamGivenSomeDestinationsInDB() {
        // Given
        final long oid1 = 888888L;
        final long oid2 = 888889L;
        final long targetId = 1234567L;
        final long accountId1 = 666666L;
        final long accountId2 = 666667L;
        final long marketId = 777777L;

        PlanDestination planDestination1 = PlanDestination.newBuilder()
            .setOid(oid1)
            .setDisplayName("DisplayName1")
            .setExternalId("externalid1")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId1).build())
            .build();
        PlanDestination planDestination2 = PlanDestination.newBuilder()
            .setOid(oid2)
            .setDisplayName("DisplayName2")
            .setExternalId("externalid2")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId2).build())
            .build();

        when(planDestinationDaoMock.getAllPlanDestinations())
            .thenReturn(Arrays.asList(planDestination1, planDestination2));

        when(planDestinationDaoMock.getPlanDestination(anyLong())).thenReturn(Optional.empty());

        planExportRpcService = exportRpcService();
        final StreamObserver<PlanDestination> mockObserver = mock(StreamObserver.class);
        // No args
        final GetPlanDestinationsRequest request = GetPlanDestinationsRequest.newBuilder().build();
        planExportRpcService.getPlanDestinations(request, mockObserver);

        // Verify
        verify(planDestinationDaoMock, times(1)).getAllPlanDestinations();
        verifyNoMoreInteractions(planDestinationDaoMock);

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, times(1)).onNext(planDestination1);
        verify(mockObserver, times(1)).onNext(planDestination2);
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);
    }

    /**
     * Test getPlanDestinations() with account id query param.  DB has no data.
     */
    @Test
    public void testGetPlanDestinationsWithAccountIdParamGivenSomeDestinationsInDB() {
        // Given
        final long oid1 = 888888L;
        final long oid2 = 888889L;
        final long targetId = 1234567L;
        final long accountId1 = 666666L;
        final long accountId2 = 666667L;
        final long marketId = 777777L;

        PlanDestination planDestination1 = PlanDestination.newBuilder()
            .setOid(oid1)
            .setDisplayName("DisplayName1")
            .setExternalId("externalid1")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId1).build())
            .build();
        PlanDestination planDestination2 = PlanDestination.newBuilder()
            .setOid(oid2)
            .setDisplayName("DisplayName2")
            .setExternalId("externalid2")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId2).build())
            .build();

        when(planDestinationDaoMock.getAllPlanDestinations())
            .thenReturn(Arrays.asList(planDestination1, planDestination2));

        when(planDestinationDaoMock.getPlanDestination(anyLong())).thenReturn(Optional.empty());

        planExportRpcService = exportRpcService();
        final StreamObserver<PlanDestination> mockObserver = mock(StreamObserver.class);
        // No args
        final GetPlanDestinationsRequest request = GetPlanDestinationsRequest.newBuilder().setAccountId(accountId1).build();
        planExportRpcService.getPlanDestinations(request, mockObserver);

        // Verify
        verify(planDestinationDaoMock, times(1)).getAllPlanDestinations();
        verifyNoMoreInteractions(planDestinationDaoMock);

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, times(1)).onNext(planDestination1);
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);
    }

    /**
     * Test storeDiscoveredPlanDestinations when DB was empty.
     *
     * @throws IntegrityException when there is error in DAO.
     */
    @Test
    public void testStoreDiscoveredPlanDestinationsEmptyDb() throws IntegrityException {
        // Given
        final long oid1 = 888888L;
        final long oid2 = 888889L;
        final long targetId = 1234567L;
        final long accountId1 = 666666L;
        final long accountId2 = 666667L;
        final long marketId = 777777L;

        planExportRpcService = exportRpcService();

        PlanDestination planDestination1 = PlanDestination.newBuilder()
            .setDisplayName("DisplayName1")
            .setExternalId("externalid1")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId1).build())
            .build();
        PlanDestination planDestination2 = PlanDestination.newBuilder()
            .setDisplayName("DisplayName2")
            .setExternalId("externalid2")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId2).build())
            .build();

        when(planDestinationDaoMock.getAllPlanDestinations()).thenReturn(Collections.emptyList());
        when(planDestinationDaoMock.createPlanDestination(eq(planDestination1))).thenReturn(PlanDestination.newBuilder(planDestination1).setOid(oid1).build());
        when(planDestinationDaoMock.createPlanDestination(eq(planDestination2))).thenReturn(PlanDestination.newBuilder(planDestination2).setOid(oid2).build());

        final StreamObserver<StoreDiscoveredPlanDestinationsResponse> mockObserver = mock(StreamObserver.class);

        StreamObserver<PlanDestination> planDestinationStreamObserver = planExportRpcService.storeDiscoveredPlanDestinations(mockObserver);
        planDestinationStreamObserver.onNext(planDestination1);
        planDestinationStreamObserver.onNext(planDestination2);
        planDestinationStreamObserver.onCompleted();

        // Check interaction with Dao.
        verify(planDestinationDaoMock, times(1)).getAllPlanDestinations();
        verify(planDestinationDaoMock, times(2)).createPlanDestination(any(PlanDestination.class));
        verifyNoMoreInteractions(planDestinationDaoMock);

        ArgumentCaptor<StoreDiscoveredPlanDestinationsResponse> storeDiscoveredPlanDestinationsResponseArgumentCaptor =
            ArgumentCaptor.forClass(StoreDiscoveredPlanDestinationsResponse.class);

        // Check observer response stored properly.
        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, times( 1)).onNext(storeDiscoveredPlanDestinationsResponseArgumentCaptor.capture());
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);

        // Check two responses are stored in the observer.
        List<StoreDiscoveredPlanDestinationsResponse> responses = storeDiscoveredPlanDestinationsResponseArgumentCaptor.getAllValues();
        assertNotNull(responses);
        assertEquals(1, responses.size());

        assertNotNull(responses.get(0));
    }

    /**
     * Test storeDiscoveredPlanDestinations when DB when one of the records was in the DB.
     *
     * @throws IntegrityException when error in dao.
     * @throws NoSuchObjectException when object not found in DB during update.
     */
    @Test
    public void testStoreDiscoveredPlanDestinationsWhenOneRecordWasInDB() throws IntegrityException, NoSuchObjectException {
        // Given
        final long oid1 = 888888L;
        final long oid2 = 888889L;
        final long targetId = 1234567L;
        final long accountId1 = 666666L;
        final long accountId2 = 666667L;
        final long marketId = 777777L;

        planExportRpcService = exportRpcService();
        PlanDestination planDestination1 = PlanDestination.newBuilder()
            .setOid(oid1)
            .setDisplayName("DisplayName1")
            .setExternalId("externalid1")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId1).build())
            .build();
        PlanDestination planDestination2 = PlanDestination.newBuilder()
            .setDisplayName("DisplayName2")
            .setExternalId("externalid2")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId2).build())
            .build();

        // planDestination1 was in DB
        when(planDestinationDaoMock.getAllPlanDestinations()).thenReturn(Arrays.asList(planDestination1));
        when(planDestinationDaoMock.updatePlanDestination(eq(oid1), eq(planDestination1))).thenReturn(PlanDestination.newBuilder(planDestination1).build());
        when(planDestinationDaoMock.createPlanDestination(eq(planDestination2))).thenReturn(PlanDestination.newBuilder(planDestination2).setOid(oid2).build());

        final StreamObserver<StoreDiscoveredPlanDestinationsResponse> mockObserver = mock(StreamObserver.class);
        StreamObserver<PlanDestination> planDestinationStreamObserver = planExportRpcService.storeDiscoveredPlanDestinations(mockObserver);
        planDestinationStreamObserver.onNext(planDestination1);
        planDestinationStreamObserver.onNext(planDestination2);
        planDestinationStreamObserver.onCompleted();

        // Check interaction with Dao.
        verify(planDestinationDaoMock, times(1)).getAllPlanDestinations();
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(oid1), eq(planDestination1));
        verify(planDestinationDaoMock, times(1)).createPlanDestination(eq(planDestination2));
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Check observer response stored properly.
        ArgumentCaptor<StoreDiscoveredPlanDestinationsResponse> storeDiscoveredPlanDestinationsResponseArgumentCaptor =
            ArgumentCaptor.forClass(StoreDiscoveredPlanDestinationsResponse.class);
        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, times(1)).onNext(storeDiscoveredPlanDestinationsResponseArgumentCaptor.capture());
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);

        // Check two responses are stored in the observer.
        List<StoreDiscoveredPlanDestinationsResponse> responses = storeDiscoveredPlanDestinationsResponseArgumentCaptor.getAllValues();
        assertNotNull(responses);
        assertEquals(1, responses.size());
        assertNotNull(responses.get(0));
    }

    /**
     * Test storeDiscoveredPlanDestinations when DB when one of the records was in the DB, and one previously stored record is not in discovery.
     *
     * @throws IntegrityException when error in dao.
     * @throws NoSuchObjectException when object not found in DB during update.
     */
    @Test
    public void testStoreDiscoveredPlanDestinationsWhenOneRecordWasInDBAndOneRecordShouldBeRemovedFromDB() throws IntegrityException, NoSuchObjectException {
        // Given
        final long oid1 = 888888L;
        final long oid2 = 888889L;
        final long oid3 = 888887L;
        final long targetId = 1234567L;
        final long accountId1 = 666666L;
        final long accountId2 = 666667L;
        final long accountId3  = 666668L;
        final long marketId = 777777L;

        planExportRpcService = exportRpcService();

        PlanDestination planDestination1 = PlanDestination.newBuilder()
            .setOid(oid1)
            .setDisplayName("Record in both DB and Discovery")
            .setExternalId("externalid1")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId1).build())
            .build();
        PlanDestination planDestination2 = PlanDestination.newBuilder()
            .setDisplayName("Record not in DB but in Discovery")
            .setExternalId("externalid2")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId2).build())
            .build();
        PlanDestination planDestination3 = PlanDestination.newBuilder()
            .setOid(oid3)
            .setDisplayName("Record IN DB but not in discovery")
            .setExternalId("externalid3")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId3).build())
            .build();

        // planDestination1 and planDestination3 were in DB
        when(planDestinationDaoMock.getAllPlanDestinations()).thenReturn(Arrays.asList(planDestination1, planDestination3));
        when(planDestinationDaoMock.updatePlanDestination(eq(oid1), eq(planDestination1))).thenReturn(PlanDestination.newBuilder(planDestination1).build());
        when(planDestinationDaoMock.deletePlanDestination(eq(oid3))).thenReturn(1);
        when(planDestinationDaoMock.createPlanDestination(eq(planDestination2))).thenReturn(PlanDestination.newBuilder(planDestination2).setOid(oid2).build());

        final StreamObserver<StoreDiscoveredPlanDestinationsResponse> mockObserver = mock(StreamObserver.class);
        StreamObserver<PlanDestination> planDestinationStreamObserver = planExportRpcService.storeDiscoveredPlanDestinations(mockObserver);
        planDestinationStreamObserver.onNext(planDestination1);
        planDestinationStreamObserver.onNext(planDestination2);
        planDestinationStreamObserver.onCompleted();

        // Check interaction with Dao.
        verify(planDestinationDaoMock, times(1)).getAllPlanDestinations();
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(oid1), eq(planDestination1));
        verify(planDestinationDaoMock, times(1)).createPlanDestination(eq(planDestination2));
        verify(planDestinationDaoMock, times(1)).deletePlanDestination(eq(oid3));
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Check observer response stored properly.
        ArgumentCaptor<StoreDiscoveredPlanDestinationsResponse> storeDiscoveredPlanDestinationsResponseArgumentCaptor =
            ArgumentCaptor.forClass(StoreDiscoveredPlanDestinationsResponse.class);

        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, times(1)).onNext(storeDiscoveredPlanDestinationsResponseArgumentCaptor.capture());
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);

        // Check two responses are stored in the observer.
        List<StoreDiscoveredPlanDestinationsResponse> responses = storeDiscoveredPlanDestinationsResponseArgumentCaptor.getAllValues();
        assertNotNull(responses);
        assertEquals(1, responses.size());
        assertNotNull(responses.get(0));
    }

    /**
     * Test storeDiscoveredPlanDestinations when DB when one of the records was in the DB,
     * and one previously stored record is not in discovery but is in progress state.
     *
     * @throws IntegrityException when error in dao.
     * @throws NoSuchObjectException when object not found in DB during update.
     */
    @Test
    public void testStoreDiscoveredPlanDestinationsWhenOneRecordWasInDBAndOneRecordNotInDBShouldNotBeRemoved() throws IntegrityException, NoSuchObjectException {
        // Given
        final long oid1 = 888888L;
        final long oid2 = 888889L;
        final long oid3 = 888887L;
        final long targetId = 1234567L;
        final long accountId1 = 666666L;
        final long accountId2 = 666667L;
        final long accountId3  = 666668L;
        final long marketId = 777777L;

        PlanDestination planDestination1 = PlanDestination.newBuilder()
            .setOid(oid1)
            .setDisplayName("Record in both DB and Discovery")
            .setExternalId("externalid1")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId1).build())
            .build();
        PlanDestination planDestination2 = PlanDestination.newBuilder()
            .setDisplayName("Record not in DB but in Discovery")
            .setExternalId("externalid2")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId2).build())
            .build();
        PlanDestination planDestination3 = PlanDestination.newBuilder()
            .setOid(oid3)
            .setDisplayName("Record IN DB but not in discovery, but is in progress state")
            .setExternalId("externalid3")
            .setStatus(PlanExportStatus.newBuilder().setProgress(30).setState(PlanExportState.IN_PROGRESS))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId3).build())
            .build();

        planExportRpcService = exportRpcService();
        // planDestination1 and planDestination3 were in DB
        when(planDestinationDaoMock.getAllPlanDestinations()).thenReturn(Arrays.asList(planDestination1, planDestination3));
        when(planDestinationDaoMock.createPlanDestination(eq(planDestination2))).thenReturn(PlanDestination.newBuilder(planDestination2).setOid(oid2).build());
        when(planDestinationDaoMock.updatePlanDestination(eq(oid1), eq(planDestination1))).thenReturn(PlanDestination.newBuilder(planDestination1).build());

        final StreamObserver<StoreDiscoveredPlanDestinationsResponse> mockObserver = mock(StreamObserver.class);
        StreamObserver<PlanDestination> planDestinationStreamObserver = planExportRpcService.storeDiscoveredPlanDestinations(mockObserver);
        planDestinationStreamObserver.onNext(planDestination1);
        planDestinationStreamObserver.onNext(planDestination2);
        planDestinationStreamObserver.onCompleted();

        // Check interaction with Dao.
        verify(planDestinationDaoMock, times(1)).getAllPlanDestinations();
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(oid1), eq(planDestination1));
        verify(planDestinationDaoMock, times(1)).createPlanDestination(eq(planDestination2));
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Check observer response stored properly.
        ArgumentCaptor<StoreDiscoveredPlanDestinationsResponse> storeDiscoveredPlanDestinationsResponseArgumentCaptor =
            ArgumentCaptor.forClass(StoreDiscoveredPlanDestinationsResponse.class);
        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, times(1)).onNext(storeDiscoveredPlanDestinationsResponseArgumentCaptor.capture());
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);

        // Check two responses are stored in the observer.
        List<StoreDiscoveredPlanDestinationsResponse> responses = storeDiscoveredPlanDestinationsResponseArgumentCaptor.getAllValues();
        assertNotNull(responses);
        assertEquals(1, responses.size());
        assertNotNull(responses.get(0));
    }

    /**
     * Test storeDiscoveredPlanDestinations when DB was empty, exception throw when insert error.
     *
     *  @throws IntegrityException when error in dao.
     */
    @Test
    public void testStoreDiscoveredPlanDestinationsEmptyDbWithException() throws IntegrityException {
        // Given
        final long oid1 = 888888L;
        final long oid2 = 888889L;
        final long targetId = 1234567L;
        final long accountId1 = 666666L;
        final long accountId2 = 666667L;
        final long marketId = 777777L;

        planExportRpcService = exportRpcService();
        PlanDestination planDestination1 = PlanDestination.newBuilder()
            .setDisplayName("DisplayName1")
            .setExternalId("externalid1")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId1).build())
            .build();
        PlanDestination planDestination2 = PlanDestination.newBuilder()
            .setDisplayName("DisplayName2")
            .setExternalId("externalid2")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId2).build())
            .build();

        when(planDestinationDaoMock.getAllPlanDestinations()).thenReturn(Collections.emptyList());
        when(planDestinationDaoMock.createPlanDestination(eq(planDestination1))).thenThrow(new IntegrityException("unable to insert record"));
        when(planDestinationDaoMock.createPlanDestination(eq(planDestination2))).thenReturn(PlanDestination.newBuilder(planDestination2).setOid(oid2).build());

        final StreamObserver<StoreDiscoveredPlanDestinationsResponse> mockObserver = mock(StreamObserver.class);
        StreamObserver<PlanDestination> planDestinationStreamObserver = planExportRpcService.storeDiscoveredPlanDestinations(mockObserver);
        planDestinationStreamObserver.onNext(planDestination1);
        planDestinationStreamObserver.onNext(planDestination2);
        planDestinationStreamObserver.onCompleted();

        // Check interaction with Dao.
        verify(planDestinationDaoMock, times(1)).getAllPlanDestinations();
        verify(planDestinationDaoMock, times(2)).createPlanDestination(any(PlanDestination.class));
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Check observer response stored properly.
        ArgumentCaptor<StoreDiscoveredPlanDestinationsResponse> storeDiscoveredPlanDestinationsResponseArgumentCaptor =
            ArgumentCaptor.forClass(StoreDiscoveredPlanDestinationsResponse.class);
        ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);

        verify(mockObserver, times(1)).onError(exceptionArgumentCaptor.capture());
        verify(mockObserver, times(1)).onNext(storeDiscoveredPlanDestinationsResponseArgumentCaptor.capture());
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);

        // Check exception throw.
        Exception result = exceptionArgumentCaptor.getValue();
        assertEquals(StatusException.class, result.getClass());
        assertEquals("UNKNOWN: Unable to persist: unable to insert record", result.getMessage());

        // Check one response is stored in the observer.
        List<StoreDiscoveredPlanDestinationsResponse> responses = storeDiscoveredPlanDestinationsResponseArgumentCaptor.getAllValues();
        assertNotNull(responses);
        assertEquals(1, responses.size());
        assertNotNull(responses.get(0));
    }

    /**
     * Test storeDiscoveredPlanDestinations when DB when records are in DB but unable to update.
     *
     * @throws IntegrityException when error in dao.
     * @throws NoSuchObjectException when object not found in DB during update.
     */
    @Test
    public void testStoreDiscoveredPlanDestinationsWhenErrorDuringUpdate() throws IntegrityException, NoSuchObjectException {
        // Given
        final long oid1 = 888888L;
        final long oid2 = 888889L;
        final long targetId = 1234567L;
        final long accountId1 = 666666L;
        final long accountId2 = 666667L;
        final long marketId = 777777L;

        PlanDestination planDestination1 = PlanDestination.newBuilder()
            .setOid(oid1)
            .setDisplayName("DisplayName1")
            .setExternalId("externalid1")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId1).build())
            .build();
        PlanDestination planDestination2 = PlanDestination.newBuilder()
            .setOid(oid2)
            .setDisplayName("DisplayName2")
            .setExternalId("externalid2")
            .setStatus(PlanExportStatus.newBuilder().setProgress(0).setState(PlanExportState.NONE))
            .setMarketId(marketId)
            .setTargetId(targetId)
            .setCriteria(PlanDestinationCriteria.newBuilder().setAccountId(accountId2).build())
            .build();

        planExportRpcService = exportRpcService();
        // both planDestination were in DB
        when(planDestinationDaoMock.getAllPlanDestinations()).thenReturn(Arrays.asList(planDestination1, planDestination2));
        when(planDestinationDaoMock.updatePlanDestination(eq(oid1), eq(planDestination1))).thenThrow(new NoSuchObjectException(oid1 + ""));
        when(planDestinationDaoMock.updatePlanDestination(eq(oid2), eq(planDestination2))).thenThrow(new IntegrityException(oid2 + ""));

        final StreamObserver<StoreDiscoveredPlanDestinationsResponse> mockObserver = mock(StreamObserver.class);

        StreamObserver<PlanDestination> planDestinationStreamObserver = planExportRpcService.storeDiscoveredPlanDestinations(mockObserver);
        planDestinationStreamObserver.onNext(planDestination1);
        planDestinationStreamObserver.onNext(planDestination2);
        planDestinationStreamObserver.onCompleted();

        // Check interaction with Dao.
        verify(planDestinationDaoMock, times(1)).getAllPlanDestinations();
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(oid1), eq(planDestination1));
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(oid2), eq(planDestination2));
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Check observer response stored properly.
        ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);
        ArgumentCaptor<StoreDiscoveredPlanDestinationsResponse> storeDiscoveredPlanDestinationsResponseArgumentCaptor =
            ArgumentCaptor.forClass(StoreDiscoveredPlanDestinationsResponse.class);
        verify(mockObserver, times(2)).onError(exceptionArgumentCaptor.capture());
        verify(mockObserver, times(1)).onNext(storeDiscoveredPlanDestinationsResponseArgumentCaptor.capture());
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);

        List<Exception> exceptions = exceptionArgumentCaptor.getAllValues();
        assertNotNull(exceptions);
        assertEquals(2, exceptions.size());

        Exception exception1 = exceptions.get(0);
        assertEquals(StatusException.class, exception1.getClass());
        assertEquals("NOT_FOUND: Unable to find pre-existing record: " + oid1, exception1.getMessage());
        Exception exception2 = exceptions.get(1);
        assertEquals(StatusException.class, exception2.getClass());
        assertEquals("UNKNOWN: Unable to persist: " + oid2, exception2.getMessage());

        List<StoreDiscoveredPlanDestinationsResponse> responses = storeDiscoveredPlanDestinationsResponseArgumentCaptor.getAllValues();
        assertNotNull(responses);
        assertEquals(1, responses.size());
        assertNotNull(responses.get(0));
    }

    /**
     * Test the plan export initiation RPC when the destination is unknown.
     *
     * @throws Exception if there is a failure.
     */
    @Test
    public void testStartPlanExportUnknownDestination() throws Exception {

        when(planDestinationDaoMock.getPlanDestination(eq(UNKNOWN_DESTINATION_ID)))
            .thenReturn(Optional.empty());

        planExportRpcService = exportRpcService();
        final StreamObserver<PlanExportResponse> mockObserver = mock(StreamObserver.class);
        final PlanExportRequest request = planExportRequest(UNKNOWN_DESTINATION_ID, VALID_MARKET_ID_1);

        planExportRpcService.startPlanExport(request, mockObserver);

        verify(mockObserver, never()).onError(any(Exception.class));
        // Default instance has no destination included, meaning the destination didn't exist
        verify(mockObserver, times(1)).onNext(eq(PlanExportResponse.getDefaultInstance()));
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);

        // No updates are sent because there is no destination to send them about
        verify(planExportNotificationSender, never()).onPlanDestinationProgress(any());
        verify(planExportNotificationSender, never()).onPlanDestinationStateChanged(any());
    }

    /**
     * Test the plan export initiation RPC when the destination is unknown.
     *
     * @throws Exception if there is a failure.
     */
    @Test
    public void testStartPlanExportAlreadyInProgress() throws Exception {
        when(planDestinationDaoMock.getPlanDestination(eq(IN_PROGRESS_DESTINATION_ID)))
            .thenReturn(Optional.of(makeDestination(IN_PROGRESS_DESTINATION_ID, VALID_MARKET_ID_1,
                PlanExportState.IN_PROGRESS, 42, "Running")));

        planExportRpcService = exportRpcService();
        final StreamObserver<PlanExportResponse> mockObserver = mock(StreamObserver.class);

        planExportRpcService.startPlanExport(
            planExportRequest(IN_PROGRESS_DESTINATION_ID, OTHER_MARKET_ID), mockObserver);
        verifyStreamObserver(mockObserver);

        // The request should have been rejected
        PlanExportStatus status = responseCaptor.getValue().getDestination().getStatus();
        checkStatus(status, PlanExportState.REJECTED);

        // Return the market ID of the in-progress update, not this new one.
        // The caller already knows what market it wanted to export, so it's more useful
        // to know the ID of the market currently being exported.
        assertEquals(VALID_MARKET_ID_1, responseCaptor.getValue().getDestination().getMarketId());

        // Check that this status was only returned and that we did not try to update
        // the destination, which would lose information about the in-progress export.
        verify(planDestinationDaoMock, times(1)).getPlanDestination(eq(IN_PROGRESS_DESTINATION_ID));
        verify(planDestinationDaoMock, never()).updatePlanDestination(eq(IN_PROGRESS_DESTINATION_ID), any());
        verify(planDestinationDaoMock, never()).updatePlanDestination(
            eq(IN_PROGRESS_DESTINATION_ID), any(), anyLong());
        verifyNoMoreInteractions(planDestinationDaoMock);

        // No update sent because it would be confusing for the initiator of the export that's
        // in progress to see a rejected message, when their export is still running OK.
        // Meanwhile the initiator of the new export attempt shouldn't need an async notification
        // about it because they will have received an immediate rejection response to the HTTP call.
        verify(planExportNotificationSender, never()).onPlanDestinationProgress(any());
        verify(planExportNotificationSender, never()).onPlanDestinationStateChanged(any());
    }

    /**
     * Test the plan export initiation RPC when the plan is unknown.
     *
     * @throws Exception if there is a failure.
     */
    @Test
    public void testStartPlanExportUnknownPlan() throws Exception {
        when(planDestinationDaoMock.getPlanDestination(eq(VALID_DESTINATION_ID)))
            .thenReturn(Optional.of(makeDestination(VALID_DESTINATION_ID, VALID_MARKET_ID_1,
                PlanExportState.NONE, 0, "")));

        when(planDestinationDaoMock.updatePlanDestination(eq(VALID_DESTINATION_ID), any(),
        isNull(Long.class)))
            .thenReturn(makeDestination(VALID_DESTINATION_ID, VALID_MARKET_ID_1,
                PlanExportState.REJECTED, 0, "unknown market"));

        when(planDaoMock.getPlanInstance(eq(UNKNOWN_MARKET_ID)))
            .thenReturn(Optional.empty());

        planExportRpcService = exportRpcService();
        final StreamObserver<PlanExportResponse> mockObserver = mock(StreamObserver.class);

        planExportRpcService.startPlanExport(
            planExportRequest(VALID_DESTINATION_ID, UNKNOWN_MARKET_ID), mockObserver);
        verifyStreamObserver(mockObserver);

        // The request should have been rejected
        PlanExportStatus status = responseCaptor.getValue().getDestination().getStatus();
        checkStatus(status, PlanExportState.REJECTED);

        // We should not have updated the market ID to the new bogus one
        assertEquals(VALID_MARKET_ID_1, responseCaptor.getValue().getDestination().getMarketId());

        // Check that the database was updated
        verify(planDestinationDaoMock, times(1)).getPlanDestination(eq(VALID_DESTINATION_ID));
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(VALID_DESTINATION_ID),
                statusCaptor.capture(), isNull(Long.class));
        checkStatus(statusCaptor.getValue(), PlanExportState.REJECTED);
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Verify that a notification was sent
        verify(planExportNotificationSender, never()).onPlanDestinationProgress(any());
        verify(planExportNotificationSender, times(1))
            .onPlanDestinationStateChanged(destinationCaptor.capture());

        checkStatus(destinationCaptor.getValue().getStatus(), PlanExportState.REJECTED);
        assertEquals(VALID_MARKET_ID_1, destinationCaptor.getValue().getMarketId());
    }

    /**
     * Test the plan export initiation RPC witht a validation failure.
     * See testValidatePlanExport below for more thorough tests of the
     * validation itself, this test just to verifies that the RPC fails if
     * validation fails.
     *
     * @throws Exception if there is a failure.
     */
    @Test
    public void testStartPlanExportValidationFailure() throws Exception {
        when(planDestinationDaoMock.getPlanDestination(eq(VALID_DESTINATION_ID)))
            .thenReturn(Optional.of(makeDestination(VALID_DESTINATION_ID, VALID_MARKET_ID_1,
                PlanExportState.NONE, 0, "")));

        when(planDestinationDaoMock.updatePlanDestination(eq(VALID_DESTINATION_ID), any(),
            eq(VALID_MARKET_ID_2)))
            .thenReturn(makeDestination(VALID_DESTINATION_ID, VALID_MARKET_ID_2,
                PlanExportState.REJECTED, 0, "failed validation"));

        // An invalid plan for export.
        when(planDaoMock.getPlanInstance(eq(VALID_MARKET_ID_2)))
            .thenReturn(Optional.of(
                planBuilder(PlanStatus.FAILED, PlanProjectType.USER).build()));

        planExportRpcService = exportRpcService();
        final StreamObserver<PlanExportResponse> mockObserver = mock(StreamObserver.class);

        planExportRpcService.startPlanExport(
            planExportRequest(VALID_DESTINATION_ID, VALID_MARKET_ID_2), mockObserver);
        verifyStreamObserver(mockObserver);

        // The request should have been rejected
        PlanExportStatus status = responseCaptor.getValue().getDestination().getStatus();
        checkStatus(status, PlanExportState.REJECTED);

        // We should have updated the market ID to the new one, since it was a real market
        assertEquals(VALID_MARKET_ID_2, responseCaptor.getValue().getDestination().getMarketId());

        // Check that the database was updated
        verify(planDestinationDaoMock, times(1)).getPlanDestination(eq(VALID_DESTINATION_ID));
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(VALID_DESTINATION_ID),
            statusCaptor.capture(), eq(VALID_MARKET_ID_2));
        checkStatus(statusCaptor.getValue(), PlanExportState.REJECTED);
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Verify that a notification was sent
        verify(planExportNotificationSender, never()).onPlanDestinationProgress(any());
        verify(planExportNotificationSender, times(1))
            .onPlanDestinationStateChanged(destinationCaptor.capture());

        checkStatus(destinationCaptor.getValue().getStatus(), PlanExportState.REJECTED);
        assertEquals(VALID_MARKET_ID_2, destinationCaptor.getValue().getMarketId());
    }

    /**
     * Test the plan export initiation RPC where GRPC to TP throws an exception.
     *
     * @throws Exception if there is a failure.
     */
    @Test
    public void testStartPlanExportGrpcFailure() throws Exception {
        when(planDestinationDaoMock.getPlanDestination(eq(EXCEPTION_DESTINATION_ID)))
            .thenReturn(Optional.of(makeDestination(EXCEPTION_DESTINATION_ID, VALID_MARKET_ID_1,
                PlanExportState.NONE, 0, "")));

        when(planDestinationDaoMock.updatePlanDestination(eq(EXCEPTION_DESTINATION_ID), any(),
            eq(VALID_MARKET_ID_2)))
            .thenReturn(makeDestination(EXCEPTION_DESTINATION_ID, VALID_MARKET_ID_2,
                PlanExportState.FAILED, 0, "error"));

        when(planDaoMock.getPlanInstance(eq(VALID_MARKET_ID_2)))
            .thenReturn(Optional.of(planBuilder().build()));

        planExportRpcService = exportRpcService();
        final StreamObserver<PlanExportResponse> mockObserver = mock(StreamObserver.class);

        planExportRpcService.startPlanExport(
            planExportRequest(EXCEPTION_DESTINATION_ID, VALID_MARKET_ID_2), mockObserver);
        verifyStreamObserver(mockObserver);

        // The request should fail
        PlanExportStatus status = responseCaptor.getValue().getDestination().getStatus();
        checkStatus(status, PlanExportState.FAILED);

        // We should have updated the market ID to the new one
        assertEquals(VALID_MARKET_ID_2, responseCaptor.getValue().getDestination().getMarketId());

        // Check that the database was updated
        verify(planDestinationDaoMock, times(1)).getPlanDestination(eq(EXCEPTION_DESTINATION_ID));
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(EXCEPTION_DESTINATION_ID),
            statusCaptor.capture(), eq(VALID_MARKET_ID_2));
        checkStatus(statusCaptor.getValue(), PlanExportState.FAILED);
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Verify that a notification was sent
        verify(planExportNotificationSender, never()).onPlanDestinationProgress(any());
        verify(planExportNotificationSender, times(1))
            .onPlanDestinationStateChanged(destinationCaptor.capture());

        checkStatus(destinationCaptor.getValue().getStatus(), PlanExportState.FAILED);
        assertEquals(VALID_MARKET_ID_2, destinationCaptor.getValue().getMarketId());
    }

    /**
     * Test the plan export initiation RPC whit a validation failure.
     * See testValidatePlanExport below for more thorough tests of the
     * validation itself, this test just to verifies that the RPC fails if
     * validation fails.
     *
     * @throws Exception if there is a failure.
     */
    @Test
    public void testStartPlanExportSuccess() throws Exception {
        when(planDestinationDaoMock.getPlanDestination(eq(VALID_DESTINATION_ID)))
            .thenReturn(Optional.of(makeDestination(VALID_DESTINATION_ID, VALID_MARKET_ID_1,
                PlanExportState.NONE, 0, "")));

        when(planDestinationDaoMock.updatePlanDestination(eq(VALID_DESTINATION_ID), any(),
            eq(VALID_MARKET_ID_2)))
            .thenReturn(makeDestination(VALID_DESTINATION_ID, VALID_MARKET_ID_2,
                PlanExportState.IN_PROGRESS, 0, "starting"));

        when(planDaoMock.getPlanInstance(eq(VALID_MARKET_ID_2)))
            .thenReturn(Optional.of(planBuilder().build()));

        planExportRpcService = exportRpcService();
        final StreamObserver<PlanExportResponse> mockObserver = mock(StreamObserver.class);

        planExportRpcService.startPlanExport(
            planExportRequest(VALID_DESTINATION_ID, VALID_MARKET_ID_2), mockObserver);
        verifyStreamObserver(mockObserver);

        PlanExportStatus status = responseCaptor.getValue().getDestination().getStatus();
        checkStatus(status, PlanExportState.IN_PROGRESS);

        // We should have updated the market ID to the new one
        assertEquals(VALID_MARKET_ID_2, responseCaptor.getValue().getDestination().getMarketId());

        // Check that the database was updated
        verify(planDestinationDaoMock, times(1)).getPlanDestination(eq(VALID_DESTINATION_ID));
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(VALID_DESTINATION_ID),
                statusCaptor.capture(), eq(VALID_MARKET_ID_2));
        checkStatus(statusCaptor.getValue(), PlanExportState.IN_PROGRESS);
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Verify that a notification was sent
        verify(planExportNotificationSender, times(1))
            .onPlanDestinationProgress(destinationCaptor.capture());
        verify(planExportNotificationSender, never()).onPlanDestinationStateChanged(any());

        checkStatus(destinationCaptor.getValue().getStatus(), PlanExportState.IN_PROGRESS);
        assertEquals(VALID_MARKET_ID_2, destinationCaptor.getValue().getMarketId());

        verify(tpPlanExportRpcService, times(1)).exportPlan(any());
    }

    /**
     * Test validation of plan vs plan destination compatibility checks.
     */
    @Test
    public void testValidatePlanExport() {
        final long accountId1 = 481516L;
        final long accountId2 = 2342L;

        PlanExportRequest request = PlanExportRequest.getDefaultInstance();

        PlanDestination destinationNoAccountNeeded = PlanDestination.newBuilder()
            .setOid(123L)
            .setDisplayName("foo")
            .setExternalId("bad")
            .build();

        // Can only export a successfully finished plan (FAIL)
        for (PlanStatus status : PlanStatus.values()) {
            if (status != PlanStatus.SUCCEEDED) {
                assertNotNull(PlanExportRpcService.validatePlanExport(
                    request,
                    planBuilder(status, PlanProjectType.CLOUD_MIGRATION).build(),
                    destinationNoAccountNeeded));
            }
        }

        // Can currently only export cloud migration plans (FAIL)
        for (PlanProjectType type : PlanProjectType.values()) {
            if (type != PlanProjectType.CLOUD_MIGRATION) {
                assertNotNull(PlanExportRpcService.validatePlanExport(
                    request,
                    planBuilder(PlanStatus.SUCCEEDED, type).build(),
                    destinationNoAccountNeeded));
            }
        }

        // If the destination does not have an account criteria, then no account is needed. (OK)

        assertNull(PlanExportRpcService.validatePlanExport(
            request, planBuilder().build(), destinationNoAccountNeeded));

        PlanDestination destinationHasCriteriaButNoAccountNeeded = PlanDestination.newBuilder()
            .setOid(123L)
            .setDisplayName("foo")
            .setExternalId("bad")
            .setCriteria(PlanDestinationCriteria.newBuilder().build())
            .build();

        assertNull(PlanExportRpcService.validatePlanExport(
            request, planBuilder().build(), destinationHasCriteriaButNoAccountNeeded));

        // Destination has a criteria of account 1, but plan has no associated account
        // (No no migration ScenarioChange). (FAIL)

        PlanDestination destinationWithAccount1Criteria = PlanDestination.newBuilder()
            .setOid(123L)
            .setDisplayName("foo")
            .setExternalId("bad")
            .setCriteria(PlanDestinationCriteria.newBuilder()
                .setAccountId(accountId1).build())
            .build();

        assertNotNull(PlanExportRpcService.validatePlanExport(
            request, planBuilder().setScenario(migrationScenario(null)).build(),
            destinationWithAccount1Criteria));

        // Destination has a criteria of account 1, but plan has migration scenario
        // without a DestiationAccount. (FAIL)

        assertNotNull(PlanExportRpcService.validatePlanExport(
            request, planBuilder().setScenario(
                migrationScenario(TopologyMigration.newBuilder())).build(),
            destinationWithAccount1Criteria));

        // Destination has a criteria of account 1, but plan has migration scenario
        // with a DestiationAccount that does not specify an oid. (FAIL)

        assertNotNull(PlanExportRpcService.validatePlanExport(
            request, planBuilder().setScenario(
                migrationScenario(TopologyMigration.newBuilder()
                .setDestinationAccount(MigrationReference.getDefaultInstance()))).build(),
            destinationWithAccount1Criteria));

        // Destination has a criteria of account 1, but plan has migration scenario
        // with a DestiationAccount that specifies account 2. (FAIL)

        assertNotNull(PlanExportRpcService.validatePlanExport(
            request, planBuilder().setScenario(
                migrationScenario(TopologyMigration.newBuilder()
                    .setDestinationAccount(MigrationReference.newBuilder()
                        .setOid(accountId2)))).build(),
            destinationWithAccount1Criteria));

        // Destination has a criteria of account 1, and plan has migration scenario
        // with a DestiationAccount that also specifies account 1. (OK)

        assertNull(PlanExportRpcService.validatePlanExport(
            request, planBuilder().setScenario(
                migrationScenario(TopologyMigration.newBuilder()
                    .setDestinationAccount(MigrationReference.newBuilder()
                        .setOid(accountId1)))).build(),
            destinationWithAccount1Criteria));
    }

    /**
     * Verify handling of received progress messages.
     *
     * @throws Exception if there is an error
     */
    @Test
    public void testOnPlanDestinationProgress() throws Exception {
        when(planDestinationDaoMock.updatePlanDestination(eq(VALID_DESTINATION_ID), any(),
            isNull(Long.class)))
            .thenReturn(makeDestination(VALID_DESTINATION_ID, VALID_MARKET_ID_2,
                PlanExportState.IN_PROGRESS, 42, "Working"));

        planExportRpcService = exportRpcService();
        planExportRpcService.onPlanExportProgress(VALID_DESTINATION_ID, 42, "Working");

        // Verify that the DB was updated
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(VALID_DESTINATION_ID),
            statusCaptor.capture(), isNull(Long.class));
        checkStatus(statusCaptor.getValue(), PlanExportState.IN_PROGRESS, 42);
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Verify that a notification was sent
        verify(planExportNotificationSender, times(1))
            .onPlanDestinationProgress(destinationCaptor.capture());
        verify(planExportNotificationSender, never()).onPlanDestinationStateChanged(any());

        checkStatus(destinationCaptor.getValue().getStatus(), PlanExportState.IN_PROGRESS, 42);
        assertEquals(VALID_DESTINATION_ID, destinationCaptor.getValue().getOid());
    }

    /**
     * Verify handling of received state changes.
     *
     * @throws Exception if there is an error
     */
    @Test
    public void testOnPlanExportStateChanged() throws Exception {
        when(planDestinationDaoMock.updatePlanDestination(eq(VALID_DESTINATION_ID), any(),
            isNull(Long.class)))
            .thenReturn(makeDestination(VALID_DESTINATION_ID, VALID_MARKET_ID_2,
                PlanExportState.SUCCEEDED, 100, "OK!"));

        planExportRpcService = exportRpcService();
        planExportRpcService.onPlanExportStateChanged(VALID_DESTINATION_ID,
            makeStatus(PlanExportState.SUCCEEDED, 100, "OK!"));

        // Verify that the DB was updated
        verify(planDestinationDaoMock, times(1)).updatePlanDestination(eq(VALID_DESTINATION_ID),
            statusCaptor.capture(), isNull(Long.class));
        checkStatus(statusCaptor.getValue(), PlanExportState.SUCCEEDED, 100);
        verifyNoMoreInteractions(planDestinationDaoMock);

        // Verify that a notification was sent
        verify(planExportNotificationSender, never()).onPlanDestinationProgress(any());
        verify(planExportNotificationSender, times(1))
            .onPlanDestinationStateChanged(destinationCaptor.capture());

        checkStatus(destinationCaptor.getValue().getStatus(), PlanExportState.SUCCEEDED, 100);
        assertEquals(VALID_DESTINATION_ID, destinationCaptor.getValue().getOid());
    }

    @Nonnull
    private PlanInstance.Builder planBuilder(@Nonnull PlanStatus status,
                                             @Nonnull PlanProjectType type) {
        return PlanInstance.newBuilder()
            .setPlanId(42L)
            .setStatus(status)
            .setProjectType(type);
    }

    @Nonnull PlanInstance.Builder planBuilder() {
        return planBuilder(PlanStatus.SUCCEEDED, PlanProjectType.CLOUD_MIGRATION);
    }

    private PlanExportRpcService exportRpcService() {
        return new PlanExportRpcService(planDestinationDaoMock,
            planDaoMock,
            tpPlanExportService,
            planExportNotificationSender);
    }

    private PlanExportRpcService exportRpcService(PlanDestinationDao dao) {
        return new PlanExportRpcService(dao,
            planDaoMock,
            tpPlanExportService,
            planExportNotificationSender);
    }

    private Scenario migrationScenario(@Nullable TopologyMigration.Builder migration) {
        ScenarioInfo.Builder info = ScenarioInfo.newBuilder();
        if (migration != null) {
            info.addChanges(ScenarioChange.newBuilder()
                .setTopologyMigration(migration.setDestinationEntityType(
                    DestinationEntityType.VIRTUAL_MACHINE))
                .build());
        }

        return Scenario.newBuilder()
            .setScenarioInfo(info.build())
            .build();
    }

    private void verifyStreamObserver(final StreamObserver<PlanExportResponse> mockObserver) {
        verify(mockObserver, never()).onError(any(Exception.class));
        verify(mockObserver, times(1)).onNext(responseCaptor.capture());
        verify(mockObserver, times(1)).onCompleted();
        verifyNoMoreInteractions(mockObserver);
    }

    private void checkStatus(final PlanExportStatus status, final PlanExportState state) {
        checkStatus(status, state, 0);
    }

    private void checkStatus(final PlanExportStatus status, final PlanExportState state,
                             int progress) {
        assertEquals(state, status.getState());
        assertEquals(progress, status.getProgress());
        assertFalse(status.getDescription().isEmpty());
    }

    @NotNull
    private PlanExportRequest planExportRequest(final long validDestinationId, final long validMarketId2) {
        return PlanExportRequest.newBuilder()
            .setDestinationId(validDestinationId)
            .setMarketId(validMarketId2)
            .build();
    }

    private static PlanExportStatus makeStatus(final PlanExportState none,
                                        final int progress, final String description) {
        return PlanExportStatus.newBuilder()
            .setState(none)
            .setProgress(progress)
            .setDescription(description)
            .build();
    }

    @NotNull
    private PlanDestination makeDestination(final long destinationId,
                                            final long marketId,
                                            final PlanExportState state,
                                            final int progress,
                                            final String description) {
        return PlanDestination.newBuilder()
            .setOid(destinationId)
            .setMarketId(marketId)
            .setStatus(makeStatus(state, progress, description)
            ).build();
    }

    /**
     * Simulates TP's PlanExportToTargetService.
     */
    private static class TestPlanExportToTargetService extends PlanExportToTargetServiceMole {
        /**
         * Create a mocked TP PlanExportToTargetService instance.
         */
        TestPlanExportToTargetService() {
        }

        @Override
        public PlanExportToTargetResponse exportPlan(final PlanExportToTargetRequest input) {
            return PlanExportToTargetResponse.newBuilder()
                .setStatus(makeStatus(PlanExportState.IN_PROGRESS, 0, "starting"))
                .build();
        }

        @Override
        public Optional<Throwable> exportPlanError(PlanExportToTargetRequest input) {
            if (input.getDestination().getOid() == EXCEPTION_DESTINATION_ID) {
                return Optional.of(new RuntimeException("Simultaed GRPC Failure"));
            } else {
                return Optional.empty();
            }
        }
    }
}
