package com.vmturbo.topology.processor.rpc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.annotation.Nonnull;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.TargetOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsRequest;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsRequest.GroupBy;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsResponse;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsResponse.TargetsGroupStat;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetsStatsResponse.TargetsGroupStat.StatGroup;
import com.vmturbo.common.protobuf.target.TargetDTO.SearchTargetsRequest;
import com.vmturbo.common.protobuf.target.TargetDTO.SearchTargetsResponse;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc.TargetsServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec.Builder;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;

/**
 * Test class for {@link TargetsRpcService} class.
 */
public class TargetsRpcServiceTest {

    private static AtomicLong counter = new AtomicLong(100);
    private static final String ID = "target-address";
    private static final String CLOUD_NATIVE_PROBE_TYPE = "Kubernetes-kubernetes-dc11";

    private TargetStore targetStore = mock(TargetStore.class);

    private ProbeStore probeStore = mock(ProbeStore.class);

    private TargetStatusTracker targetStatusTracker = mock(TargetStatusTracker.class);

    private IOperationManager operationManager = mock(IOperationManager.class);

    private TargetHealthRetriever targetHealthRetriever = mock(TargetHealthRetriever.class);

    private TargetsRpcService service = new TargetsRpcService(targetStore, probeStore, operationManager,
            targetStatusTracker, targetHealthRetriever);

    /**
     * gRPC server for testing. It's good to use a test server instead of making calls to
     * the service directly, because that makes sure the correct methods are overriden.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(service);

    private TargetsServiceBlockingStub clientStub;

    private Map<Long, ProbeInfo> probes;
    private Map<Long, Target> targets;

    /**
     * Initialize the test environment.
     */
    @Before
    public void init() {
        clientStub = TargetsServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        probes = new HashMap<>();
        targets = new HashMap<>();
        when(targetStore.getTarget(Mockito.anyLong())).thenAnswer(answerById(targets));
        when(targetStore.getAll())
                .thenAnswer(invocation -> new ArrayList<>(targets.values()));
        when(probeStore.getProbe(Mockito.anyLong())).thenAnswer(answerById(probes));
        when(probeStore.getProbes()).thenReturn(probes);
        when(operationManager.getLastDiscoveryForTarget(Mockito.anyLong(), Mockito.any()))
                .thenReturn(Optional.empty());
        when(operationManager.getLastValidationForTarget(Mockito.anyLong()))
                .thenReturn(Optional.empty());
    }

    @Nonnull
    private <T> Answer<Optional<T>> answerById(@Nonnull final Map<Long, T> map) {
        return invocation -> {
            final long id = (long)invocation.getArguments()[0];
            return Optional.ofNullable(map.get(id));
        };
    }

    /**
     * Tests getting targets by displayName property.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByName() throws Exception {
        final long probeId = createProbe("probe");
        final long target1 = createTargetSpy(probeId).getId();
        createTargetSpy(probeId);
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.DISPLAY_NAME)
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("tgt-" + target1)
                                .setPositiveMatch(true)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target1));
    }

    /**
     * Test that the service correctly sorts targets by display name.
     *
     * @throws Exception If any.
     */
    @Test
    public void testPaginationSortByDisplayName() throws Exception {
        final long probeId = createProbe("probe");
        final Target t1 = createTargetSpy(probeId);
        when(t1.getDisplayName()).thenReturn("1");
        final Target t2 = createTargetSpy(probeId);
        when(t2.getDisplayName()).thenReturn("2");
        final Target t3 = createTargetSpy(probeId);
        when(t3.getDisplayName()).thenReturn("3");

        assertThat(paginateThroughTargets(TargetOrderBy.TARGET_DISPLAY_NAME, 1, true), contains(t1.getId(), t2.getId(), t3.getId()));
        assertThat(paginateThroughTargets(TargetOrderBy.TARGET_DISPLAY_NAME, 1, false), contains(t3.getId(), t2.getId(), t1.getId()));
    }

    /**
     * Test that the server returns the correct total record count.
     *
     * @throws Exception If any.
     */
    @Test
    public void testPaginationTotalCount() throws Exception {
        final long probeId = createProbe("probe");
        final Target t1 = createTargetSpy(probeId);
        when(t1.getDisplayName()).thenReturn("1");
        final Target t2 = createTargetSpy(probeId);
        when(t2.getDisplayName()).thenReturn("2");
        final Target t3 = createTargetSpy(probeId);
        when(t3.getDisplayName()).thenReturn("3");

        assertThat(clientStub.searchTargets(SearchTargetsRequest.newBuilder()
                .setPaginationParams(PaginationParameters.newBuilder()
                        .setLimit(1000))
                .build()).getPaginationResponse().getTotalRecordCount(), is(3));
    }

    /**
     * Test that the service correctly sorts targets by validation status.
     *
     * @throws Exception If any.
     */
    @Test
    public void testPaginationSortByValidationStatus() throws Exception {
        final long probeId = createProbe("probe");
        final Target t1 = createTargetSpy(probeId);
        final Validation v1 = mockValidation(Status.SUCCESS, 1);
        when(operationManager.getLastValidationForTarget(t1.getId())).thenReturn(Optional.of(v1));
        when(operationManager.getLastDiscoveryForTarget(eq(t1.getId()), any())).thenReturn(Optional.empty());

        final Target t2 = createTargetSpy(probeId);

        // Target 2 finished validation, but failed full discovery.
        final Validation v2 = mockValidation(Status.SUCCESS, 1);
        final Discovery d2 = mockDiscovery(Status.FAILED, 1_000_000);
        when(operationManager.getLastValidationForTarget(t2.getId())).thenReturn(Optional.of(v2));
        when(operationManager.getLastDiscoveryForTarget(eq(t2.getId()), any())).thenReturn(Optional.empty());
        when(operationManager.getLastDiscoveryForTarget(t2.getId(), DiscoveryType.FULL)).thenReturn(Optional.of(d2));

        assertThat(paginateThroughTargets(TargetOrderBy.TARGET_VALIDATION_STATUS, 1, true), contains(t2.getId(), t1.getId()));
        assertThat(paginateThroughTargets(TargetOrderBy.TARGET_VALIDATION_STATUS, 1, false), contains(t1.getId(), t2.getId()));
    }

    /**
     * Test that the service returns all results and
     * no pagination response when there are no pagination parameters.
     *
     * @throws Exception If any.
     */
    @Test
    public void testPaginationNoPaginationParameters() throws Exception {
        final long probeId = createProbe("probe");
        final Target t1 = createTargetSpy(probeId);
        final Target t2 = createTargetSpy(probeId);
        final Target t3 = createTargetSpy(probeId);

        SearchTargetsResponse resp = clientStub.searchTargets(SearchTargetsRequest.newBuilder()
                .build());
        assertFalse(resp.hasPaginationResponse());
        assertThat(resp.getTargetsList(), containsInAnyOrder(t1.getId(), t2.getId(), t3.getId()));
    }

    /**
     * Test that the service returns an error if an invalid cursor is given for pagination.
     *
     * @throws Exception If any.
     */
    @Test
    public void testPaginationInvalidCursor() throws Exception {
        expectInvalidArgument(SearchTargetsRequest.newBuilder()
                .setPaginationParams(PaginationParameters.newBuilder()
                        .setCursor("oriestn"))
                .build());
    }

    /**
     * Test that the service returns no results if cursor is higher than number of results.
     *
     * @throws Exception If any.
     */
    @Test
    public void testPaginationCursorTooHigh() throws Exception {
        final long probeId = createProbe("probe");
        createTargetSpy(probeId);
        createTargetSpy(probeId);

        assertThat(clientStub.searchTargets(SearchTargetsRequest.newBuilder()
                .setPaginationParams(PaginationParameters.newBuilder()
                        .setCursor("100000"))
                .build()).getTargetsList(), is(Collections.emptyList()));
    }

    /**
     * Test that the service returns an error if cursor is below 0.
     *
     * @throws Exception If any.
     */
    @Test
    public void testPaginationCursorTooLow() throws Exception {
        expectInvalidArgument(SearchTargetsRequest.newBuilder()
                .setPaginationParams(PaginationParameters.newBuilder()
                        .setCursor("-1"))
                .build());
    }

    /**
     * Test filtering targets by probe type.
     *
     * @throws Exception If any.
     */
    @Test
    public void testFilterTargetsByType() throws Exception {
        final long probeId1 = createProbe("probe1");
        final long probeId2 = createProbe("probe2");
        final Target t1 = createTargetSpy(probeId1);
        final Target t2 = createTargetSpy(probeId1);
        createTargetSpy(probeId2);

        assertThat(clientStub.searchTargets(SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.PROBE_TYPE)
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions("probe1")))
                .build()).getTargetsList(), containsInAnyOrder(t1.getId(), t2.getId()));

        assertThat(clientStub.searchTargets(SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.PROBE_TYPE)
                        .setStringFilter(StringFilter.newBuilder()
                                .setPositiveMatch(false)
                                .addOptions("probe2")))
                .build()).getTargetsList(), containsInAnyOrder(t1.getId(), t2.getId()));
    }

    /**
     * Test filtering targets by hidden status.
     *
     * @throws Exception If any.
     */
    @Test
    public void testFilterTargetsByHidden() throws Exception {
        final long probeId = createProbe("probe");
        final Target t1 = createCustomizedTargetSpy(probeId, spec -> spec.setIsHidden(true));
        final Target t2 = createCustomizedTargetSpy(probeId, spec -> spec.setIsHidden(true));
        createCustomizedTargetSpy(probeId, spec -> spec.setIsHidden(false));

        assertThat(clientStub.searchTargets(SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.IS_TARGET_HIDDEN)
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions("true")))
                .build()).getTargetsList(), containsInAnyOrder(t1.getId(), t2.getId()));

        assertThat(clientStub.searchTargets(SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.IS_TARGET_HIDDEN)
                        .setStringFilter(StringFilter.newBuilder()
                                .setPositiveMatch(false)
                                .addOptions("false")))
                .build()).getTargetsList(), containsInAnyOrder(t1.getId(), t2.getId()));
    }

    /**
     * Tests getting targets by displayName property using case-insensitive search.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByNameCaseInsensitive() throws Exception {
        final long probeId = createProbe("probe");
        final long target1 = createTargetSpy(probeId).getId();
        createTargetSpy(probeId);
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.DISPLAY_NAME)
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("TgT-" + target1)
                                .setCaseSensitive(false)
                                .setPositiveMatch(true)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target1));
    }

    /**
     * Tests getting targets with display name not matching the requested regexp.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByNameNegativeMatch() throws Exception {
        final long probeId = createProbe("probe");
        final long target1 = createTargetSpy(probeId).getId();
        final long target2 = createTargetSpy(probeId).getId();
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.DISPLAY_NAME)
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex("tgt-" + target1)
                                .setPositiveMatch(false)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target2));
    }

    /**
     * Tests malformed string filter for displayName search.
     */
    @Test
    public void testGetTargetByNameIncorrectRequest() {
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.DISPLAY_NAME)
                        .setStringFilter(StringFilter.newBuilder().addOptions("value")))
                .build();

        expectInvalidArgument(req);
    }

    private void expectInvalidArgument(SearchTargetsRequest req) {
        try {
            clientStub.searchTargets(req);
            fail("Expected exception.");
        } catch (StatusRuntimeException e) {
            assertThat(e, GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT).anyDescription());
        }
    }

    /**
     * Tests retrieving targets by status. Status is populated using both discoveries and
     * validations.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByStatus() throws Exception {
        final long probeId = createProbe("probe");
        final long target1 = createTargetSpy(probeId).getId();
        final long target2 = createTargetSpy(probeId).getId();
        final long target3 = createTargetSpy(probeId).getId();
        setTargetValidationStatus(target1, Status.SUCCESS);
        setTargetValidationStatus(target2, Status.FAILED);
        setTargetValidationStatus(target3, Status.IN_PROGRESS);
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.TARGET_VALIDATION_STATUS)
                        .setStringFilter(
                                StringFilter.newBuilder().addOptions("SUCCESS").setPositiveMatch(true)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target1));
    }

    /**
     * Tests retrieving targets by status with negative match.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByStatusNegativeMatch() throws Exception {
        final long probeId = createProbe("probe");
        final long target1 = createTargetSpy(probeId).getId();
        final long target2 = createTargetSpy(probeId).getId();
        final long target3 = createTargetSpy(probeId).getId();
        setTargetValidationStatus(target1, Status.SUCCESS);
        setTargetValidationStatus(target2, Status.FAILED);
        setTargetValidationStatus(target3, Status.IN_PROGRESS);
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.TARGET_VALIDATION_STATUS)
                        .setStringFilter(
                                StringFilter.newBuilder().addOptions("SUCCESS").setPositiveMatch(false)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target2, target3));
    }

    /**
     * Tests retrieving targets by status. Test ensures that discovery operations are took into
     * account if they are present.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByStatusValidationAndDiscovery() throws Exception {
        final long probeId = createProbe("probe");
        final long target1 = createTargetSpy(probeId).getId();
        final long target2 = createTargetSpy(probeId).getId();
        final long target3 = createTargetSpy(probeId).getId();
        setTargetValidationStatus(target1, Status.SUCCESS);
        setTargetDiscoveryStatus(target2, DiscoveryType.FULL, Status.SUCCESS);
        setTargetDiscoveryStatus(target3, DiscoveryType.FULL, Status.IN_PROGRESS);
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.TARGET_VALIDATION_STATUS)
                        .setStringFilter(
                                StringFilter.newBuilder().addOptions("SUCCESS").setPositiveMatch(true)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target1, target2));
    }

    /**
     * Tests retrieving targets by status. Status selected from multiple operations finished.
     * Test ensures that the latest operation is used to detect target's status
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByStatusSeveralOperations() throws Exception {
        final long probeId = createProbe("probe");
        final long targetId = createTargetSpy(probeId).getId();

        final Discovery fullDiscovery = mock(Discovery.class);
        when(fullDiscovery.getStatus()).thenReturn(Status.FAILED);
        when(fullDiscovery.getCompletionTime())
                .thenReturn(LocalDateTime.of(2020, 1, 1, 10, 30));

        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(fullDiscovery));
        final Discovery performanceDiscovery = mock(Discovery.class);
        when(performanceDiscovery.getStatus()).thenReturn(Status.FAILED);
        when(performanceDiscovery.getCompletionTime())
                .thenReturn(LocalDateTime.of(2020, 1, 1, 10, 35));
        when(
                operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.PERFORMANCE))
                .thenReturn(Optional.of(performanceDiscovery));

        final Validation validation = mock(Validation.class);
        when(validation.getCompletionTime())
                .thenReturn(LocalDateTime.of(2020, 1, 1, 10, 40));
        when(validation.getStatus()).thenReturn(Status.SUCCESS);
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));

        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.TARGET_VALIDATION_STATUS)
                        .setStringFilter(
                                StringFilter.newBuilder().addOptions("SUCCESS").setPositiveMatch(true)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(targetId));
    }

    /**
     * Tests malformed filter used for getting targets by validation status.
     */
    @Test
    public void testGetTargetByStatusInvalidArgument() {
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.TARGET_VALIDATION_STATUS)
                        .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("SUCCESS")))
                .build();
        expectInvalidArgument(req);
    }

    /**
     * Tests retrieving targets by cloud provider type.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByCloudProvider() throws Exception {
        final long probe1 = createProbe(SDKProbeType.AZURE.getProbeType());
        final long probe2 = createProbe(SDKProbeType.AWS.getProbeType());
        final long probe3 = createProbe(SDKProbeType.VCENTER.getProbeType());
        final long target1 = createTargetSpy(probe1).getId();
        createTargetSpy(probe2);
        createTargetSpy(probe3);
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.CLOUD_PROVIDER)
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions(CloudType.AZURE.name())
                                .setPositiveMatch(true)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target1));
    }

    /**
     * Tests retrieving targets by cloud provider type with negative match.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByCloudProviderNegative() throws Exception {
        final long probe1 = createProbe(SDKProbeType.AZURE.getProbeType());
        final long probe2 = createProbe(SDKProbeType.AWS.getProbeType());
        final long probe3 = createProbe(SDKProbeType.VCENTER.getProbeType());
        final long target1 = createTargetSpy(probe1).getId();
        createTargetSpy(probe2);
        final long target3 = createTargetSpy(probe3).getId();
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.CLOUD_PROVIDER)
                        .setStringFilter(StringFilter.newBuilder()
                                .addOptions(CloudType.AWS.name())
                                .setPositiveMatch(false)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target1, target3));
    }

    /**
     * Tests malformed filter passed to cloud provider request.
     */
    @Test
    public void testGetTargetyCloudProviderInvalidAgument() {
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.CLOUD_PROVIDER)
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex(CloudType.AWS.name())
                                .setPositiveMatch(false)))
                .build();
        expectInvalidArgument(req);
    }

    /**
     * Tests no string filter passed to search targets request.
     */
    @Test
    public void testNotStringFilter() {
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.CLOUD_PROVIDER)
                        .setNumericFilter(NumericFilter.newBuilder().setValue(1L)))
                .build();
        expectInvalidArgument(req);
    }

    /**
     * Tests fetching targets by unknown property name.
     */
    @Test
    public void testUnknownPropertyName() {
        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName("some explicitly unknown property name")
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex(CloudType.AWS.name())
                                .setPositiveMatch(false)))
                .build();
        expectInvalidArgument(req);
    }

    /**
     * Tests fetching cloud native targets by K8s cluster name.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testGetCloudNativeTargetsByK8sCluster() throws Exception {
        final long probe1 = createProbe(CLOUD_NATIVE_PROBE_TYPE + "-a");
        final long probe2 = createProbe(CLOUD_NATIVE_PROBE_TYPE + "-b");
        final long target1 = createTargetSpy(probe1).getId();
        final long target2 = createTargetSpy(probe2).getId();

        when(targetStore.getProbeCategoryForTarget(target1)).thenReturn(Optional.of(ProbeCategory.CLOUD_NATIVE));
        when(targetStore.getProbeCategoryForTarget(target2)).thenReturn(Optional.of(ProbeCategory.CLOUD_MANAGEMENT));

        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.K8S_CLUSTER)
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex(CLOUD_NATIVE_PROBE_TYPE + "-a")
                                .setPositiveMatch(true)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target1));
    }

    /**
     * Tests fetching cloud native targets by K8s cluster name using case-insensitive search.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testGetCloudNativeTargetsByK8sClusterCaseInsensitive() throws Exception {
        final long probe1 = createProbe(CLOUD_NATIVE_PROBE_TYPE + "-a");
        final long probe2 = createProbe(CLOUD_NATIVE_PROBE_TYPE + "-b");
        final long target1 = createTargetSpy(probe1).getId();
        final long target2 = createTargetSpy(probe2).getId();

        when(targetStore.getProbeCategoryForTarget(target1)).thenReturn(Optional.of(ProbeCategory.CLOUD_NATIVE));
        when(targetStore.getProbeCategoryForTarget(target2)).thenReturn(Optional.of(ProbeCategory.CLOUD_MANAGEMENT));

        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.K8S_CLUSTER)
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex(CLOUD_NATIVE_PROBE_TYPE + "-A")
                                .setPositiveMatch(true)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target1));
    }

    /**
     * Tests fetching cloud native targets by K8s cluster name not matching the requested regex.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testGetCloudNativeTargetsByK8sClusterNegativeMatch() throws Exception {
        final long probe1 = createProbe(CLOUD_NATIVE_PROBE_TYPE + "-a");
        final long probe2 = createProbe(CLOUD_NATIVE_PROBE_TYPE + "-b");
        final long target1 = createTargetSpy(probe1).getId();
        final long target2 = createTargetSpy(probe2).getId();

        when(targetStore.getProbeCategoryForTarget(target1)).thenReturn(Optional.of(ProbeCategory.CLOUD_NATIVE));
        when(targetStore.getProbeCategoryForTarget(target2)).thenReturn(Optional.of(ProbeCategory.CLOUD_NATIVE));

        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.K8S_CLUSTER)
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex(CLOUD_NATIVE_PROBE_TYPE + "-a")
                                .setPositiveMatch(false)))
                .build();
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder(target2));
    }

    /**
     * Tests fetching cloud native targets by K8s cluster name not matching the requested regex with empty results.
     *
     * @throws Exception on exceptions occurred.
     */
    @Test
    public void testGetCloudNativeTargetsByK8sClusterNegativeMatchEmptyResults() throws Exception {
        final long probe1 = createProbe(CLOUD_NATIVE_PROBE_TYPE);
        final long probe2 = createProbe(SDKProbeType.AZURE.getProbeType());
        final long target1 = createTargetSpy(probe1).getId();
        final long target2 = createTargetSpy(probe2).getId();

        when(targetStore.getProbeCategoryForTarget(target1)).thenReturn(Optional.of(ProbeCategory.CLOUD_NATIVE));
        when(targetStore.getProbeCategoryForTarget(target2)).thenReturn(Optional.of(ProbeCategory.CLOUD_MANAGEMENT));

        final SearchTargetsRequest req = SearchTargetsRequest.newBuilder()
                .addPropertyFilter(PropertyFilter.newBuilder()
                        .setPropertyName(SearchableProperties.K8S_CLUSTER)
                        .setStringFilter(StringFilter.newBuilder()
                                .setStringPropertyRegex(CLOUD_NATIVE_PROBE_TYPE)
                                .setPositiveMatch(false)))
                .build();
        // Filter Cloud Native targets not matching the given regex and result returns empty.
        assertThat(clientStub.searchTargets(req).getTargetsList(), containsInAnyOrder());
    }

    /**
     * Tests targets stats group by target category.
     *
     * @throws InvalidTargetException shouldn't happen
     */
    @Test
    public void testTargetsStatsGroupedByTargetCategory() throws InvalidTargetException {
        // ARRANGE
        final long vcenterProbeId = createProbe(SDKProbeType.VCENTER.getProbeType(),
                ProbeCategory.HYPERVISOR.getCategory());
        final long serviceNowProbeId = createProbe(SDKProbeType.SERVICENOW.getProbeType(),
                ProbeCategory.ORCHESTRATOR.getCategory());
        // created 2 hypervisor and 1 orchestrator targets
        createTargetSpy(vcenterProbeId);
        createTargetSpy(vcenterProbeId);
        createTargetSpy(serviceNowProbeId);

        // ACT
        final GetTargetsStatsResponse targetsStatsResponse = clientStub.getTargetsStats(
                GetTargetsStatsRequest.newBuilder().addGroupBy(GroupBy.TARGET_CATEGORY).build());

        // ASSERT
        final List<TargetsGroupStat> targetGroupStatList =
                targetsStatsResponse.getTargetsGroupStatList();
        final TargetsGroupStat hypervisorExpectedStat = TargetsGroupStat.newBuilder()
                .setTargetsCount(2)
                .setStatGroup(StatGroup.newBuilder()
                        .setTargetCategory(ProbeCategory.HYPERVISOR.getCategory())
                        .build())
                .build();
        final TargetsGroupStat orchestratorExpectedStat = TargetsGroupStat.newBuilder()
                .setTargetsCount(1)
                .setStatGroup(StatGroup.newBuilder()
                        .setTargetCategory(ProbeCategory.ORCHESTRATOR.getCategory())
                        .build())
                .build();
        Assert.assertEquals(2, targetGroupStatList.size());
        Assert.assertTrue(targetGroupStatList.containsAll(Arrays.asList(hypervisorExpectedStat,
                orchestratorExpectedStat)));
    }

    /**
     * Tests targets stats group by target category when some of the targets are hidden and should
     * not be returned in the results.
     *
     * @throws InvalidTargetException shouldn't happen
     */
    @Test
    public void testTargetsStatsGroupedByTargetCategoryWithHiddenTargets() throws InvalidTargetException {
        // ARRANGE
        final long vcenterProbeId = createProbe(SDKProbeType.VCENTER.getProbeType(),
            ProbeCategory.HYPERVISOR.getCategory());
        final long serviceNowProbeId = createProbe(SDKProbeType.SERVICENOW.getProbeType(),
            ProbeCategory.ORCHESTRATOR.getCategory());
        // created 2 hypervisor and 1 orchestrator targets
        createTargetSpy(vcenterProbeId);
        createTargetSpy(vcenterProbeId);
        createCustomizedTargetSpy(serviceNowProbeId, spec -> spec.setIsHidden(true));

        // ACT
        final GetTargetsStatsResponse targetsStatsResponse = clientStub.getTargetsStats(
            GetTargetsStatsRequest.newBuilder().addGroupBy(GroupBy.TARGET_CATEGORY).build());

        // ASSERT
        final List<TargetsGroupStat> targetGroupStatList =
            targetsStatsResponse.getTargetsGroupStatList();
        final TargetsGroupStat hypervisorExpectedStat = TargetsGroupStat.newBuilder()
            .setTargetsCount(2)
            .setStatGroup(StatGroup.newBuilder()
                .setTargetCategory(ProbeCategory.HYPERVISOR.getCategory())
                .build())
            .build();
        Assert.assertEquals(1, targetGroupStatList.size());
        Assert.assertTrue(targetGroupStatList.containsAll(Arrays.asList(hypervisorExpectedStat)));
    }

    /**
     * Tests getting targets stat without certain group by properties.
     * In this case will be returned stat for all targets populated in one stat group without
     * grouping criteria.
     *
     * @throws InvalidTargetException shouldn't happen
     */
    @Test
    public void testTargetsStatsWithoutGroupByProperties() throws InvalidTargetException {
        // ARRANGE
        final long vcenterProbeId = createProbe(SDKProbeType.VCENTER.getProbeType(),
                ProbeCategory.HYPERVISOR.getCategory());
        final long serviceNowProbeId = createProbe(SDKProbeType.SERVICENOW.getProbeType(),
                ProbeCategory.ORCHESTRATOR.getCategory());
        // created 1 hypervisor and 1 orchestrator targets
        createTargetSpy(vcenterProbeId);
        createTargetSpy(serviceNowProbeId);

        // ACT
        final GetTargetsStatsResponse targetsStatsResponse =
                clientStub.getTargetsStats(GetTargetsStatsRequest.getDefaultInstance());

        // ASSERT
        final List<TargetsGroupStat> targetGroupStatList =
                targetsStatsResponse.getTargetsGroupStatList();
        Assert.assertEquals(1, targetGroupStatList.size());
        Assert.assertFalse(targetGroupStatList.get(0).hasStatGroup());
        Assert.assertEquals(2, targetGroupStatList.get(0).getTargetsCount());
    }

    private void setTargetValidationStatus(long targetId, Status status) {
        final Validation validation = mock(Validation.class);
        when(validation.getStatus()).thenReturn(status);
        when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
    }

    private void setTargetDiscoveryStatus(long targetId, @Nonnull DiscoveryType discoveryType,
            @Nonnull Status status) {
        final Discovery discovery = mock(Discovery.class);
        when(discovery.getStatus()).thenReturn(status);
        when(operationManager.getLastDiscoveryForTarget(targetId, discoveryType))
                .thenReturn(Optional.of(discovery));
    }

    private Target createTargetSpy(long probe) throws InvalidTargetException {
        return createCustomizedTargetSpy(probe, spec -> { });
    }

    private Target createCustomizedTargetSpy(long probe, Consumer<Builder> customizer) throws InvalidTargetException {
        final long targetId = counter.getAndIncrement();
        final TargetSpec.Builder targetSpec = TargetSpec.newBuilder()
                .setProbeId(probe)
                .addAccountValue(
                        AccountValue.newBuilder().setKey(ID).setStringValue("tgt-" + targetId));
        customizer.accept(targetSpec);
        final Target target = spy(
                new Target(targetId, probeStore, targetSpec.build(), false, true));
        targets.put(targetId, target);
        return target;
    }

    private long createProbe(@Nonnull String probeType) {
        return createProbe(probeType, "Hypervisor");
    }

    /**
     * Creates probe.
     *
     * @param probeType the probe type
     * @param probeCategory the probe category
     * @return the probe id
     */
    protected long createProbe(@Nonnull String probeType, @Nonnull String probeCategory) {
        final long probeId = counter.getAndIncrement();
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeType(probeType)
                .setProbeCategory(probeCategory)
                .setUiProbeCategory(probeCategory)
                .addAccountDefinition(AccountDefEntry.newBuilder()
                        .setCustomDefinition(CustomAccountDefEntry.newBuilder()
                                .setName(ID)
                                .setDisplayName("taregt identifier")
                                .setDescription("a comment")
                                .setPrimitiveValue(PrimitiveValue.STRING)))
                .addTargetIdentifierField(ID)
                .build();
        probes.put(probeId, probeInfo);
        return probeId;
    }

    private List<Long> paginateThroughTargets(TargetOrderBy orderBy, int limit, boolean ascending) {
        final PaginationParameters startingParams = PaginationParameters.newBuilder()
                .setAscending(ascending)
                .setLimit(limit)
                .setOrderBy(OrderBy.newBuilder()
                        .setTarget(orderBy))
                .build();
        List<Long> allResults = new ArrayList<>();
        String nextCursor = "";
        do {
            SearchTargetsRequest.Builder reqBldr = SearchTargetsRequest.newBuilder()
                    .setPaginationParams(startingParams);
            if (!nextCursor.isEmpty()) {
                reqBldr.getPaginationParamsBuilder().setCursor(nextCursor);
            }
            SearchTargetsResponse resp = clientStub.searchTargets(reqBldr.build());
            allResults.addAll(resp.getTargetsList());
            nextCursor = resp.getPaginationResponse().getNextCursor();
        } while (!nextCursor.isEmpty());
        return allResults;
    }

    private Validation mockValidation(Status status, long completionTime) {
        Validation validation = mock(Validation.class);
        when(validation.getCompletionTime()).thenReturn(LocalDateTime.ofInstant(
                Instant.ofEpochMilli(completionTime), ZoneId.systemDefault()));
        when(validation.getStatus()).thenReturn(status);
        return validation;
    }

    private Discovery mockDiscovery(Status status, long completionTime) {
        Discovery discovery = mock(Discovery.class);
        when(discovery.getCompletionTime()).thenReturn(LocalDateTime.ofInstant(
                Instant.ofEpochMilli(completionTime), ZoneId.systemDefault()));
        when(discovery.getStatus()).thenReturn(status);
        return discovery;
    }
}
