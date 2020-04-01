package com.vmturbo.topology.processor.rpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchTargetsResponse;
import com.vmturbo.common.protobuf.search.SearchableProperties;
import com.vmturbo.platform.common.dto.Discovery.AccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry;
import com.vmturbo.platform.common.dto.Discovery.CustomAccountDefEntry.PrimitiveValue;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.AccountValue;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TargetSpec;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.InvalidTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Unit test for {@link TargetSearchRpcService}.
 */
public class TargetSearchRpcServiceTest {

    private static AtomicLong counter = new AtomicLong(100);
    private static final String ID = "target-address";

    private ProbeStore probeStore;
    private IOperationManager operationManager;
    private TargetSearchRpcService service;
    @Mock
    private StreamObserver<SearchTargetsResponse> responseObserver;
    @Captor
    private ArgumentCaptor<SearchTargetsResponse> resultCaptor;
    @Captor
    private ArgumentCaptor<Throwable> errorCaptor;
    private Map<Long, ProbeInfo> probes;
    private Map<Long, Target> targets;

    /**
     * Initialize the test environment.
     */
    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        probes = new HashMap<>();
        targets = new HashMap<>();
        TargetStore targetStore = Mockito.mock(TargetStore.class);
        Mockito.when(targetStore.getTarget(Mockito.anyLong())).thenAnswer(answerById(targets));
        Mockito.when(targetStore.getAll())
                .thenAnswer(invocation -> new ArrayList<>(targets.values()));
        probeStore = Mockito.mock(ProbeStore.class);
        Mockito.when(probeStore.getProbe(Mockito.anyLong())).thenAnswer(answerById(probes));
        Mockito.when(probeStore.getProbes()).thenReturn(probes);
        operationManager = Mockito.mock(IOperationManager.class);
        service = new TargetSearchRpcService(targetStore, probeStore, operationManager);
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
        final long target1 = createTarget(probeId);
        createTarget(probeId);
        final PropertyFilter targetFilter = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("tgt-" + target1)
                        .setPositiveMatch(true))
                .build();
        Assert.assertEquals(Collections.singleton(target1), expectResult(targetFilter));
    }

    /**
     * Tests getting targets by displayName property using case-insensitive search.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByNameCaseInsensitive() throws Exception {
        final long probeId = createProbe("probe");
        final long target1 = createTarget(probeId);
        createTarget(probeId);
        final PropertyFilter targetFilter = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("TgT-" + target1)
                        .setCaseSensitive(false)
                        .setPositiveMatch(true))
                .build();
        Assert.assertEquals(Collections.singleton(target1), expectResult(targetFilter));
    }

    /**
     * Tests getting targets with display name not matching the requested regexp.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByNameNegativeMatch() throws Exception {
        final long probeId = createProbe("probe");
        final long target1 = createTarget(probeId);
        final long target2 = createTarget(probeId);
        final PropertyFilter targetFilter = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex("tgt-" + target1)
                        .setPositiveMatch(false))
                .build();
        Assert.assertEquals(Collections.singleton(target2), expectResult(targetFilter));
    }

    /**
     * Tests malformed string filter for displayName search.
     */
    @Test
    public void testGetTargetByNameIncorrectRequest() {
        final PropertyFilter targetFilter = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.DISPLAY_NAME)
                .setStringFilter(StringFilter.newBuilder().addOptions("value"))
                .build();
        expectInvalidArgument(targetFilter);
    }

    /**
     * Tests retrieving targets by status.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByStatus() throws Exception {
        final long probeId = createProbe("probe");
        final long target1 = createTarget(probeId);
        final long target2 = createTarget(probeId);
        final long target3 = createTarget(probeId);
        setTargetStatus(target1, Status.SUCCESS);
        setTargetStatus(target2, Status.FAILED);
        setTargetStatus(target3, Status.IN_PROGRESS);
        final PropertyFilter targetFilter = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.TARGET_VALIDATION_STATUS)
                .setStringFilter(
                        StringFilter.newBuilder().addOptions("SUCCESS").setPositiveMatch(true))
                .build();
        Assert.assertEquals(Collections.singleton(target1), expectResult(targetFilter));
    }

    /**
     * Tests retrieving targets by status with negative match.
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetTargetByStatusNegativeMatch() throws Exception {
        final long probeId = createProbe("probe");
        final long target1 = createTarget(probeId);
        final long target2 = createTarget(probeId);
        final long target3 = createTarget(probeId);
        setTargetStatus(target1, Status.SUCCESS);
        setTargetStatus(target2, Status.FAILED);
        setTargetStatus(target3, Status.IN_PROGRESS);
        final PropertyFilter targetFilter = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.TARGET_VALIDATION_STATUS)
                .setStringFilter(
                        StringFilter.newBuilder().addOptions("SUCCESS").setPositiveMatch(false))
                .build();
        Assert.assertEquals(Sets.newHashSet(target2, target3), expectResult(targetFilter));
    }

    /**
     * Tests malformed filter used for getting targets by validation status.
     */
    @Test
    public void testGetTargetByStatusInvalidArgument() {
        final PropertyFilter targetFilter = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.TARGET_VALIDATION_STATUS)
                .setStringFilter(StringFilter.newBuilder().setStringPropertyRegex("SUCCESS"))
                .build();
        expectInvalidArgument(targetFilter);
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
        final long target1 = createTarget(probe1);
        createTarget(probe2);
        createTarget(probe3);
        final PropertyFilter filterAzure = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.CLOUD_PROVIDER)
                .setStringFilter(StringFilter.newBuilder()
                        .addOptions(CloudType.AZURE.name())
                        .setPositiveMatch(true))
                .build();
        Assert.assertEquals(Collections.singleton(target1), expectResult(filterAzure));
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
        final long target1 = createTarget(probe1);
        createTarget(probe2);
        final long target3 = createTarget(probe3);
        final PropertyFilter filterNoAws = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.CLOUD_PROVIDER)
                .setStringFilter(StringFilter.newBuilder()
                        .addOptions(CloudType.AWS.name())
                        .setPositiveMatch(false))
                .build();
        Assert.assertEquals(Sets.newHashSet(target1, target3), expectResult(filterNoAws));
    }

    /**
     * Tests malformed filter passed to cloud provider request.
     */
    @Test
    public void testGetTargetyCloudProviderInvalidAgument() {
        final PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.CLOUD_PROVIDER)
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex(CloudType.AWS.name())
                        .setPositiveMatch(false))
                .build();
        expectInvalidArgument(filter);
    }

    /**
     * Tests no string filter passed to search targets request.
     */
    @Test
    public void testNotStringFilter() {
        final PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName(SearchableProperties.CLOUD_PROVIDER)
                .setNumericFilter(NumericFilter.newBuilder().setValue(1L))
                .build();
        expectInvalidArgument(filter);
    }

    /**
     * Tests fetching targets by unknown property name.
     */
    @Test
    public void testUnknownPropertyName() {
        final PropertyFilter filter = PropertyFilter.newBuilder()
                .setPropertyName("some explicitly unknown property name")
                .setStringFilter(StringFilter.newBuilder()
                        .setStringPropertyRegex(CloudType.AWS.name())
                        .setPositiveMatch(false))
                .build();
        expectInvalidArgument(filter);
    }

    private Set<Long> expectResult(@Nonnull PropertyFilter targetFilter) {
        service.searchTargets(targetFilter, responseObserver);
        Mockito.verify(responseObserver, Mockito.never()).onError(Mockito.any());
        Mockito.verify(responseObserver).onCompleted();
        Mockito.verify(responseObserver).onNext(resultCaptor.capture());
        return new HashSet<>(resultCaptor.getValue().getTargetsList());
    }

    private void expectInvalidArgument(@Nonnull PropertyFilter targetFilter) {
        service.searchTargets(targetFilter, responseObserver);
        Mockito.verify(responseObserver, Mockito.never()).onCompleted();
        Mockito.verify(responseObserver).onError(errorCaptor.capture());
        Assert.assertThat(errorCaptor.getValue(), CoreMatchers.instanceOf(StatusException.class));
        final StatusException exception = (StatusException)errorCaptor.getValue();
        Assert.assertEquals(Code.INVALID_ARGUMENT, exception.getStatus().getCode());
    }

    private void setTargetStatus(long targetId, Status status) {
        final Validation validation = Mockito.mock(Validation.class);
        Mockito.when(validation.getStatus()).thenReturn(status);
        Mockito.when(operationManager.getLastValidationForTarget(targetId))
                .thenReturn(Optional.of(validation));
    }

    private long createTarget(long probe) throws InvalidTargetException {
        final long targetId = counter.getAndIncrement();
        final TargetSpec targetSpec = TargetSpec.newBuilder()
                .setProbeId(probe)
                .addAccountValue(
                        AccountValue.newBuilder().setKey(ID).setStringValue("tgt-" + targetId))
                .build();
        final Target target = new Target(targetId, probeStore, targetSpec, false);
        targets.put(targetId, target);
        return targetId;
    }

    private long createProbe(@Nonnull String probeType) {
        final long probeId = counter.getAndIncrement();
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeType(probeType)
                .setProbeCategory("Hypercloud")
                .setUiProbeCategory("Hypercloud")
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
}
