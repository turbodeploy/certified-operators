package com.vmturbo.topology.processor.stitching;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.PostStitchingOperationLibrary;
import com.vmturbo.stitching.PreStitchingOperationLibrary;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.StitchingOperationLibrary;
import com.vmturbo.stitching.StringsToStringsDataDrivenStitchingOperation;
import com.vmturbo.stitching.StringsToStringsStitchingMatchingMetaData;
import com.vmturbo.stitching.cpucapacity.CpuCapacityStore;
import com.vmturbo.stitching.poststitching.CommodityPostStitchingOperationConfig;
import com.vmturbo.stitching.poststitching.DiskCapacityCalculator;
import com.vmturbo.stitching.poststitching.SetAutoSetCommodityCapacityPostStitchingOperation.MaxCapacityCache;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.probes.StandardProbeOrdering;
import com.vmturbo.topology.processor.targets.DuplicateTargetException;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Basic stitching integration test class that provide some util operations. Please define the
 * specific stitching test class under integration/ package and extend this class as parent class.
 */
public abstract class StitchingIntegrationTest {

    protected StatsHistoryServiceMole statsRpcSpy = spy(new StatsHistoryServiceMole());

    protected final StitchingOperationLibrary stitchingOperationLibrary = new StitchingOperationLibrary();
    protected final StitchingOperationStore stitchingOperationStore =
            new StitchingOperationStore(stitchingOperationLibrary);
    protected final PreStitchingOperationLibrary preStitchingOperationLibrary =
            new PreStitchingOperationLibrary();
    protected PostStitchingOperationLibrary postStitchingOperationLibrary;

    protected IdentityProvider identityProvider = mock(IdentityProvider.class);
    protected final ProbeStore probeStore = mock(ProbeStore.class);
    protected final TargetStore targetStore = mock(TargetStore.class);
    protected CpuCapacityStore cpuCapacityStore = mock(CpuCapacityStore.class);
    private final TopologyProcessorNotificationSender sender = Mockito.mock(TopologyProcessorNotificationSender.class);

    protected EntityStore entityStore = new EntityStore(targetStore, identityProvider, 0.3F, true, Collections.singletonList(sender),
            Clock.systemUTC(), false);
    protected final DiskCapacityCalculator diskCapacityCalculator =
            mock(DiskCapacityCalculator.class);

    protected final Clock clock = mock(Clock.class);

    /**
     * GRPC service rule.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(statsRpcSpy);

    /**
     * Initializes the tests.
     */
    @Before
    public void integrationSetup() {
        final StatsHistoryServiceBlockingStub statsServiceClient =
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        postStitchingOperationLibrary =
                new PostStitchingOperationLibrary(
                        new CommodityPostStitchingOperationConfig(
                                statsServiceClient, 30, 10, true),  //meaningless values
                        diskCapacityCalculator, cpuCapacityStore, clock, 0, mock(MaxCapacityCache.class), true);
        when(probeStore.getProbeIdForType(anyString())).thenReturn(Optional.<Long>empty());
        when(probeStore.getProbeOrdering()).thenReturn(new StandardProbeOrdering(probeStore));
        // the probe type doesn't matter here, just return any non-cloud probe type so it gets
        // treated as normal probe
        when(targetStore.getProbeTypeForTarget(Mockito.anyLong()))
                .thenReturn(Optional.of(SDKProbeType.HYPERV));
        when(probeStore.getProbe(anyLong())).thenReturn(Optional.of(ProbeInfo.newBuilder()
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setUiProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setProbeType(SDKProbeType.VCENTER.getProbeType())
            .build()));
    }

    @Nonnull
    protected static StringsToStringsDataDrivenStitchingOperation createDataDrivenStitchingOperation(
                    MergedEntityMetadata mergedEntityMetadata, EntityType entityType,
                    ProbeCategory probeCategory) {
        return new StringsToStringsDataDrivenStitchingOperation(
                        new StringsToStringsStitchingMatchingMetaData(entityType,
                                        mergedEntityMetadata),
                        Collections.singleton(probeCategory));
    }

    protected void setOperationsForProbe(final long probeId,
                                         @Nonnull final List<StitchingOperation<?, ?>> probeStitchingOperations) {
        stitchingOperationStore.setOperationsForProbe(probeId, probeStitchingOperations);
    }

    protected void addEntities(@Nonnull final Map<Long, EntityDTO> entities, final long targetId)
            throws IdentityServiceException, TargetNotFoundException, DuplicateTargetException {
        final long probeId = 0;
        when(identityProvider.getIdsForEntities(
                Mockito.eq(probeId),
                Mockito.eq(new ArrayList<>(entities.values()))))
                .thenReturn(entities);
        // Pretend that any target exists
        final Target mockTarget = mock(Target.class);
        final ProbeInfo probeInfo = ProbeInfo.newBuilder()
                .setProbeCategory(ProbeCategory.STORAGE.getCategory())
                .setProbeType("Whatever").build();
        when(mockTarget.getProbeInfo()).thenReturn(probeInfo);
        when(targetStore.getTarget(anyLong())).thenReturn(Optional.of(mockTarget));
        entityStore.entitiesDiscovered(probeId, targetId, 0, DiscoveryType.FULL,
            new ArrayList<>(entities.values()));
    }

    protected List<Long> oidsFor(@Nonnull final Stream<String> displayNames,
                                 @Nonnull final Map<Long, EntityDTO> entityMap) {
        return displayNames
                .map(displayName -> entityMap.entrySet().stream()
                        .filter(entityEntry -> entityEntry.getValue().getDisplayName().equals(displayName))
                        .findFirst().get())
                .map(Entry::getKey)
                .collect(Collectors.toList());
    }
}
