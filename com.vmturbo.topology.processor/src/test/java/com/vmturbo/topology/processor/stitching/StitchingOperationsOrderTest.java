package com.vmturbo.topology.processor.stitching;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.builders.CommodityBuilders;
import com.vmturbo.platform.common.builders.EntityBuilders;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Test that order of stitching operations does not impact the stitching result.
 */
public class StitchingOperationsOrderTest extends StitchingIntegrationTest {

    private StitchingManager stitchingManager;

    private static final long kubernetesTargetId = 1111L;
    private static final long customTargetId = 2222L;
    private static final long kubernetesProbeId = 3333L;
    private static final long customProbeId = 4444L;

    private static final long oid_app = 11L;
    private static final long oid_app_proxy = 111L;
    private static final long oid_vm = 22L;
    private static final long oid_vm_proxy = 222L;

    private static final MergedEntityMetadata appMergedEntityMetadata = new MergedEntityMetadataBuilder()
            .internalMatchingProperty("IP")
            .externalMatchingProperty("IP")
            .mergedBoughtCommodity(EntityType.VIRTUAL_MACHINE, ImmutableList.of(CommodityType.VCPU))
            .build();
    private static final MergedEntityMetadata vmMergedEntityMetadata = new MergedEntityMetadataBuilder()
            .internalMatchingProperty("IP")
            .externalMatchingProperty("IP")
            .mergedSoldCommodity(CommodityType.VCPU)
            .build();

    // kubernetes entities
    private final EntityDTO app = EntityBuilders.applicationComponent("app")
            .buying(CommodityBuilders.application().from("vm").key("420095D1-48B7-73AB-26F0-795BE112B531"))
            .property("IP", "10.2.1.31")
            .build();

    private final EntityDTO vm = EntityBuilders.virtualMachine("vm")
            .selling(CommodityBuilders.application().sold().key("420095D1-48B7-73AB-26F0-795BE112B531"))
            .property("IP", "10.2.1.31")
            .build();

    // Custom entities
    private final EntityDTO app_proxy = EntityBuilders.applicationComponent("app-proxy")
            .buying(CommodityBuilders.vCpuMHz().from("vm-proxy").used(150d))
            .buying(CommodityBuilders.application().from("vm-proxy").key("virtualmachine-10.2.1.31"))
            .property("IP", "10.2.1.31")
            .proxy()
            .build();

    private final EntityDTO vm_proxy = EntityBuilders.virtualMachine("vm-proxy")
            .selling(CommodityBuilders.vCpuMHz().sold().used(150d).capacity(500d))
            .selling(CommodityBuilders.application().sold().key("virtualmachine-10.2.1.31"))
            .property("IP", "10.2.1.31")
            .proxy()
            .build();

    /**
     * Rule to manage enablements via a mutable store.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

    /**
     * Test that stitching both consumer and provider proxy entities with real entities will not be
     * affected by the order of stitching operations.
     * Test stitching proxy application first, and then stitching proxy virtual machine next.
     *
     * @throws Exception exception
     */
    @Test
    public void testStitchingAppFirst() throws Exception {
        init(ImmutableList.of(
                createDataDrivenStitchingOperation(appMergedEntityMetadata,
                        EntityType.APPLICATION_COMPONENT, ProbeCategory.CLOUD_NATIVE,
                        ProbeCategory.APPLICATIONS_AND_DATABASES),
                createDataDrivenStitchingOperation(vmMergedEntityMetadata,
                        EntityType.VIRTUAL_MACHINE, ProbeCategory.CLOUD_NATIVE,
                        ProbeCategory.APPLICATIONS_AND_DATABASES)));
        testStitching();
    }

    /**
     * Test that stitching both consumer and provider proxy entities with real entities will not be
     * affected by the order of stitching operations.
     * Test stitching proxy virtual machine first, and then stitching proxy application next.
     *
     * @throws Exception exception
     */
    @Test
    public void testStitchingVirtualMachineFirst() throws Exception {
        init(ImmutableList.of(
                createDataDrivenStitchingOperation(vmMergedEntityMetadata,
                        EntityType.VIRTUAL_MACHINE, ProbeCategory.CLOUD_NATIVE,
                        ProbeCategory.APPLICATIONS_AND_DATABASES),
                createDataDrivenStitchingOperation(appMergedEntityMetadata,
                        EntityType.APPLICATION_COMPONENT, ProbeCategory.CLOUD_NATIVE,
                        ProbeCategory.APPLICATIONS_AND_DATABASES)));
        testStitching();
    }

    /**
     * Test that stitching both consumer and provider proxy entities with real entities will not be
     * affected by the order of stitching operations.
     * Test stitching proxy application first, and then stitching proxy virtual machine next.
     *
     * @throws Exception exception
     */
    @Test
    public void testStitchingBillingFirst() throws Exception {
        init(ImmutableList.of(
            createDataDrivenStitchingOperation(appMergedEntityMetadata,
                EntityType.APPLICATION_COMPONENT, ProbeCategory.CLOUD_NATIVE,
                    ProbeCategory.APPLICATIONS_AND_DATABASES),
            createDataDrivenStitchingOperation(vmMergedEntityMetadata,
                EntityType.VIRTUAL_MACHINE, ProbeCategory.CUSTOM,
                    ProbeCategory.APPLICATIONS_AND_DATABASES),
            createDataDrivenStitchingOperation(vmMergedEntityMetadata,
                EntityType.VIRTUAL_MACHINE, ProbeCategory.GUEST_OS_PROCESSES,
                    ProbeCategory.APPLICATIONS_AND_DATABASES),
            createDataDrivenStitchingOperation(vmMergedEntityMetadata,
                EntityType.VIRTUAL_MACHINE, ProbeCategory.CLOUD_NATIVE,
                    ProbeCategory.APPLICATIONS_AND_DATABASES)));
        testStitching();
    }

    /**
     * Test the stitching between Custom and Kubernetes targets. Please note that the supply chain
     * defined in this test may not reflect the true entities relationship of kubernetes targets.
     * This is just an example of verifying that stitching both proxy entities with real entities
     * will not be affected by the order of stitching operations.
     *
     * <p>Before stitching:
     *
     * <p>Kubernetes                              Custom
     *
     * <p>app (buys APPLICATION:Key1)         app-proxy (buys APPLICATION:Key2 and VCPU)
     *     |                                       |
     *    vm (sells APPLICATION:key1)          vm-proxy (sells APPLICATION:Key2 and VCPU)
     *
     * <p>MergedEntityMetadata defined in Custom probe:
     *    ApplicationComponent:
     *      matchingMetadata:
     *        internalProperty: IP
     *        externalProperty: IP
     *      commoditiesBought:
     *        providerType: VirtualMachine
     *        commodity:
     *          - VCPU
     *    VirtualMachine:
     *      matchingMetadata:
     *        internalProperty: IP
     *        externalProperty: IP
     *      commoditiesSold:
     *        - VCPU
     *
     * <p>After stitching:
     *
     * <p>Only VCPU commodities are merged from proxy entities (bought by app and sold by vm)
     * The original APPLICATION commodity between the real app and vm entities are NOT touched.
     *
     * <p>Kubernetes
     *
     * <p>app (buys APPLICATION:Key1 and VCPU)
     *          |
     *    vm (sells APPLICATION:Key1 and VCPU)
     *
     * @throws Exception exception
     */
    private void testStitching() throws Exception {
        final Map<Long, EntityDTO> kubernetesEntities = ImmutableMap.of(
                oid_app, app,
                oid_vm, vm
        );
        final Map<Long, EntityDTO> customEntities = ImmutableMap.of(
                oid_app_proxy, app_proxy,
                oid_vm_proxy, vm_proxy
        );
        addEntities(kubernetesEntities, kubernetesTargetId);
        addEntities(customEntities, customTargetId);
        final StitchingContext stitchingContext = entityStore.constructStitchingContext();
        // verify that there are 4 entities before stitching
        assertEquals(4, stitchingContext.getStitchingGraph().entityCount());

        // stitch
        Map<Long, TopologyEntityDTO.Builder> topology = stitch(stitchingContext);

        // verify that only entities from Kubernetes are left after stitching
        assertEquals(2, topology.size());

        final TopologyEntityDTO.Builder app = topology.get(oid_app);
        final TopologyEntityDTO.Builder vm = topology.get(oid_vm);
        CommoditiesBoughtFromProvider cb = verifyAndGetCommoditiesBought(app, vm);
        assertEquals(2, cb.getCommodityBoughtCount());
        verifyBuyingCommodity(cb, CommodityType.VCPU_VALUE, "", 150d);
        verifyBuyingCommodity(cb, CommodityType.APPLICATION_VALUE,
                "420095D1-48B7-73AB-26F0-795BE112B531", 0);
    }

    private void init(List<StitchingOperation<?, ?>> dataDrivenStitchingOperations) {
        setOperationsForProbe(customProbeId, dataDrivenStitchingOperations);
        setOperationsForProbe(kubernetesProbeId, Collections.emptyList());
        stitchingManager = new StitchingManager(stitchingOperationStore,
                preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore,
                targetStore, cpuCapacityStore);
        final Target kubernetesTarget = mock(Target.class);
        when(kubernetesTarget.getId()).thenReturn(kubernetesTargetId);
        final Target customTarget = mock(Target.class);
        when(customTarget.getId()).thenReturn(customTargetId);
        when(targetStore.getProbeTargets(kubernetesProbeId))
                .thenReturn(Collections.singletonList(kubernetesTarget));
        when(targetStore.getProbeTargets(customProbeId))
                .thenReturn(Collections.singletonList(customTarget));
        when(probeStore.getProbe(customProbeId)).thenReturn(Optional.empty());
        when(probeStore.getProbeIdsForCategory(ProbeCategory.CUSTOM))
                .thenReturn(Collections.singletonList(customProbeId));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.CLOUD_NATIVE))
                .thenReturn(Collections.singletonList(kubernetesProbeId));
        when(probeStore.getProbe(customProbeId)).thenReturn(Optional.of(ProbeInfo.newBuilder()
                .setProbeCategory(ProbeCategory.CUSTOM.getCategory())
                .setUiProbeCategory(ProbeCategory.CUSTOM.getCategory())
                .setProbeType(SDKProbeType.UDT.getProbeType())
                .build()));
        when(probeStore.getProbe(kubernetesProbeId)).thenReturn(Optional.of(ProbeInfo.newBuilder()
                .setProbeCategory(ProbeCategory.CLOUD_NATIVE.getCategory())
                .setUiProbeCategory(ProbeCategory.CLOUD_NATIVE.getCategory())
                .setProbeType(SDKProbeType.KUBERNETES.getProbeType())
                .build()));
    }

    private Map<Long, TopologyEntityDTO.Builder> stitch(StitchingContext stitchingContext) {
        final ConfigurableStitchingJournalFactory journalFactory = StitchingJournalFactory
                .configurableStitchingJournalFactory(Clock.systemUTC())
                .addRecorder(new StringBuilderRecorder(new StringBuilder(2048)));
        journalFactory.setJournalOptions(JournalOptions.newBuilder()
                .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY)
                .build());
        final IStitchingJournal<StitchingEntity> journal = journalFactory.stitchingJournal(stitchingContext);
        stitchingManager.stitch(stitchingContext, journal);
        return stitchingContext.constructTopology();
    }

    private CommoditiesBoughtFromProvider verifyAndGetCommoditiesBought(
            TopologyEntityDTO.Builder consumer, TopologyEntityDTO.Builder provider) {
        List<CommoditiesBoughtFromProvider> bought = consumer.getCommoditiesBoughtFromProvidersList()
                .stream()
                .filter(cb -> cb.getProviderId() == provider.getOid())
                .collect(Collectors.toList());
        assertEquals(1, bought.size());
        return bought.get(0);
    }

    private void verifyBuyingCommodity(CommoditiesBoughtFromProvider cb, int commodityType,
                                       String key, double used) {
        assertEquals(1, cb.getCommodityBoughtList().stream()
                .filter(comm -> comm.getCommodityType().getType() == commodityType)
                .filter(comm -> comm.getCommodityType().getKey().equals(key))
                .filter(comm -> comm.getUsed() == used)
                .count());
    }
}
