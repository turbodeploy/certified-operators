package com.vmturbo.topology.processor.stitching.integration;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommodityBoughtMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.CommoditySoldMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityField;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.EntityOid;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingMetadata;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.StitchingOperation;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingIntegrationTest;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Attempt to simulate guest OS processes probe stitching operation.
 */
public class GuestLoadStitchingIntegrationTest extends StitchingIntegrationTest {

    private final long vcProbeId = 1111L;
    private final long vcTargetId = 2222L;
    private final long apmProbeId = 2333L;
    private final long apmTargetId = 6666L;

    @Test
    public void testGuestLoadStitchingWithGenericOperations() throws Exception {
        testGuestLoadStitching(getDataDrivenGuestLoadStitchingOperations());
    }

    private static List<StitchingOperation<?, ?>> getDataDrivenGuestLoadStitchingOperations() {
        EntityField idField = EntityField.newBuilder().setFieldName("id").build();
        EntityOid oid = EntityOid.newBuilder().build();
        MatchingData internalMatchingData = MatchingData.newBuilder()
                .setMatchingField(idField).build();
        MatchingData externalMatchingData = MatchingData.newBuilder()
                .setMatchingEntityOid(oid).build();
        MatchingMetadata guestLoadMatchingMetadata = MatchingMetadata.newBuilder()
                .addMatchingData(internalMatchingData)
                .addExternalEntityMatchingProperty(externalMatchingData)
                .build();

        final MergedEntityMetadata guestLoadVMMergeEntityMetadata =
                MergedEntityMetadata.newBuilder().mergeMatchingMetadata(guestLoadMatchingMetadata)
                        .addAllCommoditiesSoldMetadata(soldCommoditiesFromVMToApp)
                        .build();
        final MergedEntityMetadata guestLoadAppMergeEntityMetadata =
                MergedEntityMetadata.newBuilder().mergeMatchingMetadata(guestLoadMatchingMetadata)
                        .addAllCommoditiesSoldMetadata(soldCommoditiesFromApp)
                        .addAllCommoditiesBought(boughtCommoditiesFromAppToVM)
                        .build();

        return ImmutableList.of(createDataDrivenStitchingOperation(guestLoadVMMergeEntityMetadata,
                        EntityType.VIRTUAL_MACHINE, ProbeCategory.HYPERVISOR),
                        createDataDrivenStitchingOperation(guestLoadAppMergeEntityMetadata,
                                        EntityType.APPLICATION_COMPONENT, ProbeCategory.HYPERVISOR));
    }



    private void testGuestLoadStitching(List<StitchingOperation<?, ?>>
                                                guestLoadStitchingOperationsToTest)
            throws Exception {
        final Map<Long, EntityDTO> hypervisorEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/apm_vc_data.json", 1L);
        final Map<Long, EntityDTO> guestLoadEntities =
                sdkDtosFromFile(getClass(), "protobuf/messages/apm_snmp_data.json",
                        hypervisorEntities.size() + 1L);

        addEntities(guestLoadEntities, apmTargetId);
        addEntities(hypervisorEntities, vcTargetId);

        setOperationsForProbe(apmProbeId,
                guestLoadStitchingOperationsToTest);
        setOperationsForProbe(vcProbeId, Collections.emptyList());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
                preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore,
                targetStore, cpuCapacityStore);
        final Target apmTarget = mock(Target.class);
        when(apmTarget.getId()).thenReturn(apmTargetId);
        final Target vcTarget = mock(Target.class);
        when(vcTarget.getId()).thenReturn(vcTargetId);
        when(targetStore.getProbeTargets(apmProbeId))
                .thenReturn(Collections.singletonList(apmTarget));
        when(targetStore.getProbeTargets(vcProbeId))
                .thenReturn(Collections.singletonList(vcTarget));
        when(probeStore.getProbe(apmProbeId)).thenReturn(Optional.empty());
        when(probeStore.getProbeIdsForCategory(ProbeCategory.GUEST_OS_PROCESSES))
                .thenReturn(Collections.singletonList(apmProbeId));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.HYPERVISOR))
                .thenReturn(Collections.singletonList(vcProbeId));

        final StringBuilder journalStringBuilder = new StringBuilder(2048);
        final StitchingContext stitchingContext = entityStore.constructStitchingContext();
        final ConfigurableStitchingJournalFactory journalFactory = StitchingJournalFactory
                .configurableStitchingJournalFactory(Clock.systemUTC())
                .addRecorder(new StringBuilderRecorder(journalStringBuilder));
        journalFactory.setJournalOptions(JournalOptions.newBuilder()
                .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY)
                .build());

        final IStitchingJournal<StitchingEntity> journal = journalFactory.stitchingJournal(stitchingContext);
        stitchingManager.stitch(stitchingContext, journal);
        final Map<Long, TopologyEntityDTO.Builder> topology = stitchingContext.constructTopology();

        // these proxy VMs and Apps from APM should have been removed.
        final List<Long> apmExpectedRemoved = oidsFor(Stream.of(
                "proxy-vm-1",
                "proxy-guestload-app-1"),
                guestLoadEntities);
        // these real App from APM should have been retained.
        final List<Long> apmExpectedRetained = oidsFor(Stream.of(
                "app-2"),
                guestLoadEntities);
        // all entities from VC should be retained.
        final List<Long> vcExpectedRetained = oidsFor(Stream.of(
                "vm-1",
                "guestload-app-1"),
                hypervisorEntities);

        apmExpectedRemoved.forEach(oid -> assertNull(topology.get(oid)));
        apmExpectedRetained.forEach(oid -> assertNotNull(topology.get(oid)));
        vcExpectedRetained.forEach(oid -> assertNotNull(topology.get(oid)));

        final long vm1Oid = vcExpectedRetained.get(0);
        final long app1Oid = vcExpectedRetained.get(1);
        final long app2Oid = apmExpectedRetained.get(0);
        final long proxyVm1Oid = apmExpectedRemoved.get(0);
        final long proxyApp1Oid = apmExpectedRemoved.get(1);
        final double delta = 1e-7;
        // get proxy entity from entity store, and real entity from repository
        final EntityDTO proxyVm1 = entityStore.getEntity(proxyVm1Oid).get()
                .getEntityInfo(apmTargetId).get().getEntityInfo();
        final EntityDTO proxyApp1 = entityStore.getEntity(proxyApp1Oid).get()
                .getEntityInfo(apmTargetId).get().getEntityInfo();
        final TopologyEntityDTO vm1Topo = topology.get(vm1Oid).build();
        final TopologyEntityDTO app1Topo = topology.get(app1Oid).build();
        final TopologyEntityDTO app2Topo = topology.get(app2Oid).build();

        // app-2 should has 1 VC provider
        assertEquals(1, app2Topo.getCommoditiesBoughtFromProvidersCount());
        assertEquals(vm1Topo.getOid(),
                app2Topo.getCommoditiesBoughtFromProviders(0).getProviderId());

        // verify commodities sold in VM-1 has been replaced by proxy in VM-1
        vm1Topo.getCommoditySoldListList().forEach(
            commoditySoldDTO -> {
                final int commTypeVal = commoditySoldDTO.getCommodityType().getType();
                // get commodity sold DTO in proxy VM
                final CommodityDTO proxyComm = proxyVm1.getCommoditiesSoldList().stream()
                        .filter(commSold ->
                                commSold.getCommodityType().getNumber() == commTypeVal)
                        .findFirst().get();
                assertEquals(proxyComm.getUsed(), commoditySoldDTO.getUsed(), delta);
                assertEquals(proxyComm.getPeak(), commoditySoldDTO.getPeak(), delta);
                assertEquals(proxyComm.getCapacity(), commoditySoldDTO.getCapacity(), delta);
            }
        );

        // verify commodities sold in app-1 has been replaced by proxy in app-1
        app1Topo.getCommoditySoldListList().forEach(
            commoditySoldDTO -> {
                final int commTypeVal = commoditySoldDTO.getCommodityType().getType();
                // get commodity sold DTO in proxy app
                final CommodityDTO proxyComm = proxyApp1.getCommoditiesSoldList().stream()
                        .filter(commSold ->
                                commSold.getCommodityType().getNumber() == commTypeVal)
                        .findFirst().get();
                assertEquals(proxyComm.getUsed(), commoditySoldDTO.getUsed(), delta);
                assertEquals(proxyComm.getPeak(), commoditySoldDTO.getPeak(), delta);
                assertEquals(proxyComm.getCapacity(), commoditySoldDTO.getCapacity(), delta);
            }
        );

        // verify commodities bought in app-1 has been replaced by proxy in app-1
        app1Topo.getCommoditiesBoughtFromProvidersList().forEach(
            commoditiesBoughtFromProvider -> {
                // should buy from VM-1
                assertEquals(vm1Oid, commoditiesBoughtFromProvider.getProviderId());
                commoditiesBoughtFromProvider.getCommodityBoughtList().forEach(
                    commodityBoughtDTO -> {
                        final int commTypeVal = commodityBoughtDTO.getCommodityType().getType();
                        // get commodity bought DTO in proxy app
                        final CommodityDTO proxyComm = proxyApp1.getCommoditiesBoughtList().stream()
                                .flatMap(commBoughts -> commBoughts.getBoughtList().stream())
                                .filter(commBought ->
                                        commBought.getCommodityType().getNumber() == commTypeVal)
                                .findFirst().get();
                        assertEquals(proxyComm.getUsed(), commodityBoughtDTO.getUsed(), delta);
                        assertEquals(proxyComm.getPeak(), commodityBoughtDTO.getPeak(), delta);
                    }
                );
            }
        );
    }

    private static Collection<CommoditySoldMetadata> soldCommoditiesFromApp = ImmutableList.of(
        CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.TRANSACTION).build(),
        CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.SLA_COMMODITY).build());

    private static Collection<CommoditySoldMetadata> soldCommoditiesFromVMToApp = ImmutableList.of(
        CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.VMEM).build(),
        CommoditySoldMetadata.newBuilder().setCommodityType(CommodityType.VCPU).build());

    private static Collection<CommodityBoughtMetadata> boughtCommoditiesFromAppToVM =
            ImmutableList.of(CommodityBoughtMetadata.newBuilder()
                    .addAllCommodityMetadata(ImmutableList.of(CommodityType.VMEM,
                            CommodityType.VCPU))
                    .setProviderType(EntityType.VIRTUAL_MACHINE)
                    .build());
 }
