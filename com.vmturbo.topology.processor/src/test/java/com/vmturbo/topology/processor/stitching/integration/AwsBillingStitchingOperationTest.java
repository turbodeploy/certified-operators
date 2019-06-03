package com.vmturbo.topology.processor.stitching.integration;

import static com.vmturbo.topology.processor.stitching.StitchingTestUtils.sdkDtosFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.Stitching.JournalOptions;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.billing.AwsBillingStitchingOperation;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalRecorder.StringBuilderRecorder;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.StitchingIntegrationTest;
import com.vmturbo.topology.processor.stitching.StitchingManager;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory.ConfigurableStitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;

/**
 * Integration tests for {@link AwsBillingStitchingOperation}
 */
public class AwsBillingStitchingOperationTest extends StitchingIntegrationTest {

    private final long awsProbeId = 1111L;
    private final long awsTargetId = 2222L;
    private final long awsBillingProbeId = 1234L;
    private final long awsBillingTargetId = 1235L;

    @Test
    public void testStitching() throws Exception {
        final StringBuilder journalStringBuilder = new StringBuilder(2048);
        final ConfigurableStitchingJournalFactory journalFactory = StitchingJournalFactory
            .configurableStitchingJournalFactory(Clock.systemUTC())
            .addRecorder(new StringBuilderRecorder(journalStringBuilder));
        journalFactory.setJournalOptions(JournalOptions.newBuilder()
            .setVerbosity(Verbosity.LOCAL_CONTEXT_VERBOSITY)
            .build());

        final Map<Long, EntityDTO> awsEntities =
            sdkDtosFromFile(getClass(),
                "protobuf/messages/aws_engineering_entity_dto.json",
                1L);
        final Map<Long, EntityDTO> awsBillingEntities =
            sdkDtosFromFile(getClass(),
                "protobuf/messages/aws_billing_engineering_entity_dto.json",
                awsEntities.size() + 1L);

        addEntities(awsEntities, awsTargetId);
        addEntities(awsBillingEntities, awsBillingTargetId);

        setOperationsForProbe(awsBillingProbeId,
            Collections.singletonList(new AwsBillingStitchingOperation()));
        setOperationsForProbe(awsProbeId, Collections.emptyList());

        final StitchingManager stitchingManager = new StitchingManager(stitchingOperationStore,
            preStitchingOperationLibrary, postStitchingOperationLibrary, probeStore, targetStore,
            cpuCapacityStore);

        final Target awsBillingTarget = mock(Target.class);
        when(awsBillingTarget.getId()).thenReturn(awsBillingTargetId);

        when(targetStore.getProbeTargets(awsBillingProbeId))
            .thenReturn(Collections.singletonList(awsBillingTarget));

        final Target awsTarget = mock(Target.class);
        when(awsTarget.getId()).thenReturn(awsTargetId);

        when(targetStore.getProbeTargets(awsProbeId))
            .thenReturn(Collections.singletonList(awsTarget));

        when(probeStore.getProbeIdsForCategory(ProbeCategory.BILLING))
            .thenReturn(Collections.singletonList(awsBillingProbeId));
        when(probeStore.getProbeIdsForCategory(ProbeCategory.CLOUD_MANAGEMENT))
            .thenReturn(Collections.singletonList(awsProbeId));
        when(probeStore.getProbeIdForType("AWS"))
            .thenReturn(Optional.of(awsProbeId));

        final List<String> expectedDisplayNamesWithStitchedOsName = Arrays.asList(
            "michael-turbo-62",
            "eva-turbo-61"
        );

        // Before stitching OS names for these VMs are set to some custom values
        final List<EntityDTO> preStitchedVMs = awsEntities.values().stream()
            .filter((entity -> expectedDisplayNamesWithStitchedOsName.contains(
                entity.getDisplayName())))
            .collect(Collectors.toList());
        assertEquals(2, preStitchedVMs.size());
        preStitchedVMs.forEach(vm -> {
            assertNotEquals("Linux", vm.getVirtualMachineData().getGuestName());
            vm.getCommoditiesBoughtList()
                .stream()
                .flatMap(commodityBought -> commodityBought.getBoughtList()
                    .stream()
                    .map(CommodityDTO::toBuilder)
                    .filter(this::isLicenseAccessCommodity))
                .forEach(licenseAccessCommodity ->
                    assertEquals("UNKNOWN", licenseAccessCommodity.getKey()));
        });

        final StitchingContext stitchingContext = entityStore.constructStitchingContext();
        final IStitchingJournal<StitchingEntity> journal = journalFactory.stitchingJournal(
            stitchingContext);
        stitchingManager.stitch(stitchingContext, journal);

        // After stitching, the VMs should have their guest OS name and license access
        // commodity changed to Linux
        final List<StitchingEntity> stitchedVMs = stitchingContext.getStitchingGraph().entities()
            .filter(entity -> expectedDisplayNamesWithStitchedOsName.contains(entity.getDisplayName()))
            .collect(Collectors.toList());
        assertEquals(2, stitchedVMs.size());
        stitchedVMs.forEach(vm -> {
            assertEquals("Linux",
                vm.getEntityBuilder().getVirtualMachineData().getGuestName());
            vm.getCommodityBoughtListByProvider().values()
                .stream()
                .flatMap(List::stream)
                .flatMap(commodityBought -> commodityBought.getBoughtList()
                    .stream()
                    .filter(this::isLicenseAccessCommodity))
                .forEach(licenseAccessCommodity ->
                    assertEquals("Linux", licenseAccessCommodity.getKey()));
        });

    }

    private boolean isLicenseAccessCommodity(final CommodityDTO.Builder commodity) {
        return CommodityType.LICENSE_ACCESS == commodity.getCommodityType();
    }
}
