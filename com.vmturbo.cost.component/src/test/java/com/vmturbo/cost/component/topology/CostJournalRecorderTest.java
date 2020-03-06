package com.vmturbo.cost.component.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.CostDebug.EnableCostRecordingRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.EnableCostRecordingRequest.CountFilter;
import com.vmturbo.common.protobuf.cost.CostDebug.EnableCostRecordingRequest.EntityFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CostJournalRecorderTest {

    private static final TopologyEntityDTO VM = TopologyEntityDTO.newBuilder()
            .setOid(7)
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .build();

    private static final TopologyEntityDTO DB = TopologyEntityDTO.newBuilder()
            .setOid(777)
            .setEntityType(EntityType.DATABASE_VALUE)
            .build();

    private static final TopologyEntityDTO VOLUME = TopologyEntityDTO.newBuilder()
            .setOid(77)
            .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
            .build();

    @Test
    public void testDefaultSelectorSelectsDependencies() {
        CostJournalRecorder recorder = new CostJournalRecorder();
        CostJournal<TopologyEntityDTO> vmJournal = makeJournal(VM);
        CostJournal<TopologyEntityDTO> volumeJournal = makeJournal(VOLUME);
        when(vmJournal.getDependentJournals()).thenReturn(Stream.of(volumeJournal));

        final Map<Long, CostJournal<TopologyEntityDTO>> journalMap = new LinkedHashMap<>();
        journalMap.put(VOLUME.getOid(), volumeJournal);
        journalMap.put(VM.getOid(), vmJournal);
        recorder.recordCostJournals(journalMap);

        assertThat(recorder.getJournalDescriptions(Collections.emptySet()).count(), is(2L));
    }

    @Test
    public void testDefaultSelectorSelectsOne() {
        CostJournalRecorder recorder = new CostJournalRecorder();
        CostJournal<TopologyEntityDTO> vmJournal = makeJournal(VM);
        CostJournal<TopologyEntityDTO> dbJournal = makeJournal(DB);

        final Map<Long, CostJournal<TopologyEntityDTO>> journalMap = new LinkedHashMap<>();
        journalMap.put(DB.getOid(), dbJournal);
        journalMap.put(VM.getOid(), vmJournal);
        recorder.recordCostJournals(journalMap);

        final List<String> descriptions = recorder.getJournalDescriptions(Collections.emptySet())
                        .collect(Collectors.toList());
        // DB came fist in the map.
        assertThat(descriptions, contains(dbJournal.toString()));
    }

    @Test
    public void overrideSelectorSpecificId() {
        final CostJournalRecorder recorder = new CostJournalRecorder();
        final CostJournal<TopologyEntityDTO> vmJournal = makeJournal(VM);
        final CostJournal<TopologyEntityDTO> volumeJournal = makeJournal(VOLUME);
        when(vmJournal.getDependentJournals()).thenReturn(Stream.of(volumeJournal));
        final CostJournal<TopologyEntityDTO> dbJournal = makeJournal(DB);

        final Map<Long, CostJournal<TopologyEntityDTO>> journalMap = new LinkedHashMap<>();
        journalMap.put(DB.getOid(), dbJournal);
        // VM is added second, so the default selector wouldn't pick it.
        journalMap.put(VM.getOid(), vmJournal);
        journalMap.put(VOLUME.getOid(), volumeJournal);

        // Ask to select the VM.
        recorder.overrideEntitySelector(EnableCostRecordingRequest.newBuilder()
                .setEntity(EntityFilter.newBuilder()
                        .addEntityIds(VM.getOid()))
                .build());

        recorder.recordCostJournals(journalMap);

        final List<String> descriptions = recorder.getJournalDescriptions(Collections.emptySet())
                .collect(Collectors.toList());
        // DB came first in the map, so the default selector would have picked it.
        // We overrode the selector to pick the VM. It should also get the volume dependency.
        assertThat(descriptions, containsInAnyOrder(vmJournal.toString(), volumeJournal.toString()));
    }

    @Test
    public void overrideSelectorCountGreaterThanTotal() {
        final CostJournalRecorder recorder = new CostJournalRecorder();
        final CostJournal<TopologyEntityDTO> vmJournal = makeJournal(VM);
        final CostJournal<TopologyEntityDTO> volumeJournal = makeJournal(VOLUME);
        when(vmJournal.getDependentJournals()).thenReturn(Stream.of(volumeJournal));
        final CostJournal<TopologyEntityDTO> dbJournal = makeJournal(DB);

        final Map<Long, CostJournal<TopologyEntityDTO>> journalMap = new LinkedHashMap<>();
        journalMap.put(DB.getOid(), dbJournal);
        journalMap.put(VM.getOid(), vmJournal);
        journalMap.put(VOLUME.getOid(), volumeJournal);

        // Ask to select all.
        recorder.overrideEntitySelector(EnableCostRecordingRequest.newBuilder()
                .setCount(CountFilter.newBuilder()
                    .setCount(100))
                .build());

        recorder.recordCostJournals(journalMap);

        final List<String> descriptions = recorder.getJournalDescriptions(Collections.emptySet())
                .collect(Collectors.toList());
        // Should contain everything.
        assertThat(descriptions, containsInAnyOrder(dbJournal.toString(),
                vmJournal.toString(), volumeJournal.toString()));
    }

    @Test
    public void overrideSelectorSelectAll() {
        final CostJournalRecorder recorder = new CostJournalRecorder();
        final CostJournal<TopologyEntityDTO> vmJournal = makeJournal(VM);
        final CostJournal<TopologyEntityDTO> volumeJournal = makeJournal(VOLUME);
        when(vmJournal.getDependentJournals()).thenReturn(Stream.of(volumeJournal));
        final CostJournal<TopologyEntityDTO> dbJournal = makeJournal(DB);

        final Map<Long, CostJournal<TopologyEntityDTO>> journalMap = new LinkedHashMap<>();
        journalMap.put(DB.getOid(), dbJournal);
        journalMap.put(VM.getOid(), vmJournal);
        journalMap.put(VOLUME.getOid(), volumeJournal);

        // Ask to select all.
        recorder.overrideEntitySelector(EnableCostRecordingRequest.newBuilder()
                .setRecordAll(true)
                .build());

        recorder.recordCostJournals(journalMap);

        final List<String> descriptions = recorder.getJournalDescriptions(Collections.emptySet())
                .collect(Collectors.toList());
        // Should contain everything.
        assertThat(descriptions, containsInAnyOrder(dbJournal.toString(),
                vmJournal.toString(), volumeJournal.toString()));
    }

    @Test
    public void testGetDescriptionsForSpecificId() {
        final CostJournalRecorder recorder = new CostJournalRecorder();
        final CostJournal<TopologyEntityDTO> vmJournal = makeJournal(VM);
        final CostJournal<TopologyEntityDTO> volumeJournal = makeJournal(VOLUME);
        when(vmJournal.getDependentJournals()).thenReturn(Stream.of(volumeJournal));
        final CostJournal<TopologyEntityDTO> dbJournal = makeJournal(DB);

        final Map<Long, CostJournal<TopologyEntityDTO>> journalMap = new LinkedHashMap<>();
        journalMap.put(DB.getOid(), dbJournal);
        journalMap.put(VM.getOid(), vmJournal);
        journalMap.put(VOLUME.getOid(), volumeJournal);

        // Ask to select all.
        recorder.overrideEntitySelector(EnableCostRecordingRequest.newBuilder()
                .setRecordAll(true)
                .build());

        recorder.recordCostJournals(journalMap);

        // Ask for the VM's OID.
        final List<String> descriptions = recorder.getJournalDescriptions(Collections.singleton(VM.getOid()))
                .collect(Collectors.toList());

        // Should contain the VM's journal, as well as the dependent volume.
        assertThat(descriptions, containsInAnyOrder(vmJournal.toString(), volumeJournal.toString()));
    }

    @Test
    public void testNumBroadcastsReset() {
        final CostJournalRecorder recorder = new CostJournalRecorder();
        final CostJournal<TopologyEntityDTO> vmJournal = makeJournal(VM);
        final CostJournal<TopologyEntityDTO> volumeJournal = makeJournal(VOLUME);
        when(vmJournal.getDependentJournals()).thenReturn(Stream.of(volumeJournal));
        final CostJournal<TopologyEntityDTO> dbJournal = makeJournal(DB);

        final Map<Long, CostJournal<TopologyEntityDTO>> journalMap = new LinkedHashMap<>();
        journalMap.put(DB.getOid(), dbJournal);
        journalMap.put(VM.getOid(), vmJournal);
        journalMap.put(VOLUME.getOid(), volumeJournal);

        // Ask to select all.
        recorder.overrideEntitySelector(EnableCostRecordingRequest.newBuilder()
                .setRecordAll(true)
                .setNumBroadcasts(1)
                .build());

        // First broadcast. After this, the selector should be reset.
        recorder.recordCostJournals(journalMap);

        // Second broadcast.
        recorder.recordCostJournals(journalMap);

        final List<String> descriptions = recorder.getJournalDescriptions(Collections.emptySet())
                .collect(Collectors.toList());

        // Default selector should select the db journal only, because it's first in the
        // map.
        assertThat(descriptions, containsInAnyOrder(dbJournal.toString()));
    }

    private CostJournal<TopologyEntityDTO> makeJournal(TopologyEntityDTO entity) {
        final CostJournal<TopologyEntityDTO> journal = mock(CostJournal.class);
        when(journal.getEntity()).thenReturn(entity);
        when(journal.getDependentJournals()).thenReturn(Stream.empty());
        return journal;
    }


}
