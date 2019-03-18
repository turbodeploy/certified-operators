package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.Test;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.stitching.journal.EmptyStitchingJournal;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.TargetStore;

public class CloudTopologyScopeEditorTest {
    private TopologyStitchingEntity az1London =
                    new TopologyStitchingEntity(EntityDTO.newBuilder().setEntityType(EntityType.AVAILABILITY_ZONE),
                                                10001L, 1L, 100l);
    private TopologyStitchingEntity az2London =
                    new TopologyStitchingEntity(EntityDTO.newBuilder().setEntityType(EntityType.AVAILABILITY_ZONE),
                                                10002L, 1l, 100l);
    private TopologyStitchingEntity azOhio =
                    new TopologyStitchingEntity(EntityDTO.newBuilder().setEntityType(EntityType.AVAILABILITY_ZONE),
                                                10003L, 1l, 100l);
    private TopologyStitchingEntity regionLondon =
                    new TopologyStitchingEntity(EntityDTO.newBuilder()
                                                .setEntityType(EntityType.REGION), 20001L, 1l, 100l);
    private TopologyStitchingEntity regionOhio =
                    new TopologyStitchingEntity(EntityDTO.newBuilder()
                                                .setEntityType(EntityType.REGION), 20002L, 1l, 100l);
    private TopologyStitchingEntity vmInLondon =
                    new TopologyStitchingEntity(EntityDTO.newBuilder()
                                                .setEntityType(EntityType.VIRTUAL_MACHINE), 40001l, 1l, 100l);
    private TopologyStitchingEntity computeTier =
                    new TopologyStitchingEntity(EntityDTO.newBuilder()
                                                .setEntityType(EntityType.COMPUTE_TIER), 30001L, 1l, 100l);
    private TopologyStitchingEntity ba =
                    new TopologyStitchingEntity(EntityDTO.newBuilder()
                                                .setEntityType(EntityType.BUSINESS_ACCOUNT), 50001l, 1l, 100l);
    private TopologyStitchingEntity vmInOhio =
                    new TopologyStitchingEntity(EntityDTO.newBuilder()
                                                .setEntityType(EntityType.VIRTUAL_MACHINE), 40002l, 1l, 100l);
    private TopologyStitchingEntity storageTier =
                    new TopologyStitchingEntity(EntityDTO.newBuilder()
                                                .setEntityType(EntityType.STORAGE_TIER), 70001l, 1l, 100l);
    private TopologyStitchingEntity virtualVolumeInOhio =
                    new TopologyStitchingEntity(EntityDTO.newBuilder()
                                                .setEntityType(EntityType.VIRTUAL_VOLUME), 60001l, 1l, 100l);
     // creating a cloud topology with region London and region Ohio
    // London owns az1London and az2LLondon, Ohio owns azOhio,
    // computeTier connected to London and Ohio
    // vmInLondon connected to az1London, ba owns vmInLondon
    private StitchingContext createTopology () {
        // region london owns az1London and az2London but not az1Ohio
        regionLondon.addConnectedTo(ConnectionType.OWNS_CONNECTION, az1London);
        regionLondon.addConnectedTo(ConnectionType.OWNS_CONNECTION, az2London);
        az1London.addConnectedFrom(ConnectionType.OWNS_CONNECTION, regionLondon);
        az2London.addConnectedFrom(ConnectionType.OWNS_CONNECTION, regionLondon);
        // region ohio owns azOhio
        regionOhio.addConnectedTo(ConnectionType.OWNS_CONNECTION, azOhio);
        azOhio.addConnectedFrom(ConnectionType.OWNS_CONNECTION, regionOhio);
        computeTier.addConnectedTo(ConnectionType.NORMAL_CONNECTION, regionLondon);
        regionLondon.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, computeTier);
        computeTier.addConnectedTo(ConnectionType.NORMAL_CONNECTION, regionOhio);
        regionOhio.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, computeTier);
        vmInLondon.addConnectedTo(ConnectionType.NORMAL_CONNECTION, az1London);
        az1London.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, vmInLondon);
        vmInOhio.addConnectedTo(ConnectionType.NORMAL_CONNECTION, azOhio);
        azOhio.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, vmInOhio);
        ba.addConnectedTo(ConnectionType.NORMAL_CONNECTION, vmInLondon);
        vmInLondon.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, ba);
        virtualVolumeInOhio.addConnectedTo(ConnectionType.NORMAL_CONNECTION, azOhio);
        az1London.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, virtualVolumeInOhio);
        virtualVolumeInOhio.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, vmInOhio);
        vmInOhio.addConnectedTo(ConnectionType.NORMAL_CONNECTION, virtualVolumeInOhio);
        // storage tier can be in both region london and ohio
        storageTier.addConnectedTo(ConnectionType.NORMAL_CONNECTION, regionLondon);
        storageTier.addConnectedTo(ConnectionType.NORMAL_CONNECTION, regionOhio);
        storageTier.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, virtualVolumeInOhio);
        regionLondon.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, storageTier);
        regionOhio.addConnectedFrom(ConnectionType.NORMAL_CONNECTION, storageTier);
        virtualVolumeInOhio.addConnectedTo(ConnectionType.NORMAL_CONNECTION, storageTier);
        Map<EntityDTO.Builder, TopologyStitchingEntity> stitchingEntitiesMap = new HashMap<>();
        stitchingEntitiesMap.put(az1London.getEntityBuilder(), az1London);
        stitchingEntitiesMap.put(az2London.getEntityBuilder(), az2London);
        stitchingEntitiesMap.put(azOhio.getEntityBuilder(), azOhio);
        stitchingEntitiesMap.put(regionLondon.getEntityBuilder(), regionLondon);
        stitchingEntitiesMap.put(regionOhio.getEntityBuilder(), regionOhio);
        stitchingEntitiesMap.put(computeTier.getEntityBuilder(), computeTier);
        stitchingEntitiesMap.put(vmInLondon.getEntityBuilder(), vmInLondon);
        stitchingEntitiesMap.put(vmInOhio.getEntityBuilder(), vmInOhio);
        stitchingEntitiesMap.put(ba.getEntityBuilder(), ba);
        stitchingEntitiesMap.put(virtualVolumeInOhio.getEntityBuilder(), virtualVolumeInOhio);
        stitchingEntitiesMap.put(storageTier.getEntityBuilder(), storageTier);
        Map<EntityType, Map<Long, List<TopologyStitchingEntity>>> entitiesByEntityTypeAndTarget = new HashMap<>();
        Map<Long, List<TopologyStitchingEntity>> azMap = new HashMap<>();
        azMap.put(1l, new ArrayList<>(Arrays.asList(az1London, az2London, azOhio)));
        Map<Long, List<TopologyStitchingEntity>> regionMap = new HashMap<>();
        regionMap.put(1l, new ArrayList<>(Arrays.asList(regionLondon, regionOhio)));
        Map<Long, List<TopologyStitchingEntity>> computeTierMap = new HashMap<>();
        computeTierMap.put(1l, new ArrayList<>(Arrays.asList(computeTier)));
        Map<Long, List<TopologyStitchingEntity>> vmMap = new HashMap<>();
        vmMap.put(1l, new ArrayList<>(Arrays.asList(vmInLondon, vmInOhio)));
        Map<Long, List<TopologyStitchingEntity>> baMap = new HashMap<>();
        baMap.put(1l, new ArrayList<>(Arrays.asList(ba)));
        Map<Long, List<TopologyStitchingEntity>> stMap = new HashMap<>();
        stMap.put(1l, new ArrayList<>(Arrays.asList(storageTier)));
        Map<Long, List<TopologyStitchingEntity>> vvMap = new HashMap<>();
        vvMap.put(1l, new ArrayList<>(Arrays.asList(virtualVolumeInOhio)));
        entitiesByEntityTypeAndTarget.put(EntityType.AVAILABILITY_ZONE, azMap);
        entitiesByEntityTypeAndTarget.put(EntityType.REGION, regionMap);
        entitiesByEntityTypeAndTarget.put(EntityType.COMPUTE_TIER, computeTierMap);
        entitiesByEntityTypeAndTarget.put(EntityType.VIRTUAL_MACHINE, vmMap);
        entitiesByEntityTypeAndTarget.put(EntityType.BUSINESS_ACCOUNT, baMap);
        entitiesByEntityTypeAndTarget.put(EntityType.STORAGE_TIER, stMap);
        entitiesByEntityTypeAndTarget.put(EntityType.VIRTUAL_VOLUME, vvMap);

        IdentityProvider provider = mock(IdentityProvider.class);
        TargetStore targetStore = mock(TargetStore.class);
        StitchingContext context = StitchingContext.newBuilder(11)
                        .setIdentityProvider(provider)
                        .setTargetStore(targetStore)
                        .build();
        TopologyStitchingGraph graph = new TopologyStitchingGraph(11);
        try {
            Field f1 = graph.getClass().getDeclaredField("stitchingEntities");
            f1.setAccessible(true);
            f1.set(graph, stitchingEntitiesMap);
            Field f2 = context.getClass().getDeclaredField("stitchingGraph");
            f2.setAccessible(true);
            f2.set(context, graph);
            Field f3 = context.getClass().getDeclaredField("entitiesByEntityTypeAndTarget");
            f3.setAccessible(true);
            f3.set(context, entitiesByEntityTypeAndTarget);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return context;
    }
    @Test
    public void testScope() {
        final PlanScope planScope = PlanScope.newBuilder()
                .addScopeEntries(PlanScopeEntry.newBuilder().setClassName("Region")
                        .setScopeObjectOid(20001L).setDisplayName("London").build()).build();
        StitchingContext context = createTopology();
        StitchingJournalFactory journalFactory = mock(StitchingJournalFactory.class);
        final IStitchingJournal<StitchingEntity> journal = spy(new EmptyStitchingJournal<>());
        when(journalFactory.stitchingJournal(eq(context))).thenReturn(journal);
        CloudTopologyScopeEditor scopeEditor = new CloudTopologyScopeEditor();
        scopeEditor.scope(context, planScope, journalFactory);
        assertTrue(context.size()==7);
        assertTrue(context.hasEntity(az1London));
        assertTrue(context.hasEntity(az2London));
        assertTrue(context.hasEntity(regionLondon));
        assertTrue(context.hasEntity(computeTier));
        assertTrue(context.hasEntity(vmInLondon));
        assertTrue(context.hasEntity(ba));
        assertTrue(context.hasEntity(storageTier));
        assertFalse(context.hasEntity(virtualVolumeInOhio));
        assertFalse(context.hasEntity(vmInOhio));
        assertFalse(context.hasEntity(azOhio));
        assertFalse(context.hasEntity(regionOhio));
    }
}
