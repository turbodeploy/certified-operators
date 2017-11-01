package com.vmturbo.topology.processor.topology;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Map;
import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.conversions.ConverterTest;
import com.vmturbo.topology.processor.entity.EntitiesValidationException;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.entity.EntityValidator;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.SettingsManager;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.identity.IdentityUninitializedException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.templates.DiscoveredTemplateDeploymentProfileNotifier;

/**
 * Unit test for {@link TopologyHandler}.
 */
public class TopologyHandlerTest {

    private final TopoBroadcastManager topoBroadcastManager =
            mock(TopoBroadcastManager.class);

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private final EntityStore entityStore = new EntityStore(mock(TargetStore.class),
            identityProvider, new EntityValidator());

    private TopologyHandler topologyHandler;

    private final long topologyId = 0L;

    private final long realtimeTopologyContextId = 7000;

    private final long clockTime = 77L;

    private final PolicyManager policyManager = mock(PolicyManager.class);

    private final DiscoveredTemplateDeploymentProfileNotifier discoveredTemplateDeploymentProfileNotifier =
            mock(DiscoveredTemplateDeploymentProfileNotifier.class);

    private final DiscoveredGroupUploader discoveredGroupUploader =
            mock(DiscoveredGroupUploader.class);

    private final SettingsManager settingsManager =
            mock(SettingsManager.class);

    private final Clock clock = mock(Clock.class);

    private final TopologyInfo realtimeTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(realtimeTopologyContextId)
            .setTopologyId(topologyId)
            .setCreationTime(clockTime)
            .setTopologyType(TopologyType.REALTIME)
            .build();

    @Before
    public void init() {
        final IdentityProvider identityProvider = mock(IdentityProvider.class);
        topologyHandler = new TopologyHandler(realtimeTopologyContextId, topoBroadcastManager,
                entityStore, identityProvider, policyManager,
                discoveredTemplateDeploymentProfileNotifier, discoveredGroupUploader, settingsManager,
                clock);
        when(identityProvider.generateTopologyId()).thenReturn(topologyId);
        when(clock.millis()).thenReturn(clockTime);
    }

    @Test
    public void testBroadcastTopology() throws Exception {
        final TopologyBroadcast entitiesListener = mock(TopologyBroadcast.class);
        when(topoBroadcastManager.broadcastTopology(eq(realtimeTopologyInfo)))
            .thenReturn(entitiesListener);

        addTestSnapshots();

        topologyHandler.broadcastLatestTopology();
        verify(entitiesListener, Mockito.times(4)).append(any(TopologyEntityDTO.class));
        verify(entitiesListener).finish();
        verify(discoveredGroupUploader).processQueuedGroups();
        verify(policyManager).applyPolicies(any(TopologyGraph.class),
            any(GroupResolver.class));
        verify(discoveredTemplateDeploymentProfileNotifier).sendTemplateDeploymentProfileData();
    }

    @Test
    public void testDuplicateEntities() throws Exception {
        final EntityDTO.Builder builder = EntityDTO.newBuilder()
                .setEntityType(EntityType.VIRTUAL_MACHINE);
        // Set up three topology snapshots from three different targets, all
        // of which have a DTO representing an entity with the same OID.
        // Two entities with equal properties, one entity with different properties.
        final EntityDTO[] entities = new EntityDTO[]{
                builder.setId("test1").build(),
                builder.setId("test1").build(),
                builder.setId("test2").build()
        };

        for (int targetId = 0; targetId < entities.length; ++targetId) {
            final ImmutableMap<Long, EntityDTO> snapshotMap = ImmutableMap.of(1L, entities[targetId]);
            addEntities(targetId, snapshotMap);
        }

        final TopologyBroadcast entitiesListener = mock(TopologyBroadcast.class);
        when(topoBroadcastManager.broadcastTopology(any()))
            .thenReturn(entitiesListener);

        topologyHandler.broadcastLatestTopology();
        verify(entitiesListener).append(any(TopologyEntityDTO.class));
        verify(entitiesListener).finish();
    }

    private void addTestSnapshots() throws Exception {
        final long target1Id = 10001L;
        final long target2Id = 10002L;
        final long target3Id = 10003L;

        CommonDTO.EntityDTO vmProbeDTO = ConverterTest.messageFromJsonFile("protobuf/messages/vm-1.dto.json");
        CommonDTO.EntityDTO pmProbeDTO = ConverterTest.messageFromJsonFile("protobuf/messages/pm-1.dto.json");
        CommonDTO.EntityDTO dsProbeDTO = ConverterTest.messageFromJsonFile("protobuf/messages/ds-1.dto.json");
        CommonDTO.EntityDTO vdcProbeDTO = ConverterTest.messageFromJsonFile("protobuf/messages/vdc-1.dto.json");
        ImmutableMap<Long, EntityDTO> map1 = ImmutableMap.of(1L, pmProbeDTO, 2L, vmProbeDTO, 3L, dsProbeDTO);
        ImmutableMap<Long, EntityDTO> map2 = ImmutableMap.of(4L, vdcProbeDTO);

        addEntities(target1Id, map1);
        addEntities(target2Id, map2);
        addEntities(target3Id, ImmutableMap.of());
    }

    private void addEntities(final long targetId,
                             @Nonnull final Map<Long, EntityDTO> entities)
            throws EntitiesValidationException, IdentityUninitializedException {
        final long probeId = 0;
        Mockito.when(
                identityProvider.getIdsForEntities(eq(probeId),
                        eq(new ArrayList<>(entities.values()))))
                .thenReturn(entities);
        entityStore.entitiesDiscovered(probeId, targetId, new ArrayList<>(entities.values()));
    }
}
