package com.vmturbo.topology.processor.actions.data.spec;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.GroupAndPolicyRetriever;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;

/**
 * Tests {@link ActionDataManager} class.
 */
public class ActionDataManagerTest {
    private static final long VM_ID = 1001L;
    private static final long PM_1_ID = 2001L;
    private static final long PM_2_ID = 2002L;
    private static final long CLUSTER_1_ID = 3001L;
    private static final long CLUSTER_2_ID = 3002L;
    private static final String CLUSTER_1_DISPLAY_NAME = "Cluster 1";
    private static final String CLUSTER_2_DISPLAY_NAME = "Cluster 2";

    private GrpcTestServer server;

    private SearchServiceGrpc.SearchServiceBlockingStub searchServiceRpc;

    private TopologyToSdkEntityConverter topologyToSdkEntityConverter;

    private EntityRetriever entityRetriever;

    private GroupAndPolicyRetriever groupAndPolicyRetriever;

    private ActionDataManager actionDataManager;

    private final ActionDTO.ActionInfo actionInfoMove = ActionDTO.ActionInfo.newBuilder()
        .setMove(ActionDTO.Move.newBuilder()
            .setTarget(ActionDTO.ActionEntity.newBuilder()
              .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber())
              .setId(VM_ID)
              .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM)
            )
            .addChanges(ActionDTO.ChangeProvider.newBuilder()
                .setDestination(ActionDTO.ActionEntity.newBuilder()
                    .setType(CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE.getNumber())
                    .setId(PM_2_ID)
                    .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM))
                .build())
            .build())
        .build();

    private final ActionDTO.ActionInfo actionInfoProvision = ActionDTO.ActionInfo.newBuilder()
        .setProvision(ActionDTO.Provision.newBuilder()
            .setEntityToClone(ActionDTO.ActionEntity.newBuilder()
                .setType(EntityType.PHYSICAL_MACHINE.getNumber())
                .setId(PM_1_ID)
                .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.ON_PREM))
            .build()).build();

    private final TopologyDTO.TopologyEntityDTO vmEntity  = TopologyDTO.TopologyEntityDTO
        .newBuilder()
        .setOid(VM_ID)
        .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber())
        .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
            .setProviderEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
            .setProviderId(PM_1_ID)
        )
        .build();

    private GroupDTO.Grouping cluster1 = GroupDTO.Grouping.newBuilder()
        .setId(CLUSTER_1_ID)
        .setDefinition(GroupDTO.GroupDefinition.newBuilder()
          .setDisplayName(CLUSTER_1_DISPLAY_NAME)
          .setType(CommonDTO.GroupDTO.GroupType.COMPUTE_HOST_CLUSTER)
        )
        .build();

    private GroupDTO.Grouping cluster2 = GroupDTO.Grouping.newBuilder()
        .setId(CLUSTER_2_ID)
        .setDefinition(GroupDTO.GroupDefinition.newBuilder()
            .setDisplayName(CLUSTER_2_DISPLAY_NAME)
            .setType(CommonDTO.GroupDTO.GroupType.COMPUTE_HOST_CLUSTER)
        )
        .build();

    /**
     * Sets up the test.
     *
     * @throws IOException if something goes wrong.
     */
    @Before
    public void setup() throws IOException {
        server = GrpcTestServer.newServer();
        server.start();
        searchServiceRpc = SearchServiceGrpc.newBlockingStub(server.getChannel());
        topologyToSdkEntityConverter = mock(TopologyToSdkEntityConverter.class);
        entityRetriever = mock(EntityRetriever.class);
        groupAndPolicyRetriever = mock(GroupAndPolicyRetriever.class);
        actionDataManager = new ActionDataManager(searchServiceRpc, topologyToSdkEntityConverter,
            entityRetriever, groupAndPolicyRetriever, true);

        when(entityRetriever.retrieveTopologyEntity(VM_ID)).thenReturn(Optional.of(vmEntity));
        when(groupAndPolicyRetriever.getHostCluster(PM_1_ID)).thenReturn(Optional.of(cluster1));
        when(groupAndPolicyRetriever.getHostCluster(PM_2_ID)).thenReturn(Optional.of(cluster2));
    }

    /**
     * Checks if cluster information is populated.
     */
    @Test
    public void testPopulatingClusterInformationVirtualMachineMove() {
        // ARRANGE

        // ACT
        List<CommonDTO.ContextData> contextData = actionDataManager.getContextData(actionInfoMove);

        // ASSERT
        assertThat(contextData.size(), is(4));
        assertThat(getContextValue(contextData, SDKConstants.CURRENT_HOST_CLUSTER_ID),
            is(String.valueOf(CLUSTER_1_ID)));
        assertThat(getContextValue(contextData, SDKConstants.CURRENT_HOST_CLUSTER_DISPLAY_NAME),
            is(CLUSTER_1_DISPLAY_NAME));
        assertThat(getContextValue(contextData, SDKConstants.NEW_HOST_CLUSTER_ID),
            is(String.valueOf(CLUSTER_2_ID)));
        assertThat(getContextValue(contextData, SDKConstants.NEW_HOST_CLUSTER_DISPLAY_NAME),
            is(CLUSTER_2_DISPLAY_NAME));

    }

    /**
     * Tests that cluster information is not populated for cloud entities.
     */
    @Test
    public void testPopulateClusterInformationCloud() {
        // ARRANGE
        final ActionDTO.ActionInfo actionInfo = ActionDTO.ActionInfo.newBuilder()
            .setScale(ActionDTO.Scale.newBuilder()
                .setTarget(ActionDTO.ActionEntity.newBuilder()
                    .setType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber())
                    .setId(VM_ID)
                    .setEnvironmentType(EnvironmentTypeEnum.EnvironmentType.CLOUD)
                )
            )
            .build();

        // ACT
        List<CommonDTO.ContextData> contextData = actionDataManager.getContextData(actionInfo);

        // ASSERT
        assertThat(contextData.size(), is(0));
    }

    /**
     * Tests adding information for cluster for provision action.
     */
    @Test
    public void testPopulateClusterInformationPhysicalMachineProvision() {
        // ARRANGE

        // ACT
        List<CommonDTO.ContextData> contextData = actionDataManager.getContextData(actionInfoProvision);

        // ASSERT
        assertThat(contextData.size(), is(3));
        assertThat(getContextValue(contextData, SDKConstants.CURRENT_HOST_CLUSTER_ID),
            is(String.valueOf(CLUSTER_1_ID)));
        assertThat(getContextValue(contextData, SDKConstants.CURRENT_HOST_CLUSTER_DISPLAY_NAME),
            is(CLUSTER_1_DISPLAY_NAME));

    }

    /**
     * Test the case that we have action with target vm but we cannot lookup the VM.
     */
    @Test
    public void testPopulateClusterInformationTargetVmDoesNotExists() {
        // ARRANGE
        when(entityRetriever.retrieveTopologyEntity(VM_ID)).thenReturn(Optional.empty());

        // ACT
        List<CommonDTO.ContextData> contextData = actionDataManager.getContextData(actionInfoMove);

        // ASSERT
        assertThat(contextData.size(), is(2));
        assertThat(getContextValue(contextData, SDKConstants.NEW_HOST_CLUSTER_ID),
            is(String.valueOf(CLUSTER_2_ID)));
        assertThat(getContextValue(contextData, SDKConstants.NEW_HOST_CLUSTER_DISPLAY_NAME),
            is(CLUSTER_2_DISPLAY_NAME));
    }

    /**
     * Test the case that we have action with target vm that does not have PM provider.
     */
    @Test
    public void testPopulateClusterInformationTargetVmDoesNotHavePm() {
        // ARRANGE
        when(entityRetriever.retrieveTopologyEntity(VM_ID)).thenReturn(Optional.of(
            TopologyDTO.TopologyEntityDTO
            .newBuilder()
            .setOid(VM_ID)
            .setEntityType(CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE.getNumber())
            .addCommoditiesBoughtFromProviders(CommoditiesBoughtFromProvider.newBuilder()
                .setProviderEntityType(EntityType.STORAGE.getNumber())
                .setProviderId(32L)
            )
            .build()));

        // ACT
        List<CommonDTO.ContextData> contextData = actionDataManager.getContextData(actionInfoMove);

        // ASSERT
        assertThat(contextData.size(), is(2));
        assertThat(getContextValue(contextData, SDKConstants.NEW_HOST_CLUSTER_ID),
            is(String.valueOf(CLUSTER_2_ID)));
        assertThat(getContextValue(contextData, SDKConstants.NEW_HOST_CLUSTER_DISPLAY_NAME),
            is(CLUSTER_2_DISPLAY_NAME));
    }

    private static String getContextValue(List<CommonDTO.ContextData> contextData, String key) {
        return contextData
            .stream()
            .filter(c -> key.equals(c.getContextKey()))
            .map(CommonDTO.ContextData::getContextValue)
            .findAny().orElse(null);
    }

}