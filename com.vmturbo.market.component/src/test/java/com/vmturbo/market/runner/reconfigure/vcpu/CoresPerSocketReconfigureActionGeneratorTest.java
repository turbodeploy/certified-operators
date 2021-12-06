package com.vmturbo.market.runner.reconfigure.vcpu;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Test cores per socket actions generated correctly.
 */
public class CoresPerSocketReconfigureActionGeneratorTest extends VcpuScalingReconfigureActionGeneratorTestUtils {

    private SettingPolicyServiceMole settingPolicyServiceMole = spy(new SettingPolicyServiceMole());

    private GrpcTestServer grpcTestServer = GrpcTestServer.newServer(settingPolicyServiceMole);

    private SettingPolicyServiceBlockingStub settingPolicyService;

    private Map<Long, TopologyEntityDTO> topology;

    private CoresPerSocketReconfigureActionGenerator
            generator = new CoresPerSocketReconfigureActionGenerator();

    /**
     * Set up.
     * @throws Exception any exception.
     */
    @Before
    public void setup() throws Exception {
        grpcTestServer.start();
        settingPolicyService = SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        topology = new HashMap<>();
        IdentityGenerator.initPrefix(0);
    }

    /**
     * Test cores per socket action generator works correctly.
     */
    @Test
    public void testCoresPerSocketActionGenerated() {
        TopologyEntityDTO vm1 = makeVM(1, 2, true);
        TopologyEntityDTO vm2 = makeVM(2, 4, true);
        GetEntitySettingsResponse response1 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 1, "SOCKETS", null);
        GetEntitySettingsResponse response2 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 1, "USER_SPECIFIED", null);
        GetEntitySettingsResponse response3 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 2, null, 2f);

        when(settingPolicyServiceMole.getEntitySettings(any(GetEntitySettingsRequest.class)))
                .thenReturn(ImmutableList.of(response1), ImmutableList.of(response2), ImmutableList.of(response3));

        topology.put(1L, vm1);
        topology.put(2L, vm2);

        List<Action> actions = generator.execute(settingPolicyService, topology,
                Collections.emptyList());
        //Generate actions only if the request cores per socket are different from current value.
        Assert.assertEquals(1, actions.size());
        Assert.assertEquals(4, actions.get(0).getInfo().getReconfigure().getSettingChange().getCurrentValue(), 0.0001);
        Assert.assertEquals(2, actions.get(0).getInfo().getReconfigure().getSettingChange().getNewValue(), 0.0001);
        Assert.assertEquals(2, actions.get(0).getInfo().getReconfigure().getTarget().getId(), 0.0001);
        Assert.assertTrue(actions.get(0).getExplanation().getReconfigure().getReasonSettingsList().containsAll(ImmutableList.of(1L, 2L)));
    }

    /**
     * Test cores per socket action generator works correctly.
     */
    @Test
    public void testActionNotGeneratedIfTheVMHasResizeAction() {
        TopologyEntityDTO vm1 = makeVM(1, 4, true);
        TopologyEntityDTO vm2 = makeVM(2, 4, true);
        GetEntitySettingsResponse response1 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 1, "SOCKETS", null);
        GetEntitySettingsResponse response2 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 1, "USER_SPECIFIED", null);
        GetEntitySettingsResponse response3 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 2, null, 2f);

        when(settingPolicyServiceMole.getEntitySettings(any(GetEntitySettingsRequest.class)))
                .thenReturn(ImmutableList.of(response1), ImmutableList.of(response2), ImmutableList.of(response3));

        topology.put(1L, vm1);
        topology.put(2L, vm2);

        List<Action> resizeActions = Collections.singletonList(makeResizeAction(1L));
        List<Action> actions = generator.execute(settingPolicyService, topology, resizeActions);
        //Generate actions only if the VM doesn't have resize action on it.
        Assert.assertEquals(1, actions.size());
        Assert.assertEquals(4, actions.get(0).getInfo().getReconfigure().getSettingChange().getCurrentValue(), 0.0001);
        Assert.assertEquals(2, actions.get(0).getInfo().getReconfigure().getSettingChange().getNewValue(), 0.0001);
        Assert.assertEquals(2, actions.get(0).getInfo().getReconfigure().getTarget().getId(), 0.0001);
        Assert.assertTrue(actions.get(0).getExplanation().getReconfigure().getReasonSettingsList().containsAll(ImmutableList.of(1L, 2L)));
    }


    /**
     * Test cores per socket action generator should not generate actions where no user_specified policies.
     */
    @Test
    public void testNoActionsWithoutSpecify() {
        TopologyEntityDTO vm1 = makeVM(1, 2, true);
        TopologyEntityDTO vm2 = makeVM(2, 4, true);
        GetEntitySettingsResponse response1 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 1, "MHZ", null);
        GetEntitySettingsResponse response2 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 1, "USER_SPECIFIED", null);
        GetEntitySettingsResponse response3 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 2, null, 2f);

        when(settingPolicyServiceMole.getEntitySettings(any(GetEntitySettingsRequest.class)))
                .thenReturn(ImmutableList.of(response1), ImmutableList.of(response2), ImmutableList.of(response3));

        topology.put(1L, vm1);
        topology.put(2L, vm2);

        List<Action> actions = generator.execute(settingPolicyService, topology, Collections.emptyList());
        //Generate actions only if the request cores per socket are different from current value.
        Assert.assertEquals(0, actions.size());
    }

    /**
     * Test cores per socket action generator should not generate actions where no user_specified policies.
     */
    @Test
    public void testNoActionsWithoutUserSpecified() {
        TopologyEntityDTO vm1 = makeVM(1, 2, true);
        TopologyEntityDTO vm2 = makeVM(2, 4, true);
        GetEntitySettingsResponse response1 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 1, "SOCKETS", null);
        GetEntitySettingsResponse response2 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 1, "PRESERVE", null);
        GetEntitySettingsResponse response3 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 2, null, 2f);

        when(settingPolicyServiceMole.getEntitySettings(any(GetEntitySettingsRequest.class)))
                .thenReturn(ImmutableList.of(response1), ImmutableList.of(response2), ImmutableList.of(response3));

        topology.put(1L, vm1);
        topology.put(2L, vm2);

        List<Action> actions = generator.execute(settingPolicyService, topology, Collections.emptyList());
        //Generate actions only if the request cores per socket are different from current value.
        Assert.assertEquals(0, actions.size());
    }


    /**
     * Test actions genereted only for entitySettingGroups with USER_SPECIFIED value.
     */
    @Test
    public void testMultipleEntitySettingGroup() {
        //Only VMs who have USER_SPECIFIED policy will have the actions generated
        TopologyEntityDTO vm1 = makeVM(1, 4, true);
        TopologyEntityDTO vm2 = makeVM(2, 4, true);
        EntitySettingGroup.Builder settingGroup1 = makeEntitySettingGroup(ImmutableList.of(1L), 1, "USER_SPECIFIED", null);
        EntitySettingGroup.Builder settingGroup2 = makeEntitySettingGroup(ImmutableList.of(2L), 1, "PRESERVE", null);
        GetEntitySettingsResponse response1 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 1, "SOCKETS", null);
        GetEntitySettingsResponse response2 = GetEntitySettingsResponse.newBuilder()
                .addSettingGroup(settingGroup1)
                .addSettingGroup(settingGroup2)
                .build();
        GetEntitySettingsResponse response3 = makeGetEntitySettingsResponse(ImmutableList.of(1L, 2L), 2, null, 2f);

        when(settingPolicyServiceMole.getEntitySettings(any(GetEntitySettingsRequest.class)))
                .thenReturn(ImmutableList.of(response1), ImmutableList.of(response2), ImmutableList.of(response3));

        topology.put(1L, vm1);
        topology.put(2L, vm2);

        List<Action> actions = generator.execute(settingPolicyService, topology, Collections.emptyList());

        Assert.assertEquals(1, actions.size());
        Assert.assertEquals(1, actions.get(0).getInfo().getReconfigure().getTarget().getId(), 0.0001);
    }

}