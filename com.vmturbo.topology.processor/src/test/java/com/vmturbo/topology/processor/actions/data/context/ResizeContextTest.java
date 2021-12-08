package com.vmturbo.topology.processor.actions.data.context;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Explanation.ResizeExplanation;
import com.vmturbo.common.protobuf.action.ActionDTO.Resize;
import com.vmturbo.common.protobuf.topology.ActionExecution.ExecuteActionRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData;
import com.vmturbo.topology.processor.actions.data.EntityRetrievalException;
import com.vmturbo.topology.processor.actions.data.EntityRetriever;
import com.vmturbo.topology.processor.actions.data.GroupAndPolicyRetriever;
import com.vmturbo.topology.processor.actions.data.spec.ActionDataManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * {@link ResizeContextTest} checks that {@link ResizeContext} is working as expected.
 */
public class ResizeContextTest {

    /**
     * Checks that desired cores per socket value placed from {@link Resize#getNewCpsr()} into
     * target entity {@link VirtualMachineData} instance.
     *
     * @throws ContextCreationException in case context cannot be created
     * @throws EntityRetrievalException in case entity cannot be retrieved.
     */
    @Test
    public void checkResizeHasCpsrInformation()
                    throws ContextCreationException, EntityRetrievalException {
        final ExecuteActionRequest request = createActionRequest();
        final ActionDataManager actionDataManager = Mockito.mock(ActionDataManager.class);
        final EntityStore entityStore = Mockito.mock(EntityStore.class);
        final EntityRetriever entityRetriever = Mockito.mock(EntityRetriever.class);
        final TargetStore targetStore = Mockito.mock(TargetStore.class);
        final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
        final SecureStorageClient secureStorageClient = Mockito.mock(SecureStorageClient.class);
        final GroupAndPolicyRetriever groupPolicyRetriever =
                        Mockito.mock(GroupAndPolicyRetriever.class);
        final long targetEntityId = 1L;
        final EntityDTO targetEntity = EntityDTO.newBuilder().setId(String.valueOf(1L))
                        .setVirtualMachineData(
                                        VirtualMachineData.newBuilder().setCoresPerSocketRatio(1)
                                                        .setCoresPerSocketChangeable(true).build())
                        .setEntityType(EntityType.VIRTUAL_MACHINE).build();
        Mockito.doReturn(targetEntity)
                        .when(entityRetriever).fetchAndConvertToEntityDTO(targetEntityId);
        final ResizeContext resizeContext =
                        new ResizeContext(request, actionDataManager, entityStore, entityRetriever,
                                        targetStore, probeStore, groupPolicyRetriever,
                                        secureStorageClient);
        final EntityDTO entityDto = resizeContext.getFullEntityDTO(targetEntityId);
        Assert.assertThat(entityDto.getVirtualMachineData().getCoresPerSocketRatio(),
                        CoreMatchers.is(2));
    }

    private static ExecuteActionRequest createActionRequest() {
        final ActionInfo actionInfo = ActionInfo.newBuilder()
                        .setResize(Resize.newBuilder().setNewCpsr(2)
                                        .setTarget(ActionEntity.newBuilder().setId(1)
                                                        .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                                                        .build()).build()).build();
        final ResizeExplanation explanation =
                        ResizeExplanation.newBuilder().setDeprecatedStartUtilization(1)
                                        .setDeprecatedEndUtilization(2)
                                        .setReason(CommodityType.newBuilder()
                                                        .setType(CommodityDTO.CommodityType.VCPU_VALUE)
                                                        .build()).build();
        final ActionSpec actionSpec = ActionSpec.newBuilder().setRecommendation(Action.newBuilder()
                        .setExplanation(Explanation.newBuilder().setResize(explanation).build())
                        .setId(3L).setDeprecatedImportance(1).setInfo(actionInfo).build()).build();
        return ExecuteActionRequest.newBuilder().setActionSpec(actionSpec).build();
    }
}
