package com.vmturbo.topology.processor.actions.data.spec;

import static com.vmturbo.topology.processor.actions.ActionExecutionTestUtils.createActionEntity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;

import java.util.List;

import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.common.builders.SDKConstants;
import com.vmturbo.platform.common.dto.CommonDTO.ContextData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.conversions.TopologyToSdkEntityConverter;

/**
 * Test for VolumeScaleSpec and Scale volume action context.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SearchServiceBlockingStub.class, SpecSearchUtil.class})
public class VolumeScaleSpecFactoryTest {

    private static final String VM_DISPLAYNAME = "vmDisplayName";
    private static final String RESOURCEGROUP_NAME = "resourceGroupName";

    /**
     * Test scaling volume action context, which contains attached VM's name and resourceGroup info.
     */
    @Test
    public void testVolumeScaleActionContext() {
        // Volume scale action
        final ActionDTO.ActionInfo scaleActionInfo = ActionInfo.newBuilder()
                .setScale(ActionDTO.Scale.newBuilder()
                        .setTarget(createActionEntity(7, EntityType.VIRTUAL_VOLUME))
                        .addChanges(ChangeProvider.newBuilder()
                                .setSource(createActionEntity(2, EntityType.STORAGE_TIER))
                                .setDestination(createActionEntity(3, EntityType.STORAGE_TIER))))
                .build();
        SearchServiceBlockingStub searchServiceRpc = PowerMockito.mock(SearchServiceBlockingStub.class);
        TopologyToSdkEntityConverter topologyToSdkEntityConverter = Mockito.mock(TopologyToSdkEntityConverter.class);
        ActionDataManager actionDataManager = new ActionDataManager(searchServiceRpc, topologyToSdkEntityConverter);
        TopologyEntityDTO vmEntity = TopologyEntityDTO.newBuilder()
                .setOid(1L).setDisplayName(VM_DISPLAYNAME)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .putEntityPropertyMap(SDKConstants.RESOURCE_GROUP_NAME, RESOURCEGROUP_NAME)
                .build();
        PowerMockito.mockStatic(SpecSearchUtil.class);
        PowerMockito.when(SpecSearchUtil.searchTopologyEntityDTOs(any(), any())).thenReturn(Lists.newArrayList(vmEntity));
        List<ContextData> scaleVolumeContextData = actionDataManager.getContextData(scaleActionInfo);
        assertEquals(2, scaleVolumeContextData.size());
        // Test that context data contains VM name.
        ContextData vmNameContextData = scaleVolumeContextData.stream()
                .filter(c -> SDKConstants.VM_NAME.equals(c.getContextKey())).findAny().orElse(null);
        assertNotNull(vmNameContextData);
        assertEquals(VM_DISPLAYNAME, vmNameContextData.getContextValue());
        // Test that context data contains VM resourceGroup name.
        ContextData vmRGNameContextData = scaleVolumeContextData.stream()
                .filter(c -> SDKConstants.RESOURCE_GROUP_NAME.equals(c.getContextKey())).findAny().orElse(null);
        assertNotNull(vmRGNameContextData);
        assertEquals(RESOURCEGROUP_NAME, vmRGNameContextData.getContextValue());
    }
}
