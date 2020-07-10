package com.vmturbo.cloud.commitment.analysis.persistence;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationDatapoint;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierAllocationStore;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.group.api.ImmutableGroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Class for testing the CloudCommitmentDemandWriter.
 */
public class CloudCommitmentDemandWriterTest {

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(12345L)
            .setTopologyId(333333L)
            .setCreationTime(99999L)
            .build();

    private CloudTopology<TopologyEntityDTO> cloudTopology = Mockito.mock(CloudTopology.class);

    private final OS osInfo = OS.newBuilder().setGuestOsName("Linux").setGuestOsType(OSType.LINUX).build();

    private final VirtualMachineInfo info = VirtualMachineInfo.newBuilder().setGuestOsInfo(osInfo)
            .setTenancy(Tenancy.DEFAULT).setBillingType(VMBillingType.ONDEMAND).build();

    private final VirtualMachineInfo info2 = VirtualMachineInfo.newBuilder().setGuestOsInfo(osInfo)
            .setTenancy(Tenancy.DEFAULT).setBillingType(VMBillingType.BIDDING).build();

    private final TopologyEntityDTO vm1 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setDisplayName("Test1")
            .setEntityState(EntityState.POWERED_ON)
            .setOid(1234L).setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualMachine(info)
                    .build()).build();

    private final TopologyEntityDTO vm2 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setDisplayName("Test2")
            .setEntityState(EntityState.POWERED_OFF)
            .setOid(5678L).setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualMachine(info2)
                    .build()).build();

    private final TopologyEntityDTO region1 = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.REGION_VALUE).setDisplayName("us-east-1").setOid(5454L).build();

    private final TopologyEntityDTO serviceProvider = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.SERVICE_PROVIDER_VALUE).setOid(9999L).build();

    private final TopologyEntityDTO availabilityZone = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.AVAILABILITY_ZONE_VALUE).setDisplayName("us-east-1a").setOid(6666L)
            .build();

    private final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.COMPUTE_TIER_VALUE).setDisplayName("m4-large").setOid(8888L).build();

    private final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE).setOid(7777L).build();

    private final ImmutableGroupAndMembers billingFamily = ImmutableGroupAndMembers.builder()
            .group(Grouping.newBuilder().setId(7778L).build())
            .entities(Collections.emptyList())
            .members(Collections.emptyList())
            .build();

    private final ComputeTierAllocationStore computeTierAllocationStore = Mockito.mock(ComputeTierAllocationStore.class);

    /**
     * Test the construction of allocation data points to be persisted by SqlComputeTierAllocationStore.
     */
    @Test
    public void testBuildComputeTierAllocationDatapoint() {
        long vmOid = vm1.getOid();
        Mockito.when(cloudTopology.getServiceProvider(vmOid)).thenReturn(Optional.of(serviceProvider));

        Mockito.when(cloudTopology.getConnectedRegion(vmOid)).thenReturn(Optional.of(region1));

        Mockito.when(cloudTopology.getConnectedAvailabilityZone(vmOid)).thenReturn(Optional.of(availabilityZone));

        Mockito.when(cloudTopology.getBillingFamilyForEntity(vmOid)).thenReturn(Optional.of(billingFamily));

        Mockito.when(cloudTopology.getComputeTier(vmOid)).thenReturn(Optional.of(computeTier));

        Mockito.when(cloudTopology.getOwner(vmOid)).thenReturn(Optional.of(businessAccount));

        CloudCommitmentDemandWriter commitmentDemandWriter =
                new CloudCommitmentDemandWriterImpl(computeTierAllocationStore, true);

        Mockito.when(cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE)).thenReturn(Arrays.asList(vm1, vm2));

        final ArgumentCaptor<List> listCaptorRecordOn = ArgumentCaptor.forClass(List.class);

        commitmentDemandWriter.writeAllocationDemand(cloudTopology, topologyInfo);

        Mockito.verify(computeTierAllocationStore)
                .persistAllocations(Matchers.any(), listCaptorRecordOn.capture());
        List<ComputeTierAllocationDatapoint> datapoints = listCaptorRecordOn.getValue();

        assert (datapoints.size() == 1);

        ComputeTierAllocationDatapoint datapoint = datapoints.get(0);

        assert (datapoint.entityOid() == 1234L);
        assert (datapoint.accountOid() == 7777L);
        assert (datapoint.regionOid() == 5454L);
        assert (datapoint.availabilityZoneOid().isPresent());
        assert (datapoint.availabilityZoneOid().get() == 6666L);
        assert (datapoint.serviceProviderOid() == 9999L);
        assert (datapoint.cloudTierDemand().cloudTierOid() == 8888L);
        assert (datapoint.cloudTierDemand().osType().equals(OSType.LINUX));
        assert (datapoint.cloudTierDemand().tenancy().equals(Tenancy.DEFAULT));

        // Test when demand recording is turned off
        final ComputeTierAllocationStore computeTierAllocationStore = Mockito.mock(ComputeTierAllocationStore.class);

        CloudCommitmentDemandWriter commitmentDemandWriterRecordOff = new CloudCommitmentDemandWriterImpl(
                computeTierAllocationStore, false);

        commitmentDemandWriterRecordOff.writeAllocationDemand(cloudTopology, topologyInfo);

        Mockito.verifyZeroInteractions(computeTierAllocationStore);
    }
}
