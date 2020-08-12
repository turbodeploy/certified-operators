package com.vmturbo.topology.graph;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.AnalysisSettings;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessAccountInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CronJobInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.CustomControllerInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.StorageInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.WorkloadControllerInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.topology.graph.SearchableProps.BusinessAccountProps;
import com.vmturbo.topology.graph.SearchableProps.DatabaseServerProps;
import com.vmturbo.topology.graph.SearchableProps.PmProps;
import com.vmturbo.topology.graph.SearchableProps.StorageProps;
import com.vmturbo.topology.graph.SearchableProps.VmProps;
import com.vmturbo.topology.graph.SearchableProps.VolumeProps;
import com.vmturbo.topology.graph.SearchableProps.WorkloadControllerProps;

/**
 * Unit tests for {@link ThickSearchableProps}.
 */
public class ThickSearchablePropsTest {

    /**
     * Physical machine props.
     */
    @Test
    public void testPm() {
        PhysicalMachineInfo pmInfo = PhysicalMachineInfo.newBuilder()
                .setNumCpus(1)
                .setCpuModel("foo")
                .setVendor("vendor")
                .setModel("some model")
                .setTimezone("DMZ")
                .build();

        final PmProps pm = (PmProps)ThickSearchableProps.newProps(TopologyEntityDTO.newBuilder()
            .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setPhysicalMachine(pmInfo)));
        assertThat(pm.getCpuModel(), is(pmInfo.getCpuModel()));
        assertThat(pm.getNumCpus(), is(pmInfo.getNumCpus()));
        assertThat(pm.getVendor(), is(pmInfo.getVendor()));
        assertThat(pm.getModel(), is(pmInfo.getModel()));
        assertThat(pm.getTimezone(), is(pmInfo.getTimezone()));
    }

    /**
     * Storage props.
     */
    @Test
    public void testStorage() {
        StorageInfo storageInfo = StorageInfo.newBuilder()
                .setIsLocal(true)
                .build();
        final StorageProps storage = (StorageProps)ThickSearchableProps.newProps(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.STORAGE.typeNumber())
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setStorage(storageInfo)));
        assertThat(storage.isLocal(), is(storageInfo.getIsLocal()));
    }

    /**
     * Volume props.
     */
    @Test
    public void testVolume() {
        VirtualVolumeInfo volumeInfo = VirtualVolumeInfo.newBuilder()
                .setAttachmentState(AttachmentState.ATTACHED)
                .setEncryption(true)
                .setIsEphemeral(true)
                .build();
        final VolumeProps volume = (VolumeProps)ThickSearchableProps.newProps(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_VOLUME.typeNumber())
                .setAnalysisSettings(AnalysisSettings.newBuilder()
                        .setDeletable(true))
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                        .setVirtualVolume(volumeInfo)));
        assertThat(volume.attachmentState(), is(volumeInfo.getAttachmentState()));
        assertThat(volume.isEncrypted(), is(volumeInfo.getEncryption()));
        assertThat(volume.isEphemeral(), is(volumeInfo.getIsEphemeral()));
        assertThat(volume.isDeletable(), is(true));
    }

    /**
     * VM props.
     */
    @Test
    public void testVm() {
        VirtualMachineInfo vmInfo = VirtualMachineInfo.newBuilder()
                .addConnectedNetworks("foo")
                .addConnectedNetworks("bar")
                .setGuestOsInfo(OS.newBuilder()
                        .setGuestOsName("Losedows"))
                .setNumCpus(123)
                .build();
        final VmProps vm = (VmProps)ThickSearchableProps.newProps(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setVirtualMachine(vmInfo)));
        assertThat(vm.getConnectedNetworkNames(), is(vmInfo.getConnectedNetworksList()));
        assertThat(vm.getNumCpus(), is(vmInfo.getNumCpus()));
        assertThat(vm.getGuestOsName(), is(vmInfo.getGuestOsInfo().getGuestOsName()));
    }

    /**
     * Workload controller.
     */
    @Test
    public void testWorkloadController() {
        WorkloadControllerInfo customControllerInfo = WorkloadControllerInfo.newBuilder()
                .setCustomControllerInfo(CustomControllerInfo.newBuilder()
                    .setCustomControllerType("foo"))
                .build();
        final WorkloadControllerProps controller = (WorkloadControllerProps)ThickSearchableProps.newProps(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.WORKLOAD_CONTROLLER.typeNumber())
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setWorkloadController(customControllerInfo)));
        assertThat(controller.getControllerType(), is("foo"));
        assertThat(controller.isCustom(), is(true));

        WorkloadControllerInfo regularControllerInfo = WorkloadControllerInfo.newBuilder()
                .setCronJobInfo(CronJobInfo.getDefaultInstance())
                .build();
        final WorkloadControllerProps regularController = (WorkloadControllerProps)ThickSearchableProps.newProps(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.WORKLOAD_CONTROLLER.typeNumber())
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setWorkloadController(regularControllerInfo)));
        assertThat(regularController.getControllerType(), is("CRON_JOB_INFO"));
        assertThat(regularController.isCustom(), is(false));
    }

    /**
     * BA props.
     */
    @Test
    public void testBusinessAccount() {
        BusinessAccountInfo baInfo = BusinessAccountInfo.newBuilder()
                .setAssociatedTargetId(123)
                .setAccountId("foo")
                .build();
        final BusinessAccountProps baProps = (BusinessAccountProps)ThickSearchableProps.newProps(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setBusinessAccount(baInfo)));
        assertThat(baProps.getAccountId(), is(baInfo.getAccountId()));
        assertThat(baProps.hasAssociatedTargetId(), is(true));

    }

    /**
     * DB server props.
     */
    @Test
    public void testDBServer() {
        DatabaseInfo dbInfo = DatabaseInfo.newBuilder()
                .setEngine(DatabaseEngine.MARIADB)
                .setEdition(DatabaseEdition.EXPRESS)
                .setVersion("1.0.1")
                .build();
        final DatabaseServerProps dbProps = (DatabaseServerProps)ThickSearchableProps.newProps(TopologyEntityDTO.newBuilder()
                .setEntityType(ApiEntityType.DATABASE_SERVER.typeNumber())
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setDatabase(dbInfo)));
        assertThat(dbProps.getDatabaseEngine(), is(dbInfo.getEngine()));
        assertThat(dbProps.getDatabaseEdition(), is(dbInfo.getEdition()));
        assertThat(dbProps.getDatabaseVersion(), is(dbInfo.getVersion()));

    }

    /**
     * Other props.
     */
    @Test
    public void testOther() {
        final SearchableProps props = ThickSearchableProps.newProps(TopologyEntityDTO.newBuilder()
                .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                        .setCommodityType(CommodityType.newBuilder()
                            .setType(1))
                        .setCapacity(123)
                        .build())
                .setEntityType(ApiEntityType.APPLICATION.typeNumber()));
        assertThat(props.getCommodityCapacity(1), is(123.0f));
    }

}