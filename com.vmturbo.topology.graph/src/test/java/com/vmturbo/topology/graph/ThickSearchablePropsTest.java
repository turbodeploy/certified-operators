package com.vmturbo.topology.graph;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.OSImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.AnalysisSettingsImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.BusinessAccountInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.BusinessAccountInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.CronJobInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.CustomControllerInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.DatabaseInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.DatabaseInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.PhysicalMachineInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.PhysicalMachineInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.StorageInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.StorageInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.VirtualMachineInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.VirtualMachineInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.VirtualVolumeInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.VirtualVolumeInfoView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.WorkloadControllerInfoImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TypeSpecificInfoImpl.WorkloadControllerInfoView;
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
        PhysicalMachineInfoView pmInfo = new PhysicalMachineInfoImpl()
                .setNumCpus(1)
                .setCpuModel("foo")
                .setVendor("vendor")
                .setModel("some model")
                .setTimezone("DMZ");

        final PmProps pm = (PmProps)ThickSearchableProps.newProps(new TopologyEntityImpl()
            .setEntityType(ApiEntityType.PHYSICAL_MACHINE.typeNumber())
            .setTypeSpecificInfo(new TypeSpecificInfoImpl().setPhysicalMachine(pmInfo)));
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
        StorageInfoView storageInfo = new StorageInfoImpl()
                .setIsLocal(true);
        final StorageProps storage = (StorageProps)ThickSearchableProps.newProps(new TopologyEntityImpl()
                .setEntityType(ApiEntityType.STORAGE.typeNumber())
                .setTypeSpecificInfo(new TypeSpecificInfoImpl().setStorage(storageInfo)));
        assertThat(storage.isLocal(), is(storageInfo.getIsLocal()));
    }

    /**
     * Volume props.
     */
    @Test
    public void testVolume() {
        VirtualVolumeInfoView volumeInfo = new VirtualVolumeInfoImpl()
                .setAttachmentState(AttachmentState.ATTACHED)
                .setEncryption(true)
                .setIsEphemeral(true);
        final VolumeProps volume = (VolumeProps)ThickSearchableProps.newProps(new TopologyEntityImpl()
                .setEntityType(ApiEntityType.VIRTUAL_VOLUME.typeNumber())
                .setAnalysisSettings(new AnalysisSettingsImpl()
                        .setDeletable(true))
                .setTypeSpecificInfo(new TypeSpecificInfoImpl()
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
        VirtualMachineInfoView vmInfo = new VirtualMachineInfoImpl()
                .addConnectedNetworks("foo")
                .addConnectedNetworks("bar")
                .setGuestOsInfo(new OSImpl()
                        .setGuestOsName("Losedows"))
                .setNumCpus(123);
        final VmProps vm = (VmProps)ThickSearchableProps.newProps(new TopologyEntityImpl()
                .setEntityType(ApiEntityType.VIRTUAL_MACHINE.typeNumber())
                .setTypeSpecificInfo(new TypeSpecificInfoImpl().setVirtualMachine(vmInfo)));
        assertThat(vm.getConnectedNetworkNames(), is(vmInfo.getConnectedNetworksList()));
        assertThat(vm.getNumCpus(), is(vmInfo.getNumCpus()));
        assertThat(vm.getGuestOsName(), is(vmInfo.getGuestOsInfo().getGuestOsName()));
    }

    /**
     * Workload controller.
     */
    @Test
    public void testWorkloadController() {
        WorkloadControllerInfoView customControllerInfo = new WorkloadControllerInfoImpl()
                .setCustomControllerInfo(new CustomControllerInfoImpl()
                    .setCustomControllerType("foo"));
        final WorkloadControllerProps controller = (WorkloadControllerProps)ThickSearchableProps.newProps(new TopologyEntityImpl()
                .setEntityType(ApiEntityType.WORKLOAD_CONTROLLER.typeNumber())
                .setTypeSpecificInfo(new TypeSpecificInfoImpl().setWorkloadController(customControllerInfo)));
        assertThat(controller.getControllerType(), is("foo"));
        assertThat(controller.isCustom(), is(true));

        WorkloadControllerInfoView regularControllerInfo = new WorkloadControllerInfoImpl()
                .setCronJobInfo(new CronJobInfoImpl());
        final WorkloadControllerProps regularController = (WorkloadControllerProps)ThickSearchableProps.newProps(new TopologyEntityImpl()
                .setEntityType(ApiEntityType.WORKLOAD_CONTROLLER.typeNumber())
                .setTypeSpecificInfo(new TypeSpecificInfoImpl().setWorkloadController(regularControllerInfo)));
        assertThat(regularController.getControllerType(), is("CRON_JOB_INFO"));
        assertThat(regularController.isCustom(), is(false));
    }

    /**
     * BA props.
     */
    @Test
    public void testBusinessAccount() {
        BusinessAccountInfoView baInfo = new BusinessAccountInfoImpl()
                .setAssociatedTargetId(123)
                .setAccountId("foo");
        final BusinessAccountProps baProps = (BusinessAccountProps)ThickSearchableProps.newProps(new TopologyEntityImpl()
                .setEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
                .setTypeSpecificInfo(new TypeSpecificInfoImpl().setBusinessAccount(baInfo)));
        assertThat(baProps.getAccountId(), is(baInfo.getAccountId()));
        assertThat(baProps.hasAssociatedTargetId(), is(true));

    }

    /**
     * DB server props.
     */
    @Test
    public void testDBServer() {
        DatabaseInfoView dbInfo = new DatabaseInfoImpl()
                .setEngine(DatabaseEngine.MARIADB)
                .setEdition(DatabaseEdition.EXPRESS)
                .setVersion("1.0.1");
        final DatabaseServerProps dbProps = (DatabaseServerProps)ThickSearchableProps.newProps(new TopologyEntityImpl()
                .setEntityType(ApiEntityType.DATABASE_SERVER.typeNumber())
                .setTypeSpecificInfo(new TypeSpecificInfoImpl().setDatabase(dbInfo)));
        assertThat(dbProps.getDatabaseEngine(), is(dbInfo.getEngine()));
        assertThat(dbProps.getDatabaseEdition(), is(dbInfo.getEdition().name()));
        assertThat(dbProps.getDatabaseVersion(), is(dbInfo.getVersion()));

    }

    /**
     * DB server Edition when only Raw Edition is specified.
     */
    @Test
    public void testDBServerRawEdition() {
        DatabaseInfoView dbInfo = new DatabaseInfoImpl()
                .setEngine(DatabaseEngine.MARIADB)
                .setRawEdition("Express")
                .setVersion("1.0.1");
        final DatabaseServerProps dbProps = (DatabaseServerProps)ThickSearchableProps.newProps(new TopologyEntityImpl()
                .setEntityType(ApiEntityType.DATABASE_SERVER.typeNumber())
                .setTypeSpecificInfo(new TypeSpecificInfoImpl().setDatabase(dbInfo)));
        assertThat(dbProps.getDatabaseEngine(), is(dbInfo.getEngine()));
        assertThat(dbProps.getDatabaseEdition(), is(dbInfo.getRawEdition()));
        assertThat(dbProps.getDatabaseVersion(), is(dbInfo.getVersion()));
    }

    /**
     * Other props.
     */
    @Test
    public void testOther() {
        final SearchableProps props = ThickSearchableProps.newProps(new TopologyEntityImpl()
                .addCommoditySoldList(new CommoditySoldImpl()
                        .setCommodityType(new CommodityTypeImpl()
                            .setType(1))
                        .setCapacity(123))
                .setEntityType(ApiEntityType.APPLICATION.typeNumber()));
        assertThat(props.getCommodityCapacity(1), is(123.0f));
    }

}