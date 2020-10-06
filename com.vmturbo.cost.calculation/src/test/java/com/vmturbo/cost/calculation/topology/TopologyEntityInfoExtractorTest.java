package com.vmturbo.cost.calculation.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.IpAddress;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DatabaseInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.commons.Units;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeTierConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.DatabaseConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.NetworkConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.VirtualVolumeConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class TopologyEntityInfoExtractorTest {

    private final TopologyEntityInfoExtractor entityInfoExtractor =
            new TopologyEntityInfoExtractor();

    private static final String VM_IP = "1.1.1.1";
    private static final float STORAGE_ACCESS_CAP = 5;
    private static final float STORAGE_AMOUNT_CAP = 6;
    private static final float IO_THROUGHPUT_CAP_KB = 8192;
    private static final int COMPUTE_NUM_OF_COUPONS = 7;
    private static final double DELTA = 1e-10;
    private static final long DEFAULT_ID = 0;
    private static final String DEFAULT_DB_NAME = "testDB";

    private static final TopologyEntityDTO VM = TopologyEntityDTO.newBuilder()
            .setOid(DEFAULT_ID)
            .setDisplayName("foo")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .addIpAddresses(IpAddress.newBuilder()
                                .setIpAddress(VM_IP)
                                .setIsElastic(true)
                                .build())
                            .setGuestOsInfo(OS.newBuilder()
                                    .setGuestOsType(OSType.LINUX)
                                    .setGuestOsName(OSType.LINUX.name()))
                            .setTenancy(Tenancy.DEFAULT)))
            .build();

    private static final TopologyEntityDTO AHUB_VM = TopologyEntityDTO.newBuilder()
            .setOid(DEFAULT_ID)
            .setDisplayName("foo")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .addIpAddresses(IpAddress.newBuilder()
                                    .setIpAddress(VM_IP)
                                    .setIsElastic(true)
                                    .build())
                            .setLicenseModel(EntityDTO.LicenseModel.AHUB)
                            .setGuestOsInfo(OS.newBuilder()
                                    .setGuestOsType(OSType.WINDOWS)
                                    .setGuestOsName(OSType.WINDOWS.name()))
                            .setTenancy(Tenancy.DEFAULT)))
            .build();

    private static final TopologyEntityDTO VIRTUAL_VOLUME = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
        .setOid(DEFAULT_ID)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setVirtualVolume(VirtualVolumeInfo.getDefaultInstance()))
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber()))
            .setCapacity(STORAGE_AMOUNT_CAP))
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.STORAGE_ACCESS.getNumber()))
            .setCapacity(STORAGE_ACCESS_CAP))
        .addCommoditySoldList(CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                    .setType(CommodityDTO.CommodityType.IO_THROUGHPUT.getNumber()))
            .setCapacity(IO_THROUGHPUT_CAP_KB))
        .build();

    private static final TopologyEntityDTO EMPTY_VIRTUAL_VOLUME = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.VIRTUAL_VOLUME_VALUE)
        .setOid(DEFAULT_ID)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder())
        .build();

    private static final TopologyEntityDTO COMPUTE_TIER = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(DEFAULT_ID)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
            .setComputeTier(ComputeTierInfo.newBuilder()
                .setNumCoupons(COMPUTE_NUM_OF_COUPONS).build()))
        .build();

    private static final TopologyEntityDTO DB = TopologyEntityDTO.newBuilder()
            .setOid(DEFAULT_ID)
            .setDisplayName(DEFAULT_DB_NAME)
            .setEntityType(EntityType.DATABASE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setDatabase(DatabaseInfo.newBuilder()
                            .setEngine(DatabaseEngine.SQLSERVER)
                            .setEdition(DatabaseEdition.STANDARD)
                            .setLicenseModel(LicenseModel.LICENSE_INCLUDED)
                            .setDeploymentType(DeploymentType.SINGLE_AZ)))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(CommodityType.newBuilder()
                            .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT.getNumber()))
                    .setCapacity(STORAGE_AMOUNT_CAP))
            .build();

    private static final TopologyEntityDTO EMPTY_DB = TopologyEntityDTO.newBuilder()
        .setOid(DEFAULT_ID)
        .setDisplayName(DEFAULT_DB_NAME)
        .setEntityType(EntityType.DATABASE_VALUE)
        .build();

    @Test
    public void testExtractId() {
        assertThat(entityInfoExtractor.getId(VM), is(VM.getOid()));
    }

    @Test
    public void testExtractType() {
        assertThat(entityInfoExtractor.getEntityType(VM), is(VM.getEntityType()));
    }

    @Test
    public void testExtractName() {
        assertThat(entityInfoExtractor.getName(DB), is(DB.getDisplayName()));
    }

    /**
     * Test getting entity state.
     */
    @Test
    public void testExtractState() {
        assertThat(entityInfoExtractor.getEntityState(DB), is(DB.getEntityState()));
    }

    @Test
    public void testExtractVmComputeConfig() {
        Optional<ComputeConfig> computeConfigOptional = entityInfoExtractor.getComputeConfig(VM);
        assertTrue(computeConfigOptional.isPresent());
        final ComputeConfig config = computeConfigOptional.get();
        assertThat(config.getOs(), is(OSType.LINUX));
        assertThat(config.getTenancy(), is(Tenancy.DEFAULT));
        assertThat(config.getLicenseModel(), is(EntityDTO.LicenseModel.LICENSE_INCLUDED));
    }

    @Test
    public void testExtractAhubVmComputeConfig() {
        final Optional<ComputeConfig> configOptional = entityInfoExtractor.getComputeConfig(AHUB_VM);
        assertTrue(configOptional.isPresent());
        final ComputeConfig config = configOptional.get();
        assertThat(config.getOs(), is(OSType.WINDOWS));
        assertThat(config.getTenancy(), is(Tenancy.DEFAULT));
        assertThat(config.getLicenseModel(), is(EntityDTO.LicenseModel.AHUB));
    }

    @Test
    public void testExtractComputeConfigNonVM() {
        Optional<ComputeConfig> computeConfigOptional = entityInfoExtractor.getComputeConfig(
                TopologyEntityDTO.newBuilder()
                    .setOid(1L)
                    .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
                    .setDisplayName("bar")
                    .build());
        assertFalse(computeConfigOptional.isPresent());
    }

    @Test
    public void testExtractComputeConfigNoTypeSpecificInfo() {
        Optional<ComputeConfig> computeConfigOptional = entityInfoExtractor.getComputeConfig(
                TopologyEntityDTO.newBuilder()
                        .setOid(1L)
                        .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .setDisplayName("bar")
                        .build());
        assertFalse(computeConfigOptional.isPresent());
    }

    @Test
    public void testExtractDatabaseConfig() {
        Optional<DatabaseConfig> dbConfigOptional = entityInfoExtractor.getDatabaseConfig(DB);
        assertTrue(dbConfigOptional.isPresent());
        final DatabaseConfig dbConfig = dbConfigOptional.orElse(null);
        assertNotNull(dbConfig);
        assertThat(dbConfig.getEngine(), is(DatabaseEngine.SQLSERVER));
        assertThat(dbConfig.getEdition(), is(DatabaseEdition.STANDARD));
        assertThat(dbConfig.getLicenseModel().get(), is(LicenseModel.LICENSE_INCLUDED));
        assertThat(dbConfig.getDeploymentType().get(), is(DeploymentType.SINGLE_AZ));
    }

    @Test
    public void testExtractDatabaseStorageAmount() {
        Optional<Float> dbStorageAmount = entityInfoExtractor.getDBStorageCapacity(DB);
        assertTrue(dbStorageAmount.isPresent());
        assertThat(dbStorageAmount.get(), is(STORAGE_AMOUNT_CAP));
    }

    @Test
    public void testEmptyGetDatabaseConfig() {
        final Optional<DatabaseConfig> databaseConfig = entityInfoExtractor.getDatabaseConfig(EMPTY_DB);
        assertFalse(databaseConfig.isPresent());
    }

    @Test
    public void testGetNetworkConfig() {
        Optional<NetworkConfig> netConfig = entityInfoExtractor.getNetworkConfig(VM);
        assertTrue(netConfig.isPresent());
        assertEquals(VM_IP, netConfig.get().getIPAddresses().get(0).getIpAddress());
        assertEquals(1, netConfig.get().getNumElasticIps());
    }

    @Test
    public void testGetVolumeConfig() {
        final Optional<VirtualVolumeConfig> volumeConfig = entityInfoExtractor.getVolumeConfig(VIRTUAL_VOLUME);
        assertTrue(volumeConfig.isPresent());
        assertEquals(STORAGE_ACCESS_CAP, volumeConfig.get().getAccessCapacityMillionIops(), DELTA);
        assertEquals(STORAGE_AMOUNT_CAP / 1024, volumeConfig.get().getAmountCapacityGb(), DELTA);
        assertEquals(IO_THROUGHPUT_CAP_KB / Units.KBYTE, volumeConfig.get().getIoThroughputCapacityMBps(), DELTA);
    }

    @Test
    public void testEmptyGetVolumeConfig() {
        final Optional<VirtualVolumeConfig> volumeConfig = entityInfoExtractor.getVolumeConfig(EMPTY_VIRTUAL_VOLUME);
        assertFalse(volumeConfig.isPresent());
    }

    @Test
    public void testGetComputeTierConfig() {
        final Optional<ComputeTierConfig> computeTierConfig = entityInfoExtractor.getComputeTierConfig(COMPUTE_TIER);
        assertTrue(computeTierConfig.isPresent());
        assertEquals(COMPUTE_NUM_OF_COUPONS, computeTierConfig.get().getNumCoupons());
    }

    @Test
    public void testGetEmptyConfig() {
        Optional<ComputeTierConfig> computeTierConfig = entityInfoExtractor.getComputeTierConfig(VIRTUAL_VOLUME);
        assertFalse(computeTierConfig.isPresent());
        Optional<VirtualVolumeConfig> volumeConfig = entityInfoExtractor.getVolumeConfig(COMPUTE_TIER);
        assertFalse(volumeConfig.isPresent());
        Optional<NetworkConfig> networkConfig = entityInfoExtractor.getNetworkConfig(COMPUTE_TIER);
        assertFalse(networkConfig.isPresent());
        Optional<DatabaseConfig> databaseConfig = entityInfoExtractor.getDatabaseConfig(COMPUTE_TIER);
        assertFalse(databaseConfig.isPresent());
    }

}
