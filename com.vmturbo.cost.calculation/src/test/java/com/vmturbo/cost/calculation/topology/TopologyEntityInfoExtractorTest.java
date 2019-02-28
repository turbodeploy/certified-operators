package com.vmturbo.cost.calculation.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeConfig;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class TopologyEntityInfoExtractorTest {

    private final TopologyEntityInfoExtractor entityInfoExtractor =
            new TopologyEntityInfoExtractor();

    private final TopologyEntityDTO VM = TopologyEntityDTO.newBuilder()
            .setOid(7L)
            .setDisplayName("foo")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder()
                    .setVirtualMachine(VirtualMachineInfo.newBuilder()
                            .setGuestOsInfo(OS.newBuilder()
                                    .setGuestOsType(OSType.LINUX)
                                    .setGuestOsName(OSType.LINUX.name()))
                            .setTenancy(Tenancy.DEFAULT)))
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
    public void testExtractVmComputeConfig() {
        Optional<ComputeConfig> computeConfigOptional = entityInfoExtractor.getComputeConfig(VM);
        final ComputeConfig config = computeConfigOptional.get();
        assertThat(config.getOs(), is(OSType.LINUX));
        assertThat(config.getTenancy(), is(Tenancy.DEFAULT));
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
}
