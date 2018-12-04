package com.vmturbo.repository.dto;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.PhysicalMachineInfo;

public class PhysicalMachineInfoRepoDTOTest {

    public static final String CPU_MODEL_VALUE = "cpu_model";
    public static final String VENDOR_VALUE = "vendor";
    public static final String MODEL_VALUE = "model";

    /**
     * Test filling a RepoDTO from a TypeSpecificInfo with data fields populated.
     */
    @Test
    public void testFillFromTypeSpecificInfo() {
        // arrange
        final TypeSpecificInfo testInfo = TypeSpecificInfo.newBuilder()
            .setPhysicalMachine(
                PhysicalMachineInfo.newBuilder()
                    .setCpuModel(CPU_MODEL_VALUE)
                    .setVendor(VENDOR_VALUE)
                    .setModel(MODEL_VALUE)
                .build())
            .build();
        final ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        final PhysicalMachineInfoRepoDTO testPhysicalMachineRepoDTO = new PhysicalMachineInfoRepoDTO();
        // act
        testPhysicalMachineRepoDTO.fillFromTypeSpecificInfo(testInfo, serviceEntityRepoDTO);
        // assert
        assertEquals(CPU_MODEL_VALUE, testPhysicalMachineRepoDTO.getCpuModel());
        assertEquals(MODEL_VALUE, testPhysicalMachineRepoDTO.getModel());
        assertEquals(VENDOR_VALUE, testPhysicalMachineRepoDTO.getVendor());
    }

    /**
     * Test filling a RepoDTO from an empty TypeSpecificInfo.
     */
    @Test
    public void testFillFromEmptyTypeSpecificInfo() {
        // arrange
        TypeSpecificInfo testInfo = TypeSpecificInfo.newBuilder()
            .build();
        ServiceEntityRepoDTO serviceEntityRepoDTO = new ServiceEntityRepoDTO();
        final PhysicalMachineInfoRepoDTO testPhysicalMachineRepoDTO = new PhysicalMachineInfoRepoDTO();
        // act
        testPhysicalMachineRepoDTO.fillFromTypeSpecificInfo(testInfo, serviceEntityRepoDTO);
        // assert
        assertNull(testPhysicalMachineRepoDTO.getCpuModel());
        assertNull(testPhysicalMachineRepoDTO.getModel());
        assertNull(testPhysicalMachineRepoDTO.getVendor());
    }

    /**
     * Test extracting a TypeSpecificInfo from a RepoDTO.
     */
    @Test
    public void testCreateFromRepoDTO() {
        // arrange
        PhysicalMachineInfoRepoDTO testDto = new PhysicalMachineInfoRepoDTO();
        testDto.setCpuModel(CPU_MODEL_VALUE);
        testDto.setVendor(VENDOR_VALUE);
        testDto.setModel(MODEL_VALUE);
        PhysicalMachineInfo expected = PhysicalMachineInfo.newBuilder()
            .setCpuModel(CPU_MODEL_VALUE)
            .setVendor(VENDOR_VALUE)
            .setModel(MODEL_VALUE)
            .build();
        // act
        TypeSpecificInfo result = testDto.createTypeSpecificInfo();
        // assert
        assertTrue(result.hasPhysicalMachine());
        final PhysicalMachineInfo physicalMachineInfo = result.getPhysicalMachine();
        assertThat(physicalMachineInfo, equalTo(expected));
    }

    /**
     * Test extracting a TypeSpecificInfo from an empty RepoDTO.
     */
    @Test
    public void testCreateFromEmptyRepoDTO() {
        // arrange
        PhysicalMachineInfoRepoDTO testDto = new PhysicalMachineInfoRepoDTO();
        PhysicalMachineInfo expected = PhysicalMachineInfo.newBuilder()
            .build();
        // act
        TypeSpecificInfo result = testDto.createTypeSpecificInfo();
        // assert
        assertTrue(result.hasPhysicalMachine());
        final PhysicalMachineInfo physicalMachineInfo = result.getPhysicalMachine();
        assertEquals(expected, physicalMachineInfo);
    }
}