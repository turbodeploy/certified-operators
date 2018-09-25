package com.vmturbo.api.component.external.api.mapper;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

import org.junit.Test;

import com.google.common.math.DoubleMath;
import com.turbonomic.cpucapacity.CPUInfo;

import com.vmturbo.api.dto.template.CpuModelApiDTO;

/**
 * Test for mapping between internal CPUInfo protobuf and external CpuModelApiDTO.
 **/
public class CPUInfoMapperTest {

    public static final String CPU_MODEL_1 = "model1";
    public static final Integer CORES_1 = 4;
    public static final Integer SPEED_1 = 3000;
    public static final double SCALING_FACTOR_1 = 1.0;
    public static final String CPU_MODEL_2 = "model2";
    public static final Integer CORES_2 = 8;
    public static final Integer SPEED_2 = 4000;
    public static final double SCALING_FACTOR_2 = 1.6;
    public static final double TOLERANCE = 0.0001;

    /**
     * Test that the mapper converts a lCPUInfo object into the corresponding CpuModelApiDTO object.
     */
    @Test
    public void testCpuInfoMapper() {
        // Arrange
        CpuInfoMapper mapper = new CpuInfoMapper();
        CPUInfo cpuInfo1 = new CPUInfo(CPU_MODEL_1, CORES_1, SPEED_1, SCALING_FACTOR_1);
        CPUInfo cpuInfo2 = new CPUInfo(CPU_MODEL_2, CORES_2, SPEED_2, SCALING_FACTOR_2);
        // Act
        CpuModelApiDTO cpuModelDto1 = mapper.convertCpuDTO(cpuInfo1);
        CpuModelApiDTO cpuModelDto2 = mapper.convertCpuDTO(cpuInfo2);
        // Assert
        checkCpuInfoDTOFields(cpuModelDto1, CPU_MODEL_1, CORES_1, SPEED_1, SCALING_FACTOR_1);
        checkCpuInfoDTOFields(cpuModelDto2, CPU_MODEL_2, CORES_2, SPEED_2, SCALING_FACTOR_2);
    }

    /**
     * Compare the individual fields of the CpuModelApiDTO with given CPUInfo object.
     *
     * @param cpuModelApiDTO output from the conversion to be tested
     * @param cpuModel the exptected cpu model name
     * @param cores the expected number of cores in the cpu
     * @param speed the expected CPU speed in mhz
     * @param scalingFactor the expected CPU speed scaling factor from baseline
     */
    private void checkCpuInfoDTOFields(CpuModelApiDTO cpuModelApiDTO, String cpuModel,
                                       Integer cores, Integer speed, double scalingFactor) {
        assertEquals("modleName mismatch", cpuModel, cpuModelApiDTO.getModelName());
        assertEquals("numCores mismatch", cores, cpuModelApiDTO.getNumCores());
        assertEquals("speed mismatch", speed, cpuModelApiDTO.getSpeed());
        assertTrue("scalingFactor mismatch", DoubleMath.fuzzyEquals(cpuModelApiDTO.getScalingFactor(),
                scalingFactor, TOLERANCE));
    }
}
