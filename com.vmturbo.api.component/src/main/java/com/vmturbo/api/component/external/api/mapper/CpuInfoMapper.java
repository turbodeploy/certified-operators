package com.vmturbo.api.component.external.api.mapper;


import com.vmturbo.api.dto.template.CpuModelApiDTO;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListResponse;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacity.CpuModelListResponse.CPUInfo;

/**
 * Map from internal CpuInfo structure to the external REST API structure {@link CpuModelApiDTO}.
 * The reverse mapping is not required, as this structure is never received as input from the
 * REST API caller.
 **/
public class CpuInfoMapper {
    /**
     *  Map from internal CpuInfo structure to the external REST API structure {@link CpuModelApiDTO}.
     *  The reverse mapping is not required, as this structure is never received as input from the
     *  REST API caller.
     *
     * @param cpuInfo an internal {@link CPUInfo} protobuf to be converted
     * @return a new {@link CpuModelApiDTO} constructed from the given CPUInfo
     */
    public CpuModelApiDTO convertCpuDTO (CpuModelListResponse.CPUInfo cpuInfo) {
        CpuModelApiDTO cpuModelApiDTO = new CpuModelApiDTO();
        cpuModelApiDTO.setModelName(cpuInfo.getCpuModelName());
        cpuModelApiDTO.setNumCores(cpuInfo.getCores());
        cpuModelApiDTO.setSpeed(cpuInfo.getMhz());
        cpuModelApiDTO.setScalingFactor(cpuInfo.getScalingFactor());
        return cpuModelApiDTO;
    }
}
