package com.vmturbo.components.test.utilities.utils;

import static com.vmturbo.components.test.utilities.utils.TopologyUtils.generateStressAccount;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.vmturbo.external.api.model.InputFieldApiDTO;
import com.vmturbo.external.api.model.TargetApiDTO;


/**
 * Utilities for working with StressProbe.
 **/
public class StressProbeUtils {

    private static final String STRESS_PROBE_CATEGORY = "HYPERVISOR";
    private static final String STRESS_PROBE_TYPE = "Stress-SDK";

    /**
     * Create a TargetApiDTO to use for creating a StressProbe target.
     *
     * This stress probe will be parameterized in the number of VMs desired, and calculate the
     * number of other components in the topology using {@link TopologyUtils}.generateStressAccount();
     * @param topologySize the number of SE's to instantiate in this stress-probe topology; the other
     *                    SE's will be sized accordingly
     * @return a new TargetApiDTO with the properties for a StressProbe target with the given
     * number of VMs, etc.
     */
    public static TargetApiDTO createTargetRequest(int topologySize) {
        final TargetApiDTO newTargetRequest = new TargetApiDTO();
        newTargetRequest.setCategory(STRESS_PROBE_CATEGORY);
        newTargetRequest.setType(STRESS_PROBE_TYPE);

        Map<String, Object> accountFields = generateStressAccount(topologySize).getFieldMap();
        List<InputFieldApiDTO> inputFields = accountFields.entrySet().stream()
                .map(fieldNameValue ->  {
                    final InputFieldApiDTO fieldDTO = new InputFieldApiDTO();
                    fieldDTO.setName(fieldNameValue.getKey());
                    fieldDTO.setValue(fieldNameValue.getValue().toString());
                    return fieldDTO;
                })
                .collect(Collectors.toList());

        newTargetRequest.setInputFields(inputFields);
        return newTargetRequest;
    }
}
