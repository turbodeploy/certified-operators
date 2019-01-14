package com.vmturbo.market.topology.conversions;

import static com.vmturbo.market.topology.conversions.CostDTOCreator.OSTypeMapping;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.junit.Test;

import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

public class CostDTOCreatorTest {

    /**
     * This test ensures that all the values in Enum OSType have a mapping in
     * CostDTOCreator::OSTypeMapping
     */
    @Test
    public void testOSTypeMappings() {
        List<OSType> osWithoutMapping = new ArrayList<>();
        Map<OSType, String> inversedOSTypeMapping = OSTypeMapping.entrySet().stream().collect(
                Collectors.toMap(Entry::getValue, Entry::getKey));
        for (OSType os : OSType.values()) {
            if (!inversedOSTypeMapping.containsKey(os)) {
                osWithoutMapping.add(os);
            }
        }
        String error = "Operating systems " + osWithoutMapping.stream().map(os -> os.toString())
                .collect(Collectors.joining(", ")) + " do not have mapping in " +
                "CostDTOCreator::OSTypeMapping.";
        assertTrue(error, osWithoutMapping.isEmpty());
    }
}
