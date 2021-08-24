package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.component.external.api.HealthChecksTestBase;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO;
import com.vmturbo.api.enums.healthCheck.HealthState;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;

/**
 * Contains tests for {@link HealthDataMapper}.
 */
public class HealthDataMapperTest extends HealthChecksTestBase {

    /**
     * A general test for the aggregateTargetHealthInfoToDTO(...) method.
     */
    @Test
    public void testAggregateTargetHealthInfoToDTO() {
        TargetHealth validationSuccessful = makeHealth(TargetHealthSubCategory.VALIDATION, "normallyValidated");
        TargetHealth validaionFailed = makeHealth(TargetHealthSubCategory.VALIDATION, "validationFailed",
                        ErrorType.CONNECTION_TIMEOUT, "Validation connection timeout.", 1_000_000, 1);
        TargetHealth discoverySuccessful = makeHealth(TargetHealthSubCategory.DISCOVERY, "normallyDiscovered");
        TargetHealth discoveryPending = makeHealth(TargetHealthSubCategory.DISCOVERY, "discoveryPending",
                        "Discovery pending.");

        Map<Long, TargetDetails> targetDetails = new HashMap<>();
        targetDetails.put(0L, createTargetDetails(0L, validationSuccessful, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(1L, createTargetDetails(1L, validaionFailed, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(2L, createTargetDetails(2L, discoverySuccessful, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(3L, createTargetDetails(3L, discoveryPending, new ArrayList<>(), new ArrayList<>(), false));

        List<AggregatedHealthResponseDTO> responseItems = HealthDataMapper
                        .aggregateTargetHealthInfoToDTO(targetDetails, DEFAULT_FAILED_DISCOVERY_COUNT_THRESHOLD);

        Assert.assertEquals(2, responseItems.size());
        for (AggregatedHealthResponseDTO subcategoryResponse : responseItems) {
            if (TargetHealthSubCategory.VALIDATION.name().equals(subcategoryResponse.getSubcategory())) {
                Assert.assertEquals(HealthState.CRITICAL, subcategoryResponse.getHealthState());
            } else if (TargetHealthSubCategory.DISCOVERY.name().equals(subcategoryResponse.getSubcategory())) {
                Assert.assertEquals(HealthState.MINOR, subcategoryResponse.getHealthState());
            }
        }
    }
}
