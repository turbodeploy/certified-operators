package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.component.external.api.HealthChecksTestBase;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO;
import com.vmturbo.api.enums.healthCheck.HealthState;
import com.vmturbo.api.enums.healthCheck.TargetCheckSubcategory;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.ConnectionTimeOutErrorType;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.DelayedDataErrorType;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.DuplicationErrorType;

/**
 * Contains tests for {@link HealthDataMapper}.
 */
public class HealthDataMapperTest extends HealthChecksTestBase {

     private static final ErrorTypeInfo connectionErrorTypeInfo = ErrorTypeInfo.newBuilder().setConnectionTimeOutErrorType(
            ConnectionTimeOutErrorType.getDefaultInstance()).build();

    /**
     * A general test for the aggregateTargetHealthInfoToDTO(...) method.
     */
    @Test
    public void testAggregateValidationDiscoveryInfoToDTO() {
        TargetHealth validationSuccessful = makeHealthNormal(TargetHealthSubCategory.VALIDATION, "normallyValidated");
        TargetHealth validaionFailed = makeHealthCritical(TargetHealthSubCategory.VALIDATION, "validationFailed",
                connectionErrorTypeInfo, "Validation connection timeout.", 1_000_000, 1);
        TargetHealth discoverySuccessful = makeHealthNormal(TargetHealthSubCategory.DISCOVERY, "normallyDiscovered");
        TargetHealth discoveryPending = makeHealthMinor(TargetHealthSubCategory.DISCOVERY, "discoveryPending",
                "Discovery pending.");

        Map<Long, TargetDetails> targetDetails = new HashMap<>();
        targetDetails.put(0L, createTargetDetails(0L, validationSuccessful, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(1L, createTargetDetails(1L, validaionFailed, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(2L, createTargetDetails(2L, discoverySuccessful, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(3L, createTargetDetails(3L, discoveryPending, new ArrayList<>(), new ArrayList<>(), false));

        List<AggregatedHealthResponseDTO> responseItems = HealthDataMapper
                        .aggregateTargetHealthInfoToDTO(targetDetails);

        Assert.assertEquals(2, responseItems.size());
        for (AggregatedHealthResponseDTO subcategoryResponse : responseItems) {
            if (TargetCheckSubcategory.VALIDATION.name().equals(subcategoryResponse.getSubcategory())) {
                Assert.assertEquals(HealthState.CRITICAL, subcategoryResponse.getHealthState());
            } else if (TargetCheckSubcategory.DISCOVERY.name().equals(subcategoryResponse.getSubcategory())) {
                Assert.assertEquals(HealthState.MINOR, subcategoryResponse.getHealthState());
            }
        }
    }

    /**
     * Check that only the categories in non-NORMAL health state are reported.
     */
    @Test
    public void testReportNonNormal() {
        TargetHealth validationSuccessful = makeHealthNormal(TargetHealthSubCategory.VALIDATION, "normallyValidated");
        TargetHealth discoveryPending = makeHealthMinor(TargetHealthSubCategory.DISCOVERY, "discoveryPending",
                        "Discovery pending.");
        TargetHealth discoveryFailed = makeHealthCritical(TargetHealthSubCategory.DISCOVERY, "discoveryFailed",
                connectionErrorTypeInfo, "Discovery connection timeout.", 1_000_000, 6);
        TargetHealth duplicationFound = makeHealthCritical(TargetHealthSubCategory.DUPLICATION, "duplication",
                        ErrorTypeInfo.newBuilder().setDuplicationErrorType(DuplicationErrorType.getDefaultInstance())
                                .build(), "Duplication found.", 1_000_001);
        TargetHealth targetWithDelayedData = makeHealthCritical(TargetHealthSubCategory.DELAYED_DATA,
                "targetWithDelayedData", ErrorTypeInfo.newBuilder()
                        .setDelayedDataErrorType(DelayedDataErrorType.getDefaultInstance()).build(),
                "Too old data.");

        Map<Long, TargetDetails> targetDetails = new HashMap<>();
        targetDetails.put(0L, createTargetDetails(0L, validationSuccessful, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(1L, createTargetDetails(1L, discoveryPending, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(2L, createTargetDetails(2L, discoveryFailed, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(3L, createTargetDetails(3L, duplicationFound, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(4L, createTargetDetails(4L, targetWithDelayedData, new ArrayList<>(), new ArrayList<>(), false));

        List<AggregatedHealthResponseDTO> responseItems = HealthDataMapper
                        .aggregateTargetHealthInfoToDTO(targetDetails);

        Assert.assertEquals(3, responseItems.size());

        List<String> expectedSubcategoriesNames = Arrays.asList(TargetCheckSubcategory.DELAYED_DATA.toString(),
                        TargetCheckSubcategory.DISCOVERY.toString(), TargetCheckSubcategory.DUPLICATION.toString());
        for (AggregatedHealthResponseDTO subcategoryResponse : responseItems) {
            Assert.assertTrue(expectedSubcategoriesNames.contains(subcategoryResponse.getSubcategory()));
            Assert.assertEquals(HealthState.CRITICAL, subcategoryResponse.getHealthState());
            Assert.assertEquals(1, subcategoryResponse.getNumberOfItems());
        }
    }
}
