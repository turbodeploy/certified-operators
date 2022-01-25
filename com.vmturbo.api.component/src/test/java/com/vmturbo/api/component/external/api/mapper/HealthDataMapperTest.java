package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.component.external.api.HealthChecksTestBase;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO;
import com.vmturbo.api.enums.health.HealthState;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.ConnectionTimeOutErrorType;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.DuplicationErrorType;

/**
 * Contains tests for {@link HealthDataMapper}.
 */
public class HealthDataMapperTest extends HealthChecksTestBase {
    private static final Map<TargetHealthSubCategory, Set<HealthState>> EXPECTED_STATES = new EnumMap<>(TargetHealthSubCategory.class);

    private static final ErrorTypeInfo connectionErrorTypeInfo = ErrorTypeInfo.newBuilder().setConnectionTimeOutErrorType(
            ConnectionTimeOutErrorType.getDefaultInstance()).build();

    static {
        EXPECTED_STATES.put(TargetHealthSubCategory.VALIDATION, EnumSet.of(HealthState.CRITICAL, HealthState.NORMAL));
        EXPECTED_STATES.put(TargetHealthSubCategory.DISCOVERY, EnumSet.of(HealthState.MINOR, HealthState.NORMAL));
        EXPECTED_STATES.put(TargetHealthSubCategory.DUPLICATION, EnumSet.of(HealthState.CRITICAL));
    }

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
        TargetHealth duplicationFound = makeHealthCritical(TargetHealthSubCategory.DUPLICATION, "duplication",
                                                           ErrorTypeInfo.newBuilder().setDuplicationErrorType(DuplicationErrorType.getDefaultInstance())
                                                                   .build(), "Duplication found.", 1_000_001);

        Map<Long, TargetDetails> targetDetails = new HashMap<>();
        targetDetails.put(0L, createTargetDetails(0L, validationSuccessful, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(1L, createTargetDetails(1L, validaionFailed, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(2L, createTargetDetails(2L, discoverySuccessful, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(3L, createTargetDetails(3L, discoveryPending, new ArrayList<>(), new ArrayList<>(), false));
        targetDetails.put(4L, createTargetDetails(4L, duplicationFound, new ArrayList<>(), new ArrayList<>(), false));

        List<AggregatedHealthResponseDTO> responseItems = HealthDataMapper
                        .aggregateTargetHealthInfoToDTO(targetDetails);

        Assert.assertEquals(5, responseItems.size());
        final Map<TargetHealthSubCategory, Set<HealthState>> categoryToStates = responseItems.stream().collect(Collectors
                        .groupingBy(response -> TargetHealthSubCategory.valueOf(response.getSubcategory()),
                                    Collectors.mapping(AggregatedHealthResponseDTO::getHealthState,
                                                       Collectors.toSet())));
        Assert.assertEquals(EXPECTED_STATES, categoryToStates);
    }
}
