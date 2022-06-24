package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ApplicationServiceInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationServiceData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationServiceData.Platform;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ApplicationServiceData.Tier;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTOOrBuilder;

public class ApplicationServiceInfoMapperTest {

    @Rule
    public TestName testName = new TestName();
    private static final Logger logger = LogManager.getLogger();
    private static final String PLATFORM = "linux";
    private static final String TIER = "Standard";
    private static final int APP_COUNT = 4;
    private static final int INSTANCE_COUNT = 21;
    private static final int MAX_INSTANCE_COUNT = 30;
    private static final boolean ZONE_REDUNDANT = true;

    @Test
    public void testExtractAppServiceInfo() {
        final EntityDTOOrBuilder appDto = EntityDTO.newBuilder().
                setApplicationServiceData(ApplicationServiceData.newBuilder()
                                .setAppCount(APP_COUNT)
                                .setCurrentInstanceCount(INSTANCE_COUNT)
                                .setPlatform(Platform.valueOf(PLATFORM.toUpperCase()))
                                .setTier(Tier.valueOf(TIER.toUpperCase()))
                                .setMaxInstanceCount(MAX_INSTANCE_COUNT)
                                .setZoneRedundant(ZONE_REDUNDANT)
                                .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setApplicationService(ApplicationServiceInfo.newBuilder()
                        .setAppCount(APP_COUNT)
                        .setCurrentInstanceCount(INSTANCE_COUNT)
                        .setPlatform(ApplicationServiceInfo.Platform.valueOf(PLATFORM.toUpperCase()))
                        .setTier(ApplicationServiceInfo.Tier.valueOf(TIER.toUpperCase()))
                        .setMaxInstanceCount(MAX_INSTANCE_COUNT)
                        .setZoneRedundant(ZONE_REDUNDANT)
                        .build())
                .build();
        assertExtractedTypeSpecificInfo(appDto, expected);
    }

    @Test
    public void testExtractAppServiceDataMissingFields() {
        final EntityDTOOrBuilder appDto = EntityDTO.newBuilder()
                .setApplicationServiceData(ApplicationServiceData.newBuilder()
                        .setAppCount(APP_COUNT)
                        .setCurrentInstanceCount(INSTANCE_COUNT)
                        .setZoneRedundant(ZONE_REDUNDANT)
                        .build());
        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setApplicationService(ApplicationServiceInfo.newBuilder()
                        .setAppCount(APP_COUNT)
                        .setCurrentInstanceCount(INSTANCE_COUNT)
                        .setZoneRedundant(ZONE_REDUNDANT)
                        .build())
                .build();
        assertExtractedTypeSpecificInfo(appDto, expected);
    }

    private void assertExtractedTypeSpecificInfo(EntityDTOOrBuilder appDto, TypeSpecificInfo expected) {
        // act
        final ApplicationServiceInfoMapper mapper = new ApplicationServiceInfoMapper();
        TypeSpecificInfo actual = mapper.mapEntityDtoToTypeSpecificInfo(appDto, Collections.emptyMap());
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Test: %s\n", testName.getMethodName()));
        sb.append(String.format("Entity DTO: \n%s\n", appDto));
        sb.append(String.format("Expected topology DTO: \n%s\n", expected));
        sb.append(String.format("Actual topology DTO: \n%s", actual));
        logger.info(sb.toString());
        // assert
        assertThat("Expected type specific info does not match actual type specific info",
                actual, equalTo(expected));
    }
}
