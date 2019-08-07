package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.BusinessUserInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.BusinessUserData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.SessionData;

/**
 * Tests to test the business user type specific info creation.
 */
public class BusinessUserMapperTest {

    private static final String VM = "123";
    private static final Long VM_OID = 123L;
    private static final long DURATION = 300000L;

    /**
     * Tests if business user data is mapped correctly to
     * the businesss user data in TypeSpecificInfo.
     */
    @Test
    public void testExtractTypeSpecificInfo() {

        final BusinessUserData buData = BusinessUserData.newBuilder()
                .addSessionData(SessionData.newBuilder()
                        .setVirtualMachine(VM).setSessionDuration(DURATION).build())
                .build();

        final EntityDTO buEntityDTO = EntityDTO.newBuilder()
                .setBusinessUserData(buData)
                .setId("ID")
                .setEntityType(EntityType.BUSINESS_USER).build();

        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setBusinessUser(BusinessUserInfo.newBuilder()
                        .putVmOidToSessionDuration(VM_OID, DURATION).build()).build();
        final BusinessUserMapper testBuilder = new BusinessUserMapper();

        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(buEntityDTO,
                Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }
}
