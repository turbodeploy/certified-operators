package com.vmturbo.topology.processor.conversions.typespecific;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.DesktopPoolInfo.VmWithSnapshot;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolAssignmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolCloneType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.DesktopPoolData.DesktopPoolProvisionType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualDatacenterData;

/**
 * Tests to test the desktop pool type specific info creation.
 */
public class DesktopPoolInfoMapperTest {

    private static final String SNAPSHOT = "/Clone Snapshot";
    private static final String MASTER_IMAGE = "123";
    private static final long MASTER_IMAGE_OID = 123L;

    /**
     * Tests if desktop pool data is mapped correctly to
     * the desktop pool data in TypeSpecificInfo.
     */
    @Test
    public void testExtractTypeSpecificInfo() {

        final DesktopPoolData dpData = DesktopPoolData.newBuilder()
                .setAssignmentType(DesktopPoolAssignmentType.DYNAMIC)
                .setProvisionType(DesktopPoolProvisionType.UPFRONT)
                .setCloneType(DesktopPoolCloneType.FULL)
                .setSnapshot(SNAPSHOT)
                .setMasterImage(MASTER_IMAGE).build();

        final VirtualDatacenterData vdcData =
                VirtualDatacenterData.newBuilder().setDesktopPoolData(dpData).build();

        final EntityDTO dpEntityDTO = EntityDTO.newBuilder()
                .setVirtualDatacenterData(vdcData)
                .setId("ID")
                .setEntityType(EntityType.DESKTOP_POOL).build();

        TypeSpecificInfo expected = TypeSpecificInfo.newBuilder()
                .setDesktopPool(DesktopPoolInfo.newBuilder()
                        .setAssignmentType(DesktopPoolAssignmentType.DYNAMIC)
                        .setProvisionType(DesktopPoolProvisionType.UPFRONT)
                        .setCloneType(DesktopPoolCloneType.FULL)
                        .setVmWithSnapshot(VmWithSnapshot.newBuilder()
                                .setVmReferenceId(MASTER_IMAGE_OID)
                                .setSnapshot(SNAPSHOT)
                                .build())
                        .build())
                .build();
        final DesktopPoolInfoMapper testBuilder = new DesktopPoolInfoMapper();

        // act
        TypeSpecificInfo result = testBuilder.mapEntityDtoToTypeSpecificInfo(dpEntityDTO,
                Collections.emptyMap());
        // assert
        assertThat(result, equalTo(expected));
    }
}
