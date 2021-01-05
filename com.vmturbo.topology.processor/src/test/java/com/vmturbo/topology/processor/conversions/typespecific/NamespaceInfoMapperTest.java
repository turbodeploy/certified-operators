package com.vmturbo.topology.processor.conversions.typespecific;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.NamespaceData;

/**
 * Test {@link NamespaceInfoMapper}.
 */
public class NamespaceInfoMapperTest {
    private final NamespaceData.Builder data = NamespaceData.newBuilder()
        .setAverageNodeCpuFrequency(10.0);
    private final EntityDTO.Builder entity = EntityDTO.newBuilder()
        .setEntityType(EntityType.NAMESPACE)
        .setId("foo");
    private final NamespaceInfoMapper mapper = new NamespaceInfoMapper();

    /**
     * testNamespaceInfo.
     */
    @Test
    public void testNamespaceInfo() {
        entity.setNamespaceData(data);
        final TypeSpecificInfo info = mapper.mapEntityDtoToTypeSpecificInfo(entity, Collections.emptyMap());
        assertEquals(10.0, info.getNamespace().getAverageNodeCpuFrequency(), 0);
    }
}