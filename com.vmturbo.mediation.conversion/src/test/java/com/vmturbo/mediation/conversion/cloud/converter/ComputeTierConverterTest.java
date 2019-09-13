package com.vmturbo.mediation.conversion.cloud.converter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO;
import com.vmturbo.platform.common.dto.ProfileDTO.EntityProfileDTO.VMProfileDTO;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Unit tests for ComputeTierConverter.
 */
public class ComputeTierConverterTest {

    private ComputeTierConverter converter;

    /**
     * Initializes instance of ComputeTierConverter.
     */
    @Before
    public void setUp() {
        converter = new ComputeTierConverter(SDKProbeType.AZURE);
    }

    /**
     * Test that computeTier family is set from the VMProfileDTO instanceSizeFamily field.
     */
    @Test
    public void testInstanceSizeFamilySet() {
        final String family = "m4";
        final String computeTierId = family + ".small";
        final CloudDiscoveryConverter cloudDiscoveryConverter = mock(CloudDiscoveryConverter.class);
        when(cloudDiscoveryConverter.getProfileDTO(computeTierId))
                .thenReturn(EntityProfileDTO.newBuilder()
                        .setId("1122")
                        .setEntityType(EntityType.VIRTUAL_MACHINE)
                        .setVmProfileDTO(VMProfileDTO.newBuilder().setInstanceSizeFamily(family)
                                .build()).build());
        final EntityDTO.Builder builder = EntityDTO.newBuilder().setId(computeTierId);
        converter.convert(builder, cloudDiscoveryConverter);
        Assert.assertEquals(family, builder.getComputeTierData().getFamily());
    }
}