package com.vmturbo.mediation.aws.billing;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;

/**
 * Test {@link AwsBillingSupplychainConverter}.
 */
public class AwsBillingSupplychainConverterTest {

    /**
     * Tests if there is a VM template in the supply chain definition.
     */
    @Test
    public void testConvert() {
        final AwsBillingConversionProbe probe = new AwsBillingConversionProbe();
        final Set<TemplateDTO> templates = probe.getSupplyChainDefinition();
        Assert.assertEquals(3, templates.size());

        TemplateDTO vmTemplate = templates.stream()
                .filter(t -> t.getTemplateClass() == EntityType.VIRTUAL_MACHINE)
                .findFirst().orElse(null);

        Assert.assertNotNull(vmTemplate);
    }
}
