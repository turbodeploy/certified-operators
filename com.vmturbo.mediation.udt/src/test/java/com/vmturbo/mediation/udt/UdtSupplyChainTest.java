package com.vmturbo.mediation.udt;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.MatchingData;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;

/**
 * Test class for {@link UdtSupplyChain}.
 */
public class UdtSupplyChainTest {

    /**
     * Tests that BUSINESS_APPLICATION is the top of the supply chain.
     */
    @Test
    public void testChainTop() {
        UdtSupplyChain supplyChain = new UdtSupplyChain();
        Set<TemplateDTO> dto = supplyChain.getSupplyChainDefinition();
        TemplateDTO top = dto.stream().filter(template -> template.getCommoditySoldCount() == 0)
                .findFirst().orElse(null);
        Assert.assertNotNull(top);
        Assert.assertNotNull(top.getTemplateClass());
        Assert.assertEquals(EntityType.BUSINESS_APPLICATION, top.getTemplateClass());
    }

    /**
     * Tests that all templates have matching rules based on OID.
     */
    @Test
    public void testHasOidMatching() {
        UdtSupplyChain supplyChain = new UdtSupplyChain();
        Set<TemplateDTO> dto = supplyChain.getSupplyChainDefinition();
        Set<TemplateDTO> verifiedDto = dto.stream().filter(this::hasOidMatching).collect(Collectors.toSet());
        Assert.assertEquals(dto.size(), verifiedDto.size());
    }

    private boolean hasOidMatching(TemplateDTO template) {
        List<MatchingData> matchingData = template.getMergedEntityMetaData()
                .getMatchingMetadata().getExternalEntityMatchingPropertyList();
        if (!matchingData.isEmpty()) {
            return matchingData.get(0).hasMatchingEntityOid();
        }
        return false;
    }

}
