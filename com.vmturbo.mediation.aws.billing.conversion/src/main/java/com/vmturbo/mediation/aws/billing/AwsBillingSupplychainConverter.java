package com.vmturbo.mediation.aws.billing;

import static com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;

import java.util.Set;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.Provider.ProviderType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateCommodity;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainNodeBuilder;

/**
 * Helper class to convert supply chain definition from classic AwsBilling format to the new one.
 */
class AwsBillingSupplychainConverter {

    private final Set<TemplateDTO> definition;

    /**
     * Constructor.
     *
     * @param definition Supply chain definition to be converted.
     */
    AwsBillingSupplychainConverter(Set<TemplateDTO> definition) {
        this.definition = definition;
    }

    /**
     * Converting. Adding template DTO for describing VMs that sell/buy coupons.
     *
     * @return a set of templates.
     */
    Set<TemplateDTO> convert() {
        TemplateDTO vmDto = new SupplyChainNodeBuilder()
                .entity(EntityType.VIRTUAL_MACHINE, TemplateType.BASE, -1)
                .sellingOptional(CommodityType.COUPON)
                .provider(EntityType.RESERVED_INSTANCE, ProviderType.LAYERED_OVER)
                .buys(TemplateCommodity.newBuilder().setCommodityType(CommodityType.COUPON).build())
                .buildEntity();
        definition.add(vmDto);
        return definition;
    }
}
