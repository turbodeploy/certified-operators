package com.vmturbo.mediation.udt;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.APPLICATION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_APPLICATION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_TRANSACTION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CONTAINER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CONTAINER_POD;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATABASE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATABASE_SERVER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.SERVICE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants.ID;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainNodeBuilder;

/**
 * Supply chain definition for the 'UserDefinedTopology' Probe.
 */
public class UdtSupplyChain {

    /*pkg*/ UdtSupplyChain() {
        // default constructor
    }

    /**
     * Creates a set of DTOs, which describe UDE Probe`s supply chain.
     *
     * @return a set of {@link TemplateDTO}.
     */
    @Nonnull
    public Set<TemplateDTO> getSupplyChainDefinition() {

        final TemplateType templateType = TemplateType.BASE;
        final int templatePriority = -1;
        final Collection<CommodityType> appCommodity = ImmutableSet.of(APPLICATION);

        final SupplyChainNodeBuilder businessAppNode = createSupplyChainNodeBuilder(BUSINESS_APPLICATION,
                templateType, templatePriority, ImmutableSet.of());
        final SupplyChainNodeBuilder businessTransactionNode = createSupplyChainNodeBuilder(BUSINESS_TRANSACTION,
                templateType, templatePriority, appCommodity);
        final SupplyChainNodeBuilder serviceNode = createSupplyChainNodeBuilder(SERVICE,
                templateType, templatePriority, appCommodity);
        final SupplyChainNodeBuilder appComponentNode = createSupplyChainNodeBuilder(APPLICATION_COMPONENT,
                templateType, templatePriority, appCommodity);
        final SupplyChainNodeBuilder databaseServerNode = createSupplyChainNodeBuilder(DATABASE_SERVER,
                templateType, templatePriority, appCommodity);
        final SupplyChainNodeBuilder databaseNode = createSupplyChainNodeBuilder(DATABASE,
                templateType, templatePriority, appCommodity);
        final SupplyChainNodeBuilder virtualMachineNode = createSupplyChainNodeBuilder(VIRTUAL_MACHINE,
                templateType, templatePriority, appCommodity);
        final SupplyChainNodeBuilder containerNode = createSupplyChainNodeBuilder(CONTAINER,
                templateType, templatePriority, appCommodity);
        final SupplyChainNodeBuilder containerPodNode = createSupplyChainNodeBuilder(CONTAINER_POD,
                templateType, templatePriority, appCommodity);

        return new SupplyChainBuilder()
                .top(businessAppNode)
                .entity(businessTransactionNode)
                .entity(serviceNode)
                .entity(appComponentNode)
                .entity(databaseServerNode)
                .entity(databaseNode)
                .entity(virtualMachineNode)
                .entity(containerNode)
                .entity(containerPodNode)
                .configure();
    }

    private static SupplyChainNodeBuilder
    createSupplyChainNodeBuilder(EntityType entityType, TemplateType templateType, int priority,
                                 Collection<CommodityType> commoditiesWithKey) {
        final SupplyChainNodeBuilder builder = new SupplyChainNodeBuilder();
        builder.entity(entityType, templateType, priority);
        for (final CommodityType commodity : commoditiesWithKey) {
            builder.sellingOptionalWithKey(commodity);
        }
        MergedEntityMetadataBuilder mergedMetaData = new MergedEntityMetadataBuilder()
                .internalMatchingField(ID, Collections.emptyList())
                .externalMatchingEntityOid();
        builder.mergedBy(mergedMetaData.build());
        return builder;
    }

}
