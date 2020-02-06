package com.vmturbo.topology.processor.probes.internal;

import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.APPLICATION;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.COLLECTION_TIME;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.CONNECTION;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DB_CACHE_HIT_RATE;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.DB_MEM;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.HEAP;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.RESPONSE_TIME;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.SLA_COMMODITY;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.THREADS;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.TRANSACTION;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.TRANSACTION_LOG;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VCPU;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VMEM;
import static com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType.VSTORAGE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.APPLICATION_COMPONENT;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_APPLICATION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_TRANSACTION;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CONTAINER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.DATABASE_SERVER;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.SERVICE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE;
import static com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType.STRING;
import static com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants.ID;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainNodeBuilder;

/**
 * A definition of a supply chain for the UDE Probe.
 */
public class UserDefinedEntitiesSupplyChain {

    private static final Map<EntityType, Collection<CommodityType>> SELL_COMMODITY_MAP = Maps.newHashMap();

    static {
        SELL_COMMODITY_MAP.put(BUSINESS_APPLICATION,  ImmutableSet.of(TRANSACTION, RESPONSE_TIME, SLA_COMMODITY));
        SELL_COMMODITY_MAP.put(BUSINESS_TRANSACTION,  ImmutableSet.of(TRANSACTION, RESPONSE_TIME, SLA_COMMODITY));
        SELL_COMMODITY_MAP.put(SERVICE,               ImmutableSet.of(TRANSACTION, RESPONSE_TIME, SLA_COMMODITY));
        SELL_COMMODITY_MAP.put(APPLICATION_COMPONENT, ImmutableSet.of(TRANSACTION, RESPONSE_TIME, SLA_COMMODITY,
                                                      HEAP, COLLECTION_TIME, THREADS, CONNECTION, VCPU, VMEM));
        SELL_COMMODITY_MAP.put(DATABASE_SERVER,       ImmutableSet.of(TRANSACTION, RESPONSE_TIME, SLA_COMMODITY,
                                                      DB_MEM, DB_CACHE_HIT_RATE, TRANSACTION_LOG, CONNECTION, VCPU, VSTORAGE));
        SELL_COMMODITY_MAP.put(VIRTUAL_MACHINE,       ImmutableSet.of(VMEM, VCPU, VSTORAGE));
        SELL_COMMODITY_MAP.put(CONTAINER,             ImmutableSet.of(VMEM, VCPU, VSTORAGE));
    }

    /*pkg*/ UserDefinedEntitiesSupplyChain() {
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
        final SupplyChainNodeBuilder virtualMachineNode = createSupplyChainNodeBuilder(VIRTUAL_MACHINE,
                templateType, templatePriority, appCommodity);
        final SupplyChainNodeBuilder containerNode = createSupplyChainNodeBuilder(CONTAINER,
                templateType, templatePriority, appCommodity);

        return new SupplyChainBuilder()
                .top(businessAppNode)
                .entity(businessTransactionNode)
                .entity(serviceNode)
                .entity(appComponentNode)
                .entity(databaseServerNode)
                .entity(virtualMachineNode)
                .entity(containerNode)
                .configure();
    }

    private static SupplyChainNodeBuilder
    createSupplyChainNodeBuilder(EntityType entityType, TemplateType templateType, int priority,
                                 Collection<CommodityType> commoditiesWithKey) {
        final SupplyChainNodeBuilder builder = new SupplyChainNodeBuilder();
        builder.entity(entityType, templateType, priority);
        for (final CommodityType commodity : SELL_COMMODITY_MAP.get(entityType)) {
            builder.sellingOptional(commodity);
        }
        for (final CommodityType commodity : commoditiesWithKey) {
            builder.sellingOptionalWithKey(commodity);
        }
        MergedEntityMetadataBuilder mergedMetaData = new MergedEntityMetadataBuilder()
                .internalMatchingType(STRING).internalMatchingField(ID, Collections.emptyList())
                .externalMatchingType(STRING).externalMatchingEntityOid();
        builder.mergedBy(mergedMetaData.build());
        return builder;
    }

}
