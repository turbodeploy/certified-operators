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
import static com.vmturbo.platform.common.dto.SupplyChain.Provider.ProviderType.HOSTING;
import static com.vmturbo.platform.common.dto.SupplyChain.Provider.ProviderType.LAYERED_OVER;
import static com.vmturbo.platform.sdk.common.supplychain.ServerEntityPropDefConstants.VM_IP;
import static com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants.ID;
import static com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants.IP_ADDRESS;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.SupplyChain;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
import com.vmturbo.platform.sdk.common.supplychain.ExternalLinkBuilder;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainLink;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainLinkBuilder;
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

    private static final String IP_ADDRESS_DESCRIPTION = "IP Address of the VM hosting the discovered node";

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

        final TemplateType templateType = TemplateType.EXTENSION;
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

        SupplyChainBuilder supplyChainBuilder = new SupplyChainBuilder();
        supplyChainBuilder.top(businessAppNode);

        createBusinessAppLinks(supplyChainBuilder, businessAppNode);
        createBusinessTransactionLinks(supplyChainBuilder, businessTransactionNode);
        createServiceLinks(supplyChainBuilder, serviceNode);
        createAppComponentLinks(supplyChainBuilder, appComponentNode);
        createDbServerLinks(supplyChainBuilder, databaseServerNode);

        return supplyChainBuilder
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
            builder.selling(commodity);
        }
        for (final CommodityType commodity : commoditiesWithKey) {
            builder.sellingWithKey(commodity);
        }
        MergedEntityMetadataBuilder mergedMetaData = new MergedEntityMetadataBuilder()
                .internalMatchingType(STRING).internalMatchingField(ID, Collections.emptyList())
                .externalMatchingType(STRING).externalMatchingEntityOid();
        builder.mergedBy(mergedMetaData.build());
        return builder;
    }

    private void createBusinessAppLinks(
            SupplyChainBuilder supplyChainBuilder,
            SupplyChainNodeBuilder businessApp) {
        final SupplyChainLink businessAppToBusinessTransactionLink = new SupplyChainLinkBuilder()
                .link(BUSINESS_APPLICATION, BUSINESS_TRANSACTION, LAYERED_OVER)
                .commodityWithKey(APPLICATION)
                .build();

        final SupplyChainLink businessAppToServiceLink = new SupplyChainLinkBuilder()
                .link(BUSINESS_APPLICATION, SERVICE, LAYERED_OVER)
                .commodityWithKey(APPLICATION)
                .build();

        final SupplyChainLink businessAppToDbLink = new SupplyChainLinkBuilder()
                .link(BUSINESS_APPLICATION, DATABASE_SERVER, LAYERED_OVER)
                .commodityWithKey(APPLICATION)
                .build();

        supplyChainBuilder.entity(businessApp)
                .connectsTo(businessAppToDbLink)
                .connectsTo(businessAppToBusinessTransactionLink)
                .connectsTo(businessAppToServiceLink);
    }

    private void createBusinessTransactionLinks(
            SupplyChainBuilder supplyChainBuilder,
            SupplyChainNodeBuilder businessTransaction) {
        final SupplyChainLink businessTransactionToServiceLink = new SupplyChainLinkBuilder()
                .link(BUSINESS_TRANSACTION, SERVICE, LAYERED_OVER)
                .commodityWithKey(APPLICATION)
                .build();

        supplyChainBuilder.entity(businessTransaction)
                .connectsTo(businessTransactionToServiceLink);
    }

    private void createServiceLinks(
            SupplyChainBuilder supplyChainBuilder,
            SupplyChainNodeBuilder service) {
        final SupplyChainLink serviceToAppComponentLink = new SupplyChainLinkBuilder()
                .link(SERVICE, APPLICATION_COMPONENT, LAYERED_OVER)
                .commodityWithKey(APPLICATION)
                .build();

        final SupplyChainLink serviceToDbServerLink = new SupplyChainLinkBuilder()
                .link(SERVICE, DATABASE_SERVER, LAYERED_OVER)
                .commodityWithKey(APPLICATION)
                .build();

        supplyChainBuilder.entity(service)
                .connectsTo(serviceToAppComponentLink)
                .connectsTo(serviceToDbServerLink);
    }

    private void createAppComponentLinks(
            SupplyChainBuilder supplyChainBuilder,
            SupplyChainNodeBuilder appComponent) {
        final SupplyChainLink appComponentToVmLink = new SupplyChainLinkBuilder()
                .link(APPLICATION_COMPONENT, VIRTUAL_MACHINE, HOSTING)
                .commodity(VMEM)
                .commodity(VCPU)
                .commodityWithKey(APPLICATION)
                .build();

        final SupplyChain.ExternalEntityLink appComponentToVmExternalLink = new ExternalLinkBuilder()
                .link(APPLICATION_COMPONENT, VIRTUAL_MACHINE, HOSTING)
                .commodityWithKey(APPLICATION)
                .probeEntityPropertyDef(IP_ADDRESS, IP_ADDRESS_DESCRIPTION)
                .externalEntityPropertyDef(VM_IP)
                .build();

        final SupplyChainLink appComponentToContainerLink = new SupplyChainLinkBuilder()
                .link(APPLICATION_COMPONENT, CONTAINER, HOSTING)
                .commodity(VMEM)
                .commodity(VCPU)
                .commodityWithKey(APPLICATION)
                .build();

        final SupplyChain.ExternalEntityLink appComponentToContainerExternalLink = new ExternalLinkBuilder()
                .link(APPLICATION_COMPONENT, CONTAINER, HOSTING)
                .commodityWithKey(APPLICATION)
                .probeEntityPropertyDef(IP_ADDRESS, IP_ADDRESS_DESCRIPTION)
                .externalEntityPropertyDef(VM_IP)
                .build();

        supplyChainBuilder.entity(appComponent)
                .connectsTo(appComponentToVmLink)
                .connectsTo(appComponentToVmExternalLink)
                .connectsTo(appComponentToContainerLink)
                .connectsTo(appComponentToContainerExternalLink);
    }

    private void createDbServerLinks(
            SupplyChainBuilder supplyChainBuilder,
            SupplyChainNodeBuilder dbServer) {
        final SupplyChainLink dbServerToVmLink = new SupplyChainLinkBuilder()
                .link(DATABASE_SERVER, VIRTUAL_MACHINE, HOSTING)
                .commodity(VMEM)
                .commodity(VCPU)
                .commodityWithKey(APPLICATION)
                .build();

        final SupplyChain.ExternalEntityLink dbServerToVmExternalLink = new ExternalLinkBuilder()
                .link(DATABASE_SERVER, VIRTUAL_MACHINE, HOSTING)
                .commodityWithKey(APPLICATION)
                .probeEntityPropertyDef(IP_ADDRESS, IP_ADDRESS_DESCRIPTION)
                .externalEntityPropertyDef(VM_IP)
                .build();

        final SupplyChainLink dbServerToContainerLink = new SupplyChainLinkBuilder()
                .link(DATABASE_SERVER, CONTAINER, HOSTING)
                .commodity(VMEM)
                .commodity(VCPU)
                .commodity(VSTORAGE)
                .commodityWithKey(APPLICATION)
                .build();

        final SupplyChain.ExternalEntityLink dbServerToContainerExternalLink = new ExternalLinkBuilder()
                .link(DATABASE_SERVER, CONTAINER, HOSTING)
                .commodityWithKey(APPLICATION)
                .probeEntityPropertyDef(IP_ADDRESS, IP_ADDRESS_DESCRIPTION)
                .externalEntityPropertyDef(VM_IP)
                .build();

        supplyChainBuilder.entity(dbServer)
                .connectsTo(dbServerToVmLink)
                .connectsTo(dbServerToVmExternalLink)
                .connectsTo(dbServerToContainerLink)
                .connectsTo(dbServerToContainerExternalLink);
    }
}
