package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.runner.cost.MarketPriceTable;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle.ComputePrice;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.market.topology.conversions.ReservedInstanceKey;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/*
 * The input to the Stable marriage algorithm.
 * Integration with XL:
 * add a constructor to convert market data structures into SMA data structures.
 */
public class SMAInput {

    private static final Logger logger = LogManager.getLogger();

    /*
     * List of input contexts
     */
    public final List<SMAInputContext> inputContexts;


    // map to convert commodity sold key to OSType.
    private static Map<String, OSType> nameToOSType = new HashMap<>();

    static {
        nameToOSType.put("LINUX",                  OSType.LINUX);
        nameToOSType.put("LINUX_SQL_ENTERPRISE",   OSType.LINUX_WITH_SQL_ENTERPRISE);
        nameToOSType.put("LINUX_SQL_STANDARD",     OSType.LINUX_WITH_SQL_STANDARD);
        nameToOSType.put("LINUX_SQL_WEB",          OSType.LINUX_WITH_SQL_WEB);
        nameToOSType.put("RHEL",                   OSType.RHEL);
        nameToOSType.put("SUSE",                   OSType.SUSE);
        nameToOSType.put("WINDOWS",                OSType.WINDOWS);
        nameToOSType.put("WINDOWS_BYOL",           OSType.WINDOWS_BYOL);
        nameToOSType.put("WINDOWS_SERVER",         OSType.WINDOWS_SERVER);
        nameToOSType.put("WINDOWS_SERVER_BURST",   OSType.WINDOWS_SERVER_BURST);
        nameToOSType.put("WINDOWS_SQL_ENTERPRISE", OSType.WINDOWS_WITH_SQL_ENTERPRISE);
        nameToOSType.put("WINDOWS_SQL_STANDARD",   OSType.WINDOWS_WITH_SQL_STANDARD);
        nameToOSType.put("WINDOWS_SQL_WEB",        OSType.WINDOWS_WITH_SQL_WEB);
    }

    /**
     * Constructor for SMAInput.
     * @param contexts the input to SMA partioned into contexts.
     *                 Each element in list corresponds to a context.
     */
    public SMAInput(@Nonnull final List<SMAInputContext> contexts) {
        this.inputContexts = Objects.requireNonNull(contexts, "contexts is null!");
    }

    /**
     * Compute SMAInput from XL data structures.
     * We are not processing any on-prem VMs, therefore, the code uses the CloudTopology.
     * The CloudTopology provides the VMs and computeTiers, and each VM's source computeTier.
     * We only need computeTiers and RIs in contexts where there are VMs.
     * Therefore, process VMs first to collect set of contexts, then process the computeTiers to
     * generate the SMATemplates.  Only after the SMATemplates are created, are the VMs
     * currentTemplate and providers be updated.
     *
     * @param cloudTopology the source cloud topology to get a VM's the source compute tier.
     * @param providers     a map from VM ID  to list of compute tier ID s that are providers
     *                      for this VM.
     * @param cloudCostData what are the cloud costs.
     * @param marketPriceTable used to figure out the discounts for buisness accounts
     * @param consistentScalingHelper used to figure out the consistent scaling information.
     * @param marketPriceTable price table to compute on-demand cost.
     */
    public SMAInput(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull Map<Long, Set<Long>> providers,
            @Nonnull CloudCostData<TopologyEntityDTO> cloudCostData,
            @Nonnull MarketPriceTable marketPriceTable,
            @Nonnull ConsistentScalingHelper consistentScalingHelper) {
        // check input parameters are not null
        Objects.requireNonNull(cloudTopology, "source cloud topology is null");
        Objects.requireNonNull(providers, "providers are null");
        Objects.requireNonNull(cloudCostData, "cloudCostData is null");
        Objects.requireNonNull(marketPriceTable, "marketPriceTable is null");

        // maps from SMAContext to entities.  Needed to build SMAInputContext.
        Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs = new HashMap<>();
        Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs = new HashMap<>();
        Map<SMAContext, Set<SMATemplate>> smaContextToTemplates = new HashMap<>();

        SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator =
                new SMAReservedInstanceKeyIDGenerator();

        /*
         * Map from computeTier ID to context to template, needed to convert provider list from
         * compute tier to template.  Need context, because their is a one to many relationship
         * between a compute tier and templates, one template per context for a compute tier.
         */
        Table<Long, SMAContext, SMATemplate> computeTierToContextToTemplateMap = HashBasedTable.create();
        // Map from region ID to OSType to context: used to restrict compute tier and RI generation.
        Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext = HashBasedTable.create();
        // keep track of business accounts in the context's billing family.  Needed to process compute tiers.
        Map<SMAContext, Set<Long>> contextToBusinessAccountIds = new HashMap<>();


        /*
         * For each virtual machines, create a VirtualMachine and partition into SMAContexts.
         */
        List<TopologyEntityDTO> virtualMachines =
            cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE);
        for (TopologyEntityDTO vm : virtualMachines) {
            if (!(vm.getEntityState() == EntityState.POWERED_ON)) {
                logger.debug(" vm={} state={} != POWERED_ON", vm.getOid(), vm.getEntityState().name());
                continue;
            }
            processVirtualMachine(vm, cloudTopology, cloudCostData, smaContextToVMs,
                regionToOsTypeToContext, contextToBusinessAccountIds,
                    consistentScalingHelper);
        }

        /*
         * For each ComputeTier, generate SMATemplate only in contexts where VMs exist.
         */
        final List<TopologyEntityDTO> computeTiers =
            cloudTopology.getAllEntitiesOfType(EntityType.COMPUTE_TIER_VALUE);
        int templatesCreated = processComputeTiers(computeTiers, cloudTopology, cloudCostData,
            regionToOsTypeToContext, contextToBusinessAccountIds, computeTierToContextToTemplateMap,
            smaContextToTemplates, marketPriceTable);

        /*
         * For each the RI, create an  and  into SMAContexts.
         */
        int numberRIsCreated = 0;
        int numberRIs = 0;
        for (ReservedInstanceData data : cloudCostData.getAllRiBought()) {
            numberRIs++;
            if (processReservedInstance(data, cloudTopology, computeTierToContextToTemplateMap,
                regionToOsTypeToContext, smaContextToRIs,
                    reservedInstanceKeyIDGenerator)) {
                numberRIsCreated++;
            }
        }

        /*
         * Update VM's current template and provider list.
         */
        for (SMAContext context : smaContextToVMs.keySet()) {
            Set<SMAVirtualMachine> vmsInContext = smaContextToVMs.get(context);
            updateVirtualMachines(vmsInContext, computeTierToContextToTemplateMap, providers,
                    cloudTopology, context,
                    reservedInstanceKeyIDGenerator, cloudCostData);
        }

        /*
         * build input contexts.
         */
        Set<SMAContext> smaContexts = smaContextToVMs.keySet();
        inputContexts = new ArrayList<>();
        for (SMAContext context : smaContexts) {
            Set<SMAVirtualMachine> smaVMs = smaContextToVMs.get(context);
            Set<SMAReservedInstance> smaRIs = smaContextToRIs.get(context);
            Set<SMATemplate> smaTemplates = smaContextToTemplates.get(context);
            if (ObjectUtils.isEmpty(smaTemplates)) {
                continue;
            }
            SMAInputContext inputContext = new SMAInputContext(context,
                Lists.newArrayList(smaVMs), (smaRIs == null ? new ArrayList<>()
                    : Lists.newArrayList(smaRIs)),
                Lists.newArrayList(smaTemplates));
            inputContexts.add(inputContext);
        }
    }

    /**
     * Create a SMA Virtual Machine and SMA context from a VM topology entity DTO.
     * Because scale actions do not modify region, osType or tenancy, the regionsToOsTypeToContext
     * keeps track of the what osTypes and contexts that are in a region.
     *  @param entity                    topology entity DTO that is a VM.
     * @param cloudTopology             the cloud topology to find source template.
     * @param cloudCostData             where to find costs and RI related info.  E.g. RI coverage for a VM.
     * @param smaContextToVMs           map from SMA context to set of SMA virtual machines, to be updated
     * @param regionToOsTypeToContext   table from region ID  to osType to set of SMAContexts, to be updated
     * @param contextToBusinessAccountIds map from context to set of business account IDs, to be updated.
     * @param consistentScalingHelper   used to figure out the consistent scaling information.
     */
    private void processVirtualMachine(@Nonnull TopologyEntityDTO entity,
                                       @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                       @Nonnull CloudCostData cloudCostData,
                                       Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs,
                                       Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext,
                                       Map<SMAContext, Set<Long>> contextToBusinessAccountIds,
                                       ConsistentScalingHelper consistentScalingHelper) {
        long oid = entity.getOid();
        String name = entity.getDisplayName();
        int entityType = entity.getEntityType();
        if (entityType != EntityType.VIRTUAL_MACHINE_VALUE) {
            logger.error("processVirtualMachine: entity is not a VM: ID={} name={}",
                oid, name);
            return;
        }
        if (!entity.getTypeSpecificInfo().hasVirtualMachine()) {
            logger.error("processVirtualMachine: entity ID={} name={} doesn't have VirutalMachineInfo",
                oid, name);
            return;
        }
        VirtualMachineInfo vmInfo = entity.getTypeSpecificInfo().getVirtualMachine();
        Optional<TopologyEntityDTO> zoneOptional = cloudTopology.getConnectedAvailabilityZone(oid);
        long zoneId = SMAUtils.NO_ZONE;
        if (zoneOptional.isPresent()) {
            zoneId = zoneOptional.get().getOid();
        } else {
            logger.error("processVirtualMachine: VM ID={} can't find availabilty zone",
                oid);
        }
        String tenancyName = vmInfo.getTenancy().name();
        Tenancy tenancy = Tenancy.valueOf(tenancyName);
        String osName = vmInfo.getGuestOsInfo().getGuestOsType().name();
        OSType  osType = OSType.valueOf(osName);
        /*
         * create Context
         */

        long regionId = getVMRegionId(oid, cloudTopology);
        long businessAccountId = getBusinessAccountId(oid, cloudTopology, "VM");
        long masterAccountId = getMasterAccountId(businessAccountId, cloudTopology, "VM");
        SMAContext context = new SMAContext(SMACSP.UNKNOWN, osType, regionId, masterAccountId, tenancy);
        Set<Long> accounts = contextToBusinessAccountIds.get(context);
        if (accounts == null) {
            accounts = new HashSet<>();
            contextToBusinessAccountIds.put(context, accounts);
        }
        accounts.add(businessAccountId);
        Optional<String> groupIdOptional = consistentScalingHelper.getScalingGroupId(oid);
        String groupId = SMAUtils.NO_GROUP_ID;
        if (groupIdOptional.isPresent()) {
            groupId = groupIdOptional.get();
        }
        /*
         * Create Virtual Machine
         */
        SMAVirtualMachine vm = new SMAVirtualMachine(oid,
            name,
            groupId,
            businessAccountId,
            null,
            new ArrayList<SMATemplate>(),
            SMAUtils.NO_RI_COVERAGE,
            zoneId,
            SMAUtils.NO_CURRENT_RI);
        if (vm == null) {
            logger.error("processVirtualMachine: createSMAVirtualMachine failed for VM ID={}", vm.getOid());
            return;
        }
        Set<SMAVirtualMachine> contextVMs = smaContextToVMs.get(context);
        if (contextVMs == null) {
            contextVMs = new HashSet<>();
            smaContextToVMs.put(context, contextVMs);
        }
        contextVMs.add(vm);
        // Update table from region to osType to Set of tenancy
        updateRegionsToOsTypeToContext(regionToOsTypeToContext, regionId, osType, context);
    }

    /**
     * For each VM, updates its current template, currentRICoverage, and providers.
     * This must be run after the compute tiers are processed and created the SMATemplates.
     *
     * @param vms the set of VMs that must be updated.
     * @param computeTierToContextToTemplateMap map from computeTier ID to context to template.
     * @param providersList map from VM ID to its set of computeTier IDs.
     * @param cloudTopology dictionary.
     * @param context the input context the VM belongs to.
     * @param reservedInstanceKeyIDGenerator ID generator for ReservedInstanceKey
     * @param cloudCostData             where to find costs and RI related info.  E.g. RI coverage for a VM.
     */
    private void updateVirtualMachines(Set<SMAVirtualMachine> vms,
                                       Table<Long, SMAContext, SMATemplate> computeTierToContextToTemplateMap,
                                       Map<Long, Set<Long>> providersList,
                                       CloudTopology<TopologyEntityDTO> cloudTopology,
                                       SMAContext context,
                                       SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator,
                                       @Nonnull CloudCostData cloudCostData) {


        for (SMAVirtualMachine vm : vms) {
            long oid = vm.getOid();
            String name = vm.getName();
            Optional<TopologyEntityDTO> currentTemplateOptional = cloudTopology.getComputeTier(oid);
            SMATemplate currentTemplate = null;
            long computeTierID = -1;
            if (!currentTemplateOptional.isPresent()) {
                logger.error("updateVirtualMachines: VM ID={} name={} doesn't have a compute Tier",
                    oid, name);
            } else {
                computeTierID = currentTemplateOptional.get().getOid();
                currentTemplate = computeTierToContextToTemplateMap.get(computeTierID, context);
                if (currentTemplate == null) {
                    logger.error("updateVirtualMachines: VM ID={} name={} can't find template ID={} in list of templates",
                        oid, name, computeTierID);
                } else {
                    vm.setCurrentTemplate(currentTemplate);
                }
            }
            Set<Long> providerOids = providersList.get(oid);
            List<SMATemplate> providers = new ArrayList<>();
            if (providerOids == null) {
                logger.error("updateVirtualMachines: could not find providers for VM ID={} name={}",
                    oid, name);
                vm.setNaturalTemplate(vm.getCurrentTemplate());
            } else {
                for (long providerId : providerOids) {
                    SMATemplate template = computeTierToContextToTemplateMap.get(providerId, context);
                    if (template == null) {
                        logger.error("updateVirtualMachines: VM ID={} name={} could not find providers ID={} in computeTierToContextToTemplateMap",
                            oid, name, providerId);      // currently expected
                    } else {
                        providers.add(template);
                    }
                }
                vm.setProviders(providers);
                vm.updateNaturalTemplateAndMinCostProviderPerFamily();
            }
            Pair<Long, Float> currentRICoverage = computeRiCoverage(oid, cloudTopology, cloudCostData, reservedInstanceKeyIDGenerator);
            logger.debug("processVirtualMachine: ID={} name={} cloudCostData currentRICoverage={} currentRI={}",
                    oid, name, currentRICoverage.getSecond(), currentRICoverage.getFirst());
            if (currentRICoverage.getSecond() > SMAUtils.EPSILON) {
                vm.setCurrentRIKey(currentRICoverage.getFirst());
                vm.setCurrentRICoverage(currentRICoverage.getSecond());
            }
        }
    }

    /**
     * Given a set of computer tiers, generate the corresponding SMATemplates.
     * A compute tier does not specify either a tenancy, a master or a business account.
     * Only generate SMATemplates that are needed by the Virtual machines that may be scaled.
     *
     * @param computeTiers                Entity that is expected to be a computeTier.
     * @param cloudTopology               dictionary.
     * @param cloudCostData               costs
     * @param regionToOsTypeToContext     table from region to osType to set of contexts.
     * @param contextToBusinessAccountIds set of business accounts in a context's billing
     * @param computeTierIdToContextToTemplateMap compute tier ID to context to Template map, to be updated
     * @param smaContextToTemplates       map from context to template, to be updated
     * @param marketPriceTable            price table to compute on-demand cost.
     * @return true if a template is created, else false
     */
    private int processComputeTiers(List<TopologyEntityDTO> computeTiers,
                                    CloudTopology<TopologyEntityDTO> cloudTopology,
                                    CloudCostData cloudCostData,
                                    Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext,
                                    Map<SMAContext, Set<Long>> contextToBusinessAccountIds,
                                    Table<Long, SMAContext, SMATemplate> computeTierIdToContextToTemplateMap,
                                    Map<SMAContext, Set<SMATemplate>> smaContextToTemplates,
                                    MarketPriceTable marketPriceTable) {
        int numberTemplatesCreated = 0;
        // set of valid regions
        for (Long regionId : regionToOsTypeToContext.rowKeySet()) {
            // set of valid compute tiers in that region.
            for (TopologyEntityDTO computeTier : computeTiers) {
                boolean created = processComputeTier(computeTier, cloudTopology, cloudCostData,
                    regionToOsTypeToContext, contextToBusinessAccountIds, regionId,
                    computeTierIdToContextToTemplateMap, smaContextToTemplates,
                        marketPriceTable);
                if (created) {
                    numberTemplatesCreated++;
                }
            }
        }
        return numberTemplatesCreated;
    }

    /**
     * Given an topology entity that is a compute tier, generate an SMATemplate.
     * A compute tier does not specify either a tenancy, a master or a business account.
     * A compute tier may support multiple OSTypes, which are found as sold commodities.
     * A compute tier may not be supported in a region, which can be determined by checking the
     * compute tier's connected entities.
     * Only generate SMATemplates that are needed by the Virtual machines that may be scaled.
     *
     * @param computeTier                 Entity that is expected to be a computeTier.
     * @param cloudTopology               entity dictionary.
     * @param cloudCostData               cost dictionary.
     * @param regionToOsTypeToContext     table from region ID to osType to set of contexts.
     * @param contextToBusinessAccountIds set of business accounts in a context's billing family.
     * @param regionId                    the ID of the region we are in.
     * @param tierToContextToTemplateMap  computeTier ID to context to template map, to be updated
     * @param smaContextToTemplates       map from context to template, to be updated
     * @param marketPriceTable            price table to compute on-demand cost.
     * @return true if a template is created, else false
     */
    private boolean processComputeTier(TopologyEntityDTO computeTier,
                                       CloudTopology<TopologyEntityDTO> cloudTopology,
                                       CloudCostData cloudCostData,
                                       Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext,
                                       Map<SMAContext, Set<Long>> contextToBusinessAccountIds,
                                       long regionId,
                                       Table<Long, SMAContext, SMATemplate> tierToContextToTemplateMap,
                                       Map<SMAContext, Set<SMATemplate>> smaContextToTemplates,
                                       MarketPriceTable marketPriceTable
    ) {
        long oid = computeTier.getOid();
        String name = computeTier.getDisplayName();
        if (computeTier.getEntityType() != EntityType.COMPUTE_TIER_VALUE) {
            logger.error("processComputeTier: entity ID={} name={} is not a computeTier: ",
                oid, name);
            return false;
        }
        if (!computeTier.getTypeSpecificInfo().hasComputeTier()) {
            logger.error("createSMATemplate: entity ID={} name={} doesn't have ComputeTierInfo",
                oid, name);
            return false;
        }
        ComputeTierInfo computeTierInfo = computeTier.getTypeSpecificInfo().getComputeTier();
        String family = computeTierInfo.getFamily();
        int coupons = computeTierInfo.getNumCoupons();
        Set<OSType> osTypes = computeOsTypes(computeTier);

        /*
         * Find all the contexts in which this template belongs.  Iterate threw osTypes and lookup
         * contexts.
         */
        for (OSType  osType : osTypes) {
            Set<SMAContext> contexts = regionToOsTypeToContext.get(regionId, osType);
            if (CollectionUtils.isEmpty(contexts)) {
                logger.trace("processComputeTier: ID={} name={}: regionId={} osType={} NOT found in regionToOsTypeToContext",
                    oid, name, regionId, osType.name(), regionId);
                continue;
            }
            logger.debug("processComputeTier: ID={} name={}: regionId={} osType={} found in regionToOsTypeToContext.",
                    oid, name, regionId, osType.name());
            for (SMAContext context: contexts) {
                Set<SMATemplate> templates = smaContextToTemplates.get(context);
                if (templates == null) {
                    templates = new HashSet<>();
                    smaContextToTemplates.put(context, templates);
                }
                /*
                 * For each osType create template with osType specific cost.
                 */
                SMATemplate template = new SMATemplate(oid, name, family, coupons, context, computeTier);
                templates.add(template);
                for (long businessAccountId: contextToBusinessAccountIds.get(context)) {
                    updateTemplateRate(template, osType, regionId, businessAccountId, cloudCostData,
                        cloudTopology, marketPriceTable);
                }
                logger.debug("processComputeTier: region={} new SMATemplate: oid={} name={} family={} coupons={}",
                        regionId, oid, name, family, coupons);
                tierToContextToTemplateMap.put(oid, context, template);
            }
        }
        return true;
    }

    /**
     * A computeTier may have commoditySold for multiple osTypes.  Find them all.
     *
     * @param entity  compute tier
     * @return list of osTypes that are sold as commodities.
     */
    private Set<OSType> computeOsTypes(TopologyEntityDTO entity) {
        Set<OSType> osTypes = new HashSet<>();
        for (CommoditySoldDTO commoditySold : entity.getCommoditySoldListList()) {
            CommodityType commodityType = commoditySold.getCommodityType();
            if (commodityType.getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE) {
                String osTypeName = commodityType.getKey();
                OSType osType = convertToOSType(osTypeName);
                osTypes.add(osType);
            }
        }
        return osTypes;
    }

    private OSType convertToOSType(String value) {
        OSType osType = nameToOSType.get(value.toUpperCase());
        if (osType == null) {
            return OSType.UNKNOWN_OS;
        } else {
            return osType;
        }
    }

    /**
     * Derived a template's on-demand cost from cloud cost data.
     *
     * @param template the template, whose cost will be updated.
     * @param osType the OS type.
     * @param regionId region where template resides.
     * @param businessAccountId business account of the template.
     * @param cloudCostData cost dictionary.
     * @param cloudTopology entity dictionary.
     * @param marketPriceTable price table to compute on-demand cost.
     */
    private void updateTemplateRate(SMATemplate template, OSType osType, long regionId, long businessAccountId,
                                    CloudCostData cloudCostData, CloudTopology<TopologyEntityDTO> cloudTopology,
                                    MarketPriceTable marketPriceTable) {
        long oid = template.getOid();
        String name = template.getName();
        /*
         * compute on-demand rate
         */
        // business account specific pricing.
        Optional<AccountPricingData> pricingDataOptional = cloudCostData.getAccountPricingData(businessAccountId);
        if (!pricingDataOptional.isPresent()) {
            logger.error("updateTemplateRate: template ID={} name={} doesn't have pricingData for accountId={}",
                oid, name, businessAccountId);
            return;
        } else {
            AccountPricingData accountPricingData = pricingDataOptional.get();
            ComputePriceBundle bundle = marketPriceTable.getComputePriceBundle(template.getComputeTier(), regionId, accountPricingData);
            List<ComputePrice> computePrices = bundle.getPrices();
            double hourlyRate    = 0.0d;
            for (ComputePrice price: computePrices) {
                if (price.getOsType() == osType) {  // FYI, UKNOWN_OS has a price, therefore can scale to natural template.
                    hourlyRate = price.getHourlyPrice();
                    break;
                }
            }
            if (hourlyRate == 0.0d) {
                logger.error("updateTemplateRate: could not find hourlyRate for tier ID={}:name={} osType={} in accountId={} regionId={}",
                    oid, name, osType.name(), businessAccountId, regionId);
            } else if (osType != OSType.UNKNOWN_OS) {
                logger.debug("Template on-demand Rate={}: name={} ID={}  accountId={} regionId={} osType={}",
                    hourlyRate, name, oid, businessAccountId, regionId, osType.name());
            }
            template.setOnDemandCost(businessAccountId, new SMACost((float)hourlyRate, 0f));

            /*
             * compute discounted costs
             * For AWS, there are no discounted costs.
             */
            template.setDiscountedCost(businessAccountId, SMAUtils.zeroCost);
        }
    }

    /**
     * Create SMAReservedInstance and SMAContext from ReservedInstanceData.
     * An SMA context:
     * 1) SMACSP csp;
     * 2) OSType  os;
     * 3) String region;
     * 4) String billingAccount;
     * 5) Tenancy tenancy;
     *
     * @param data            TopologyEntityDTO that is an RI
     * @param cloudTopology   topology to get business account and region.
     * @param templateMap     used to look up SMATemplate given the computeTier ID
     * @param regionToOsTypeToContext  map from regionID to OSType to context.
     * @param smaContextToRIs map from context to set of RIs, to be updated
     * @param reservedInstanceKeyIDGenerator ID generator for ReservedInstanceKey
     * @return true if RI is created
     */
    private boolean processReservedInstance(ReservedInstanceData data,
                                            CloudTopology<TopologyEntityDTO> cloudTopology,
                                            Table<Long, SMAContext, SMATemplate> templateMap,
                                            Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext,
                                            Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs,
                                            SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator) {
        ReservedInstanceBought riBought = data.getReservedInstanceBought();
        ReservedInstanceBoughtInfo riBoughtInfo = riBought.getReservedInstanceBoughtInfo();
        long businessAccountId = riBoughtInfo.getBusinessAccountId();
        long masterAccountId = getMasterAccountId(businessAccountId, cloudTopology, "RI");
        String name = riBoughtInfo.getProbeReservedInstanceId();  // getDisplayName();
        long zoneId = riBoughtInfo.getAvailabilityZoneId();
        zoneId = (zoneId == 0 ? SMAUtils.NO_ZONE : zoneId);
        int count = riBoughtInfo.getNumBought();
        ReservedInstanceBoughtCost boughtCost = riBoughtInfo.getReservedInstanceBoughtCost();

        ReservedInstanceSpec riSpec = data.getReservedInstanceSpec();
        long riID = riBought.getId();
        ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();
        // Can't find template until after processComputeTier

        ReservedInstanceType type = riSpecInfo.getType();
        int years = type.getTermYears();
        double riRate = computeRIRate(boughtCost, years);
        String tenancyName = riSpecInfo.getTenancy().name();
        Tenancy tenancy = Tenancy.valueOf(tenancyName);
        String osTypeName = riSpecInfo.getOs().name();
        OSType  osType = OSType.valueOf(osTypeName);
        long regionId = riSpecInfo.getRegionId();

        boolean found = contextExists(regionToOsTypeToContext, masterAccountId, regionId, osType, tenancy);
        if (found == false) {
            logger.info("no context billingAccountId={} regionId={} OSType={} Tenancy={} for RI name={}",
                 masterAccountId, regionId, osType.name(), tenancy.name(), name);
            return false;
        }

        SMAContext context = new SMAContext(SMACSP.UNKNOWN, osType, regionId, masterAccountId, tenancy);
        long templateId = riSpecInfo.getTierId();
        SMATemplate template = templateMap.get(templateId, context);
        if (template == null) {
            // should be ERROR
            logger.debug("processReservedInstance: can't find template with ID={} in templateMap for RI ID={} name={}",
                    templateId, riID, name);
            template = SMAUtils.BOGUS_TEMPLATE;
        }
        ReservedInstanceKey reservedInstanceKey = new ReservedInstanceKey(data,
                template.getFamily());
        long riKeyId = reservedInstanceKeyIDGenerator.lookUpRIKey(reservedInstanceKey, riID);
        String templateName = template.getName();
        logger.debug("processReservedInstance: ID={} name={} template={} new SMAcontext: osType={} regionId={} masterAccountId={} tenancy={}",
            riKeyId, name, templateName, osType, regionId, masterAccountId, tenancy);
        SMAReservedInstance ri = new SMAReservedInstance(riID,
            riKeyId,
            name,
            businessAccountId,
            template,
            zoneId,
            count,
            riSpecInfo.getSizeFlexible());
        if (ri == null) {
            logger.info("processReservedInstance: regionId={} template={} new SMA_RI FAILED: oid={} name={} accountId={} template={} zondId={} OS={} tenancy={} count={} utilization={} rate RI={} on-demand={}",
                regionId, templateName, riID, name, businessAccountId, templateName, zoneId, osType.name(),
                tenancy.name(), count, riRate);
        } else {
            logger.info("processReservedInstance: regionId={} template={} SMARI: oid={} name={} accountId={} template={} zondId={} OS={} tenancy={} count={} utilization={} rate RI={} on-demand={}",
                regionId, templateName, riID, name, businessAccountId, templateName, zoneId, osType.name(),
                tenancy.name(), count, riRate);
            Set<SMAReservedInstance> smaRIs = smaContextToRIs.get(context);
            if (smaRIs == null) {
                smaRIs = new HashSet<>();
                smaContextToRIs.put(context, smaRIs);
            }
            smaRIs.add(ri);
        }
        return true;
    }

    /**
     * Determine if there exists a context for csp, masterAccountId, regionId, osType and Tenancy.
     * @param regionToOsTypeToContext Table containing contexts.
     * @param masterAccountId master (billing) accountID
     * @param regionId region ID
     * @param osType OS
     * @param tenancy Tenancy
     * @return true if exists.
     */
    private boolean contextExists(Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext,
                                  long masterAccountId, long regionId, OSType osType, Tenancy tenancy) {
        Set<SMAContext> contexts = regionToOsTypeToContext.get(regionId, osType);
        if (contexts == null) {
            return false;
        }
        boolean found = false;
        // verify  exists a context with CSP, billing account and tenancy.
        for (SMAContext context : contexts) {
            if (context.getTenancy() == tenancy &&
                context.getBillingAccountId() == masterAccountId
            ) {
                found = true;
                break;
            }
        }
        return found;
    }

    /**
     * Find the region.
     *
     * @param oid           ID  of topology entity
     * @param cloudTopology where to look for the
     * @return region ID
     */
    private long getVMRegionId(long oid,
                               @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology
    ) {
        long regionId = -1;
        Optional<TopologyEntityDTO> regionOpt = cloudTopology.getConnectedRegion(oid);
        if (!regionOpt.isPresent()) {
            logger.error("getRegionID: can't find region for VM ID={}", oid);
        } else {
            regionId = regionOpt.get().getOid();
        }
        return regionId;
    }

    /**
     * Find the business account.
     *
     * @param oid           ID  of topology entity
     * @param cloudTopology where to look for the business account
     * @param msg           who is this.
     * @return master account ID
     */
    private long getBusinessAccountId(long oid,
                                      @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                      String msg) {
        long masterAccountId = -1;
        Optional<TopologyEntityDTO> accountOpt = cloudTopology.getOwner(oid);
        if (!accountOpt.isPresent()) {
            logger.error("getBusinessAccount{}: can't find business account for ID={}", msg, oid);
        } else {
            masterAccountId = accountOpt.get().getOid();
        }
        return masterAccountId;
    }

    /**
     * Find the master account.  In AWS, the master account is the billing family.
     * In Azure, the master account is the subscription.
     *
     * @param oid           ID  of topology entity
     * @param cloudTopology where to look for the master account
     * @param msg           who is this.
     * @return master account ID
     */
    private long getMasterAccountId(long oid,
                                    @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                    String msg) {
        long masterAccountId = oid;
        Optional<TopologyEntityDTO> accountOpt = cloudTopology.getOwner(oid);
        if (!accountOpt.isPresent()) {
            // if ID  is a master account, expect accountOpt to be empty
            logger.trace("getMasterAccount{}: can't find master account for ID={}", msg, oid);
        } else {
            masterAccountId = accountOpt.get().getOid();
            while (true) {
                accountOpt = cloudTopology.getOwner(masterAccountId);
                if (accountOpt.isPresent()) {
                    masterAccountId = accountOpt.get().getOid();
                } else {
                    break;
                }
            }
        }
        return masterAccountId;
    }

    /**
     * Compute the RI coverage of a VM.
     * @param vmoid         oid of virtualMachine
     * @param cloudTopology  the source cloud topology to get a VM's the source compute tier.
     * @param cloudCostData  cost dictionary
     * @param reservedInstanceKeyIDGenerator ID generator for ReservedInstanceKey
     * @return RI coverage of the VM
     */
    @Nonnull
    private Pair<Long, Float> computeRiCoverage(Long vmoid,
                                    CloudTopology<TopologyEntityDTO> cloudTopology,
                                    CloudCostData cloudCostData,
                                    SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator) {
        Pair<Long, Float> currentRICoverage = new Pair(SMAUtils.NO_CURRENT_RI, SMAUtils.NO_RI_COVERAGE);
        Optional<EntityReservedInstanceCoverage> coverageOptional = cloudCostData.getRiCoverageForEntity(vmoid);
        if (coverageOptional.isPresent()) {
            EntityReservedInstanceCoverage coverage = coverageOptional.get();
            currentRICoverage = computeCoverage(coverage, reservedInstanceKeyIDGenerator);
        } else {
            logger.error("processVirtualMachine: could not coverage VM ID={}",
                vmoid);
        }
        return currentRICoverage;
    }

    /**
     * Compute the current rI Converage and the SMA specific RI Key ID.
     * @param riCoverage RI coverage information.
     * @param reservedInstanceKeyIDGenerator ID generator for ReservedInstanceKey
     * @return RI coverage of the VM as RIKeyID,Coverage pair.
     */
    private Pair<Long, Float> computeCoverage(EntityReservedInstanceCoverage riCoverage,
                                              SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator) {
        Map<Long, Double> riToCoupons = riCoverage.getCouponsCoveredByRi();
        float utilization = SMAUtils.NO_RI_COVERAGE;
        long riKeyID = SMAUtils.NO_CURRENT_RI;
        for (Entry<Long, Double> coupons : riToCoupons.entrySet()) {
            utilization += coupons.getValue();
            if (coupons.getValue() > SMAUtils.EPSILON) {
                riKeyID =  reservedInstanceKeyIDGenerator
                        .getRIKeyIDForRIBoughtID(coupons.getKey());
            }
        }
        return new Pair(riKeyID, utilization);
    }

    private double computeRIRate(ReservedInstanceBoughtCost boughtCost, int years) {
        double fixedCost = boughtCost.getFixedCost().getAmount();
        double recurringCostPerHour = boughtCost.getRecurringCostPerHour().getAmount();
        double usageCostPerHour = boughtCost.getUsageCostPerHour().getAmount();
        double amortizedFixedCost = fixedCost / (float)(years * 365 * 24);
        return amortizedFixedCost + recurringCostPerHour + usageCostPerHour;
    }

    /**
     * Update regionToOsTypeToContext table.
     *
     * @param regionToOsTypeToContext table to capture the relationship from region to osType to set of contexts.
     * @param regionId region ID.
     * @param osType osType.
     * @param context context.
     */
    private void updateRegionsToOsTypeToContext(Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext,
                                                long regionId, OSType  osType, SMAContext context) {
        Map<OSType, Set<SMAContext>> map = (Map<OSType, Set<SMAContext>>) regionToOsTypeToContext.row(regionId);
        if (map == null) {
            map = new HashMap<>();
            regionToOsTypeToContext.put(regionId, osType, new HashSet<>());
        }
        Set<SMAContext> contexts = regionToOsTypeToContext.get(regionId, osType);
        if (contexts == null) {
            contexts = new HashSet<>();
        }
        contexts.add(context);
        regionToOsTypeToContext.put(regionId, osType, contexts);
    }

    public List<SMAInputContext> getContexts() {
        return inputContexts;
    }

    @Override
    public String toString() {
        return "SMAInput{" +
            "inputContexts=" + inputContexts.size() +
            '}';
    }

    /**
     * This class is to generate unique IDs for ReservedInstanceKey.
     */
    class SMAReservedInstanceKeyIDGenerator {
        //map from ReservedInstanceKey to riKeyID
        private Map<ReservedInstanceKey, Long> riKeyToOid = new HashMap();
        //map from RIBought to riKeyID
        private Map<Long, Long> riBoughtToRiKeyOID = new HashMap();
        //index for ID generator
        private AtomicLong riKeyIndex = new AtomicLong(0);

        /**
         * Create a unique id for the given ReservedInstanceKey. If id is already generated
         * then return it. Also associate the generated keyIDwith RIBoughtID
         *
         * @param riKey ReservedInstanceKey we are trying to find the riKey id for.
         * @param riBoughtID the id of the ri bought.
         * @return the id corresponding to the ReservedInstanceKey riKey.
         */
        public long lookUpRIKey(ReservedInstanceKey riKey, Long riBoughtID) {
            Long riKeyOID = riKeyToOid.get(riKey);
            if (riKeyOID != null) {
                riBoughtToRiKeyOID.put(riBoughtID, riKeyOID);
                return riKeyOID;
            } else {
                riKeyOID = riKeyIndex.getAndIncrement();
                riKeyToOid.put(riKey, riKeyOID);
                riBoughtToRiKeyOID.put(riBoughtID, riKeyOID);
                return riKeyOID;
            }
        }

        public Long getRIKeyIDForRIBoughtID(Long riBoughtID) {
            return riBoughtToRiKeyOID.get(riBoughtID);
        }

    }
}
