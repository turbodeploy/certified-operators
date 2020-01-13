package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
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
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/*
 * The input to the Stable marriage algorithm.
 *
 * Integration with XL:
 *  add a constructor to convert market data structures into SMA data structures.
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
     * @param marketPriceTable price table to compute on-demand cost.
     */
    public SMAInput(
        @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
        @Nonnull Map<Long, Set<Long>> providers,
        @Nonnull CloudCostData cloudCostData,
        @Nonnull MarketPriceTable marketPriceTable
    ) {
        // check input parameters are not null
        Objects.requireNonNull(cloudTopology, "source cloud topology is null");
        Objects.requireNonNull(providers, "providers are null");
        Objects.requireNonNull(cloudCostData, "cloudCostData is null");
        Objects.requireNonNull(marketPriceTable, "marketPriceTable is null");

        // maps from SMAContext to entities.  Needed to build SMAInputContext.
        Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs = new HashMap<>();
        Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs = new HashMap<>();
        Map<SMAContext, Set<SMATemplate>> smaContextToTemplates = new HashMap<>();
        // Set of VMs, needed to update VMs after SMATemplates are created.
        Set<SMAVirtualMachine> vms = new HashSet<>();
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
        Map<Long, Set<SMAVirtualMachine>> regionToVirtualMachines = new HashMap<>();
        List<TopologyEntityDTO> virtualMachines =
            cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE);
        int numberVMsCreated = 0;
        for (TopologyEntityDTO vm : virtualMachines) {
            if (!(vm.getEntityState() == EntityState.POWERED_ON)) {
                logger.debug(" vm={} state={} != POWERED_ON", vm.getOid(), vm.getEntityState().name());
                continue;
            }
            numberVMsCreated++;
            processVirtualMachine(vm, cloudTopology, cloudCostData, vms, smaContextToVMs,
                regionToOsTypeToContext, regionToVirtualMachines, contextToBusinessAccountIds);
        }
        dumpVMsByRegion(regionToVirtualMachines);
        dumpRegionToOsTypeToContext(regionToOsTypeToContext);

        /*
         * For each ComputeTier, generate SMATemplate only in contexts where VMs exist.
         */
        Map<Long, Set<SMATemplate>> regionToTemplates = new HashMap<>();
        final List<TopologyEntityDTO> computeTiers =
            cloudTopology.getAllEntitiesOfType(EntityType.COMPUTE_TIER_VALUE);
        int templatesCreated = processComputeTiers(computeTiers, cloudTopology, cloudCostData,
            regionToOsTypeToContext, contextToBusinessAccountIds, computeTierToContextToTemplateMap,
            smaContextToTemplates, regionToTemplates, marketPriceTable);

        logger.info("created {} templates from {} computeTiers in {} contexts", templatesCreated,
            computeTiers.size(), smaContextToTemplates.keySet().size());
        for (SMAContext context : smaContextToTemplates.keySet()) {
            logger.info("context: masterAccount={} region={} os={} tenancy={}",
                context.getBillingAccountId(), context.getRegionId(), context.getOs().name(), context.getTenancy().name());
        }
        dumpTemplatesByRegion(regionToTemplates);

        /*
         * Update VM's current template and provider list.
         */
        for (SMAContext context : smaContextToVMs.keySet()) {
            Set<SMAVirtualMachine> vmsInContext = smaContextToVMs.get(context);
            updateVirtualMachines(vmsInContext, computeTierToContextToTemplateMap, providers, cloudTopology, context);
        }
        dumpVMsByContext(smaContextToVMs, numberVMsCreated);

        /*
         * For each the RI, create an  and  into SMAContexts.
         */
        Map<Long, Set<SMAReservedInstance>> regionToReservedInstances = new HashMap<>();
        int numberRIsCreated = 0;
        int numberRIs = 0;
        for (ReservedInstanceData data : cloudCostData.getAllRiBought()) {
            numberRIs++;
            if (processReservedInstance(data, cloudTopology, computeTierToContextToTemplateMap,
                regionToOsTypeToContext, smaContextToRIs, regionToReservedInstances)) {
                numberRIsCreated++;
            }
        }
        logger.info("created {} RIs from {} found in {} contexts", numberRIs,
            numberRIsCreated, smaContextToRIs.keySet().size());
        for (SMAContext context : smaContextToRIs.keySet()) {
            logger.info("context: masterAccount={} region={} os={} tenancy={}",
                context.getBillingAccountId(), context.getRegionId(), context.getOs().name(), context.getTenancy().name());
        }
        dumpRIsByRegion(regionToReservedInstances);

        /*
         * build input contexts.
         */
        Set<SMAContext> smaContexts = smaContextToVMs.keySet();
        logger.info("Build Input Contexts from {} contexts", smaContexts.size());
        inputContexts = new ArrayList<>();
        for (SMAContext context : smaContexts) {
            Set<SMAVirtualMachine> smaVMs = smaContextToVMs.get(context);
            if (ObjectUtils.isEmpty(smaVMs)) {
                // there may be a RI context that has no VMs.
                logger.warn(" no VM for context={}", context);
            }
            Set<SMAReservedInstance> smaRIs = smaContextToRIs.get(context);
            Set<SMATemplate> smaTemplates = smaContextToTemplates.get(context);
            if (ObjectUtils.isEmpty(smaTemplates)) {
                logger.error(" no template for context={}", context);
                continue;
            }
            logger.info(": context: billing={} region={} OS={} tenancy={} #VMs={} #RIs={} #templates={}",
                context.getBillingAccountId(), context.getRegionId(), context.getOs().name(),
                context.getTenancy().name(), smaVMs.size(), (smaRIs == null ? 0 : smaRIs.size()),
                smaTemplates.size());
            SMAInputContext inputContext = new SMAInputContext(context,
                Lists.newArrayList(smaVMs), (smaRIs == null ? null : Lists.newArrayList(smaRIs)),
                Lists.newArrayList(smaTemplates));
            inputContexts.add(inputContext);
        }
        logger.info("inputContexts.size()={}", inputContexts.size());
    }

    /**
     * Create a SMA Virtual Machine and SMA context from a VM topology entity DTO.
     * Because scale actions do not modify region, osType or tenancy, the regionsToOsTypeToContext
     * keeps track of the what osTypes and contexts that are in a region.
     *
     * @param entity                    topology entity DTO that is a VM.
     * @param cloudTopology             the cloud topology to find source template.
     * @param cloudCostData             where to find costs and RI related info.  E.g. RI coverage for a VM.
     * @param vms                       set of virtual machines, to be updated
     * @param smaContextToVMs           map from SMA context to set of SMA virtual machines, to be updated
     * @param regionToOsTypeToContext   table from region ID  to osType to set of SMAContexts, to be updated
     * @param regionToVirtualMachines   map from region ID  to set of SMAVirtualMachines, to be updated
     * @param contextToBusinessAccountIds map from context to set of business account IDs, to be updated.
     */
    private void processVirtualMachine(@Nonnull TopologyEntityDTO entity,
                                       @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                       @Nonnull CloudCostData cloudCostData,
                                       Set<SMAVirtualMachine> vms,
                                       Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs,
                                       Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext,
                                       Map<Long, Set<SMAVirtualMachine>> regionToVirtualMachines,
                                       Map<SMAContext, Set<Long>> contextToBusinessAccountIds
    ) {
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
        if (osType == OSType.UNKNOWN_OS && !osName.equalsIgnoreCase(OSType.UNKNOWN_OS.name())) {
            logger.warn("processVirtualMachine: osName={} converted to UNKNOWN_OS", osName);
        }
        /*
         * create Context
         */
        // TODO: find CSP
        SMACSP csp = SMACSP.AWS;
        long regionId = getVMRegionId(oid, cloudTopology);
        long businessAccountId = getBusinessAccountId(oid, cloudTopology, "VM");
        long masterAccountId = getMasterAccountId(businessAccountId, cloudTopology, "VM");
        logger.info("processVirtualMachine: new SMAContext csp={} osType={} region={} masterAccount={} account={} tenancy={}",
            csp, osType, regionId, masterAccountId, businessAccountId, tenancy);
        SMAContext context = new SMAContext(csp, osType, regionId, masterAccountId, tenancy);

        float currentRICoverage = computeRiCoverage(entity, cloudTopology, cloudCostData);
        Set<Long> accounts = contextToBusinessAccountIds.get(context);
        if (accounts == null) {
            accounts = new HashSet<>();
            contextToBusinessAccountIds.put(context, accounts);
        }
        accounts.add(businessAccountId);
        // TODO: determine GROUP ID
        long groupId = SMAUtils.NO_GROUP_ID;
        /*
         * Create Virtual Machine
         */
        SMAVirtualMachine vm = new SMAVirtualMachine(oid,
            name,
            groupId,
            businessAccountId,
            null,
            new ArrayList<SMATemplate>(),
            currentRICoverage,
            zoneId);
        if (vm == null) {
            logger.error("processVirtualMachine: createSMAVirtualMachine failed for VM ID={}", vm.getOid());
            return;
        } else {
            logger.debug("processVirtualMachine: region={} new SMAVirtualMachine: csp={} osType={} region={} masterAccount={} tenancy={}",
                regionId, csp, osType, regionId, masterAccountId, tenancy);
        }
        updateRegionToVirtualMachines(regionToVirtualMachines, regionId, vm);
        vms.add(vm);
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
     * @param templateMap map from computeTier ID to template.
     * @param providersList map from VM ID to its set of computeTier IDs.
     * @param cloudTopology dictionary.
     */
    private void updateVirtualMachines(Set<SMAVirtualMachine> vms,
                                       Table<Long, SMAContext, SMATemplate> templateMap,
                                       Map<Long, Set<Long>> providersList,
                                       CloudTopology<TopologyEntityDTO> cloudTopology,
                                       SMAContext context) {
        for (SMAVirtualMachine vm : vms) {
            long oid = vm.getOid();
            String name = vm.getName();
            Optional<TopologyEntityDTO> currentTemplateOptional = cloudTopology.getComputeTier(oid);
            SMATemplate currentTemplate = null;
            long templateId = -1;
            int numberOfCoupons = 0;
            if (!currentTemplateOptional.isPresent()) {
                logger.error("updateVirtualMachines: VM ID={} name={} doesn't have a compute Tier",
                    oid, name);
            } else {
                templateId = currentTemplateOptional.get().getOid();
                currentTemplate = templateMap.get(templateId, context);
                if (currentTemplate == null) {
                    logger.error("updateVirtualMachines: VM ID={} name={} can't find template ID={} in list of templates",
                        oid, name, templateId);
                } else {
                    vm.setCurrentTemplate(currentTemplate);
                    numberOfCoupons = currentTemplate.getCoupons();
                }
            }
            // TODO: where to find number of this VM's coupons that are covered by RIs.  Is it a commodity?
            float numberOfCouponsUsed = SMAUtils.NO_RI_COVERAGE;
            vm.setCurrentRICoverage(numberOfCouponsUsed);

            Set<Long> providerOids = providersList.get(oid);
            List<SMATemplate> providers = new ArrayList<>();
            if (providerOids == null) {
                logger.error("updateVirtualMachines: could not find providers for VM ID={} name={}",
                    oid, name);
                vm.setNaturalTemplate(vm.getCurrentTemplate());
            } else {
                for (long providerId : providerOids) {
                    SMATemplate template = templateMap.get(providerId, context);
                    if (template == null) {
                        logger.error("updateVirtualMachines: VM ID={} name={} could not find providers ID={} in templateMap",
                            oid, name, providerId);      // currently expected
                    } else {
                        providers.add(template);
                    }
                }
                vm.setProviders(providers);
                vm.updateNaturalTemplateAndMinCostProviderPerFamily();
            }
            logger.info("updateVirtualMachines: ID={} name={} currentTemplate={} currentRICoverage={} # providers={}",
                oid, name, currentTemplate, numberOfCouponsUsed, providers.size());
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
     * @param regionToTemplates           map from region to set of templates, to be updated
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
                                    Map<Long, Set<SMATemplate>> regionToTemplates,
                                    MarketPriceTable marketPriceTable
    ) {
        /*
         * Make sure we only look at computeTiers are that are valid for a region.
         * Map from region ID to set of IDs of the compute tiers in that region.
         */
        Map<Long, Set<Long>> regionIdToComputeTierIdsMap =
            computeRegionToComputeTierMap(computeTiers, regionToOsTypeToContext.rowKeySet());

        int numberTemplatesCreated = 0;
        // set of valid regions
        for (Long regionId : regionToOsTypeToContext.rowKeySet()) {
            // set of valid compute tiers in that region.
            Set<Long> tiers = regionIdToComputeTierIdsMap.get(regionId);
            for (TopologyEntityDTO computeTier : computeTiers) {
                boolean created = processComputeTier(computeTier, cloudTopology, cloudCostData,
                    regionToOsTypeToContext, contextToBusinessAccountIds, regionId,
                    computeTierIdToContextToTemplateMap, smaContextToTemplates, regionToTemplates,
                    marketPriceTable);
                if (created) {
                    numberTemplatesCreated++;
                }
            }
        }
        return numberTemplatesCreated;
    }

    /*
     * For a set of compute tiers, compute a map from region ID to compute tier IDs that belong in
     * that region.
     *
     * @param computeTiers set of compute tiers
     * @param validRegionIds set of region IDs that are valid, don't consider any other regions.
     * @return map from region ID to set of compute tier IDs.
     */
    private Map<Long, Set<Long>> computeRegionToComputeTierMap(List<TopologyEntityDTO> computeTiers,
                                                               Set<Long> validRegionIds
    ) {
        /*
         * Compute map from compute tier ID to set of region IDs
         */
        Map<Long, Set<Long>> computeTierIdToRegionIdsMap = new HashMap<>();
        for (TopologyEntityDTO computeTier : computeTiers) {
            long computeTierId = computeTier.getOid();
            Set<Long> regionIds = computeTier.getConnectedEntityListList().stream()
                .filter(connEntity -> connEntity.getConnectedEntityType() == EntityType.REGION_VALUE)
                .filter(connEntity -> validRegionIds.contains(connEntity.getConnectedEntityId()))
                .map(connEntity -> new Long(connEntity.getConnectedEntityId()))
                .collect(Collectors.toSet());
            computeTierIdToRegionIdsMap.put(computeTierId, regionIds);
        }
        // dump what we found
        if (logger.isDebugEnabled()) {
            for (Long tierId : computeTierIdToRegionIdsMap.keySet()) {
                Set<Long> set = computeTierIdToRegionIdsMap.get(tierId);
                logger.debug("computeRegionToComputeTierMap:  computeTierToRegionIdsMap tierID={} regionsIDs={}",
                    tierId, set.stream()
                        .map(regionId -> Long.toString(regionId))
                        .collect(Collectors.joining(",")));
            }
        }
        /*
         * compute map from region Id to set of compute tier IDs
         */
        Map<Long, Set<Long>> regionIdToComputeTierIdsMap = new HashMap<>();
        for (Long computeTierId : computeTierIdToRegionIdsMap.keySet()) {
            for (Long regionId : computeTierIdToRegionIdsMap.get(computeTierId)) {
                Set<Long> computeTierIds = regionIdToComputeTierIdsMap.get(regionId);
                if (computeTierIds == null) {
                    computeTierIds = new HashSet<>();
                    regionIdToComputeTierIdsMap.put(regionId, computeTierIds);
                }
                computeTierIds.add(computeTierId);
            }
        }
        if (logger.isDebugEnabled()) {
            // dump the end result
            for (Long regionId : regionIdToComputeTierIdsMap.keySet()) {
                Set<Long> set = regionIdToComputeTierIdsMap.get(regionId);
                logger.debug("computeRegionToComputeTierMap: regionIdToComputeTierIds regionID={} tiers={}",
                    regionId, set.stream()
                        .map(tierId -> Long.toString(tierId))
                        .collect(Collectors.joining(",")));
            }
        }
        return regionIdToComputeTierIdsMap;
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
     * @param regionIdToTemplates         map from region to set of templates, to be updated
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
                                       Map<Long, Set<SMATemplate>> regionIdToTemplates,
                                       MarketPriceTable marketPriceTable
    ) {
        // TODO: figure out what is the CSP from
        SMACSP csp = SMACSP.AWS;
        long oid = computeTier.getOid();
        String name = computeTier.getDisplayName();
        if (computeTier.getEntityType() != EntityType.COMPUTE_TIER_VALUE) {
            logger.error("processComputeTier: entity ID={} name={} is not a computeTier: ",
                oid, name);
            return false;
        }
        Map<OSType, Set<SMAContext>> map = regionToOsTypeToContext.row(regionId);
        if (CollectionUtils.isEmpty(map)) {
            logger.info("processComputeTier: ID={} name={} in regionId={} where there is no osType, skip!",
                oid, name, regionId);
            return false;
        }
        if (!computeTier.getTypeSpecificInfo().hasComputeTier()) {
            logger.error("createSMATemplate: entity ID={} name={} doesn't have ComputeTierInfo",
                oid, name);
            return false;
        }
        ComputeTierInfo computeTierInfo = computeTier.getTypeSpecificInfo().getComputeTier();
        if (CollectionUtils.isEmpty(regionToOsTypeToContext.row(regionId))) {
            logger.debug("processComputeTier: ID={} name={} in regionId={} where there are no VMs, skip!",
                oid, name, regionId);
            return false;
        }
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
                if (context.getCsp() != csp) {
                    logger.debug("processComputeTier: csp={} != context.csp={} for {}",
                        csp.name(), context.getCsp().name(), context);
                }
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
                updateRegionToTemplates(regionIdToTemplates, regionId, template);
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
     * @param regionToReservedInstances map from context to set of RIs, to be updated
     * @return true if RI is created
     */
    private boolean processReservedInstance(ReservedInstanceData data,
                                            CloudTopology<TopologyEntityDTO> cloudTopology,
                                            Table<Long, SMAContext, SMATemplate> templateMap,
                                            Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext,
                                            Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs,
                                            Map<Long, Set<SMAReservedInstance>> regionToReservedInstances
    ) {
        // TODO: figure out correct CSP.
        SMACSP csp = SMACSP.AWS;
        ReservedInstanceBought riBought = data.getReservedInstanceBought();
        ReservedInstanceBoughtInfo riBoughtInfo = riBought.getReservedInstanceBoughtInfo();
        long businessAccountId = riBoughtInfo.getBusinessAccountId();
        long masterAccountId = getMasterAccountId(businessAccountId, cloudTopology, "RI");
        String name = riBoughtInfo.getProbeReservedInstanceId();  // getDisplayName();
        long zoneId = riBoughtInfo.getAvailabilityZoneId();
        zoneId = (zoneId == 0 ? SMAUtils.NO_ZONE : zoneId);
        int count = riBoughtInfo.getNumBought();
        ReservedInstanceBoughtCost boughtCost = riBoughtInfo.getReservedInstanceBoughtCost();
        float utilization = computeRIUtilization(riBoughtInfo.getReservedInstanceBoughtCoupons());

        ReservedInstanceSpec riSpec = data.getReservedInstanceSpec();
        long oid = riSpec.getId();
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

        boolean found = contextExists(regionToOsTypeToContext, csp, masterAccountId, regionId, osType, tenancy);
        if (found == false) {
            logger.info("no context CSP={} billingAccountId={} regionId={} OSType={} Tenancy={} for RI name={}",
                csp.name(), masterAccountId, regionId, osType.name(), tenancy.name(), name);
            return false;
        }

        SMAContext context = new SMAContext(csp, osType, regionId, masterAccountId, tenancy);
        long templateId = riSpecInfo.getTierId();
        SMATemplate template = templateMap.get(templateId, context);
        if (template == null) {
            // should be ERROR
            logger.debug("processReservedInstance: can't find template with ID={} in templateMap for RI ID={} name={}",
                    templateId, oid, name);
            template = SMAUtils.BOGUS_TEMPLATE;
        }
        String templateName = template.getName();
        logger.debug("processReservedInstance: ID={} name={} template={} new SMAcontext: csp={} osType={} regionId={} masterAccountId={} tenancy={}",
            oid, name, templateName, csp, osType, regionId, masterAccountId, tenancy);

         SMAReservedInstance ri = new SMAReservedInstance(oid,
            name,
            businessAccountId,
            utilization,
            template,
            zoneId,
            count,
            context);
        if (ri == null) {
            logger.info("processReservedInstance: regionId={} template={} new SMA_RI FAILED: oid={} name={} accountId={} template={} zondId={} OS={} tenancy={} count={} utilization={} rate RI={} on-demand={}",
                regionId, templateName, oid, name, businessAccountId, templateName, zoneId, osType.name(),
                tenancy.name(), count, utilization, riRate);
        } else {
            logger.info("processReservedInstance: regionId={} template={} SMARI: oid={} name={} accountId={} template={} zondId={} OS={} tenancy={} count={} utilization={} rate RI={} on-demand={}",
                regionId, templateName, oid, name, businessAccountId, templateName, zoneId, osType.name(),
                tenancy.name(), count, utilization, riRate);
            updateRegionToReservedInstance(regionToReservedInstances, regionId, ri);
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
     * @param csp cloud service provider
     * @param masterAccountId master (billing) accountID
     * @param regionId region ID
     * @param osType OS
     * @param tenancy Tenancy
     * @return true if exists.
     */
    private boolean contextExists(Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext, SMACSP csp,
                                  long masterAccountId, long regionId, OSType osType, Tenancy tenancy) {
        Set<SMAContext> contexts = regionToOsTypeToContext.get(regionId, osType);
        if (contexts == null) {
            return false;
        }
        boolean found = false;
        // verify  exists a context with CSP, billing account and tenancy.
        for (SMAContext context : contexts) {
            if (context.getCsp() == csp &&
                context.getTenancy() == tenancy &&
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

    private float computeRIUtilization(ReservedInstanceBoughtCoupons boughtCoupons) {
        int numberOfCoupons = boughtCoupons.getNumberOfCoupons();
        double numberOfCouponsUsed = boughtCoupons.getNumberOfCouponsUsed();
        return (float)(numberOfCouponsUsed / (float)numberOfCoupons);
    }

    /**
     * Compute the RI coverage of a VM.
     * @param entity         virtualMachine
     * @param cloudTopology  the source cloud topology to get a VM's the source compute tier.
     * @param cloudCostData  cost dictionary
     * @return RI coverage of the VM
     */
    private float computeRiCoverage(TopologyEntityDTO entity,
                                    CloudTopology<TopologyEntityDTO> cloudTopology,
                                    CloudCostData cloudCostData
    ) {
        long oid = entity.getOid();
        String name = entity.getDisplayName();
        float currentRICoverage = 0.0f;
        Optional<EntityReservedInstanceCoverage> coverageOptional = cloudCostData.getRiCoverageForEntity(oid);
        if (coverageOptional.isPresent()) {
            EntityReservedInstanceCoverage coverage = coverageOptional.get();
            currentRICoverage = computeCoverage(coverage);
        } else {
            logger.error("processVirtualMachine: could not coverage VM ID={}",
                oid);
        }
        logger.info("processVirtualMachine: ID={} name={} cloudCostData currentRICoverage={}",
            oid, name, currentRICoverage);
        return currentRICoverage;
    }

    private float computeCoverage(EntityReservedInstanceCoverage riCoverage) {
        Map<Long, Double> riToCoupons = riCoverage.getCouponsCoveredByRi();
        double utilization = 0.0;
        for (double coupons : riToCoupons.values()) {
            utilization += coupons;
        }
        return (float) utilization;
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

    /*
     * Help with verification
     */
    private void updateRegionToVirtualMachines(Map<Long, Set<SMAVirtualMachine>> map,
                                               long regionId,
                                               SMAVirtualMachine vm) {
        Set set = map.get(regionId);
        if (set == null) {
            set = new HashSet<SMAVirtualMachine>();
            map.put(regionId, set);
        }
        set.add(vm);
    }

    private void updateRegionToReservedInstance(Map<Long, Set<SMAReservedInstance>> map,
                                                long regionId,
                                                SMAReservedInstance reservedInstance) {
        Set set = map.get(regionId);
        if (set == null) {
            set = new HashSet<SMAVirtualMachine>();
            map.put(regionId, set);
        }
        set.add(reservedInstance);
    }

    private void updateRegionToTemplates(Map<Long, Set<SMATemplate>> map,
                                                 long regionId,
                                                 SMATemplate template) {
        Set set = map.get(regionId);
        if (set == null) {
            set = new HashSet<SMATemplate>();
            map.put(regionId, set);
        }
        set.add(template);
    }

    /*
     * Debugging code
     */
    /**
     * Dump out VMs by context. Must be called after call to updateVirtualMachines.
     *
     * @param smaContextToVMs context to VM map
     * @param nVMsCreated     number of VMs created
     */
    private void dumpVMsByContext(Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs, int nVMsCreated) {
        logger.info("found {} VMs in {} contexts", nVMsCreated, smaContextToVMs.keySet().size());
        for (SMAContext context : smaContextToVMs.keySet()) {
            logger.info("context: masterAccount={} region={} os={} tenancy={} #VMs={}",
                context.getBillingAccountId(), context.getRegionId(), context.getOs().name(),
                context.getTenancy().name(), smaContextToVMs.get(context).size());
            for (SMAVirtualMachine vm : smaContextToVMs.get(context)) {
                logger.info("   VM: ID={} name={} template={}", vm.getOid(), vm.getName(),
                    vm.getCurrentTemplate().getName());
            }
        }
    }

    private void dumpRegionToOsTypeToContext(Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext) {
        logger.info("dump region to osType to context table for {} regions",
            regionToOsTypeToContext.rowKeySet().size());
        for (long regionId : regionToOsTypeToContext.rowKeySet()) {
            logger.info("  region={}", regionId);
            Map<OSType, Set<SMAContext>> map = regionToOsTypeToContext.row(regionId);
            for (OSType  osType : map.keySet()) {
                logger.info("    osType={} contexts={}", osType, map.get(osType));
            }
        }
    }

    private void dumpVMsByRegion(Map<Long, Set<SMAVirtualMachine>> regionToVirtualMachines) {
        logger.info("dumpVMsByRegion: for {} region, dump the VMs", regionToVirtualMachines.keySet().size());
        for (long regionId : regionToVirtualMachines.keySet()) {
            logger.info("dumpVMsByRegion: region={} size={}", regionId,
                regionToVirtualMachines.get(regionId).size());
            for (SMAVirtualMachine vm : regionToVirtualMachines.get(regionId)) {
                logger.debug("dumpVMsByRegion:   VM ID={} name={}", vm.getOid(), vm.getName());
            }
        }
    }

    private void dumpRIsByRegion(Map<Long, Set<SMAReservedInstance>> regionToReservedInstances) {
        logger.info("dumpRIsByRegion: for {} regions, dump the RIs", regionToReservedInstances.keySet().size());
        for (long regionId : regionToReservedInstances.keySet()) {
            logger.info("dumpRIsByRegion: region={} size={}", regionId,
                regionToReservedInstances.get(regionId).size());
            for (SMAReservedInstance ri : regionToReservedInstances.get(regionId)) {
                logger.info("dumpRIsByRegion:   RI ID={} name={} template={}", ri.getOid(),
                    ri.getName(), ri.getTemplate().getName());
            }
        }
    }

    private void dumpTemplatesByRegion(Map<Long, Set<SMATemplate>> regionToTemplates) {
        logger.info("dumpTemplatesByRegion: for {} regions, dump the templates", regionToTemplates.keySet().size());
        for (long regionId : regionToTemplates.keySet()) {
            logger.info("dumpTemplatesByRegion: region={} size={}", regionId,
                regionToTemplates.get(regionId).size());
            for (SMATemplate template : regionToTemplates.get(regionId)) {
                logger.debug("dumpTemplatesByRegion:   Template ID={} name={}", template.getOid(), template.getName());
            }
        }
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
}
