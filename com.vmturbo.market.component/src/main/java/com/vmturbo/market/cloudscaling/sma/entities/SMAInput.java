package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.Collections;
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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

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
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry.LicensePrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;

/**
 * The input to the Stable marriage algorithm.
 * Integration with XL:
 *   add a constructor to convert market data structures into SMA data structures.
 */
public class SMAInput {

    private static final Logger logger = LogManager.getLogger();

    /**
     * List of input contexts.
     */
    public final List<SMAInputContext> inputContexts;

    /*
     * Dictionary for SMACSP by region.
     */
    private CspFromRegion cspFromRegion = new CspFromRegion();

    /*
     * if Azure, then OS type agnostic and use UNKNOWN_OS
     */
    private static final Set<OSType> UNKNOWN_OS_SINGLETON_SET = Collections.singleton(OSType.UNKNOWN_OS);

    // map to convert commodity sold key to OSType.
    static Map<String, OSType> nameToOSType = new HashMap<>();


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
     * @param marketPriceTable used to figure out the discounts for business accounts
     * @param consistentScalingHelper used to figure out the consistent scaling information.
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

        /*
         * maps from SMAContext to entities.  Needed to build SMAInputContexts.
         * The OSType in the context is OSTypeForContext, which is UNKNOWN for Azure.
         */
        Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs = new HashMap<>();
        Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs = new HashMap<>();
        Map<SMAContext, Set<SMATemplate>> smaContextToTemplates = new HashMap<>();

        // data encapsulation of RI key ID generation.
        SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator =
                new SMAReservedInstanceKeyIDGenerator();
        /*
         * Map from computeTier OID to context to template, needed to convert provider list from
         * compute tier to template.  Need context, because their is a one to many relationship
         * between a compute tier and templates, one template per context for a compute tier.
         * The context is what is used to aggregate VMs, RIs, and Templates.
         */
        Table<Long, SMAContext, SMATemplate> computeTierOidToContextToTemplate = HashBasedTable.create();
        // Map from region ID to OSType to context: used to restrict compute tier and RI generation.
        Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts = HashBasedTable.create();
        // keep track of business accounts in the context's billing family.  Needed to process compute tiers.
        Map<SMAContext, Set<Long>> contextToBusinessAccountIds = new HashMap<>();


        /*
         * For each virtual machines, create a VirtualMachine and partition into SMAContexts.
         */
        List<TopologyEntityDTO> virtualMachines =
            cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE);
        int numberVMsCreated = 0;
        for (TopologyEntityDTO vm : virtualMachines) {
            if (!(vm.getEntityState() == EntityState.POWERED_ON)) {
                logger.debug(" VM={} state={} != POWERED_ON", vm.getOid(), vm.getEntityState().name());
                continue;
            }
            numberVMsCreated++;
            processVirtualMachine(vm, cloudTopology, cloudCostData, smaContextToVMs,
                regionIdToOsTypeToContexts, contextToBusinessAccountIds,
                    consistentScalingHelper);
        }
        logger.info("created {} VMs from {} VirtualMachines in {} contexts", numberVMsCreated,
            virtualMachines.size(), smaContextToVMs.keySet().size());
        dumpContextToVMs(smaContextToVMs);
        dumpRegionIdToOsTypeToContexts(regionIdToOsTypeToContexts);
        dumpContextToBusinessAccountsIds(contextToBusinessAccountIds);

        /*
         * For each ComputeTier, generate SMATemplates, but only in contexts where VMs exist.
         */
        final List<TopologyEntityDTO> computeTiers =
            cloudTopology.getAllEntitiesOfType(EntityType.COMPUTE_TIER_VALUE);
        int numberTemplatesCreated = processComputeTiers(computeTiers, cloudTopology, cloudCostData,
            regionIdToOsTypeToContexts, contextToBusinessAccountIds, computeTierOidToContextToTemplate,
            smaContextToTemplates, marketPriceTable);
        logger.info("created {} templates from {} compute tiers in {} contexts", numberTemplatesCreated,
            computeTiers.size(), smaContextToTemplates.keySet().size());
        dumpSmaContextsToTemplates(smaContextToTemplates);
        dumpComputeTierOidToContextToTemplate(computeTierOidToContextToTemplate);

        /*
         * For each the RI, create an  and  into SMAContexts.
         */
        int numberRIsCreated = 0;
        for (ReservedInstanceData data : cloudCostData.getAllRiBought()) {
            if (processReservedInstance(data, cloudTopology, computeTierOidToContextToTemplate,
                regionIdToOsTypeToContexts, smaContextToRIs,
                    reservedInstanceKeyIDGenerator)) {
                numberRIsCreated++;
            }
        }
        logger.info("created {} RIs from {} RI bought data in {} contexts", numberRIsCreated,
            cloudCostData.getAllRiBought().size(), smaContextToRIs.keySet().size());
        dumpSmaContextsToRIs(smaContextToRIs);

        /*
         * Update VM's current template and provider list.
         */
        for (SMAContext context : smaContextToVMs.keySet()) {
            Set<SMAVirtualMachine> vmsInContext = smaContextToVMs.get(context);
            updateVirtualMachines(vmsInContext, computeTierOidToContextToTemplate, providers,
                    cloudTopology, context,
                    reservedInstanceKeyIDGenerator, cloudCostData);
        }
        dumpContextToVMsFinal(smaContextToVMs);

        /*
         * build input contexts.
         */
        Set<SMAContext> smaContexts = smaContextToVMs.keySet();
        inputContexts = new ArrayList<>();
        for (SMAContext context : smaContexts) {
            Set<SMAVirtualMachine> smaVMs = smaContextToVMs.get(context);
            if (ObjectUtils.isEmpty(smaVMs)) {
                // there may be a RI context that has no VMs.
                logger.error(" no VM for context={}", context);
                continue;
            }
            Set<SMAReservedInstance> smaRIs = smaContextToRIs.getOrDefault(context, Collections.emptySet());
            Set<SMATemplate> smaTemplates = smaContextToTemplates.get(context);
            if (ObjectUtils.isEmpty(smaTemplates)) {
                logger.error(" no template for context={}", context);
                continue;
            }
            logger.info(": context={} #VMs={} #RIs={} #templates={}",
                context, smaVMs.size(), smaRIs.size(), smaTemplates.size());
            SMAInputContext inputContext = new SMAInputContext(context,
                Lists.newArrayList(smaVMs),
                (smaRIs == null ? new ArrayList<>() : Lists.newArrayList(smaRIs)),
                Lists.newArrayList(smaTemplates));
            inputContexts.add(inputContext);
        }
    }

    /**
     * Create a SMA Virtual Machine and SMA context from a VM topology entity DTO.
     * Because scale actions do not modify region, osType or tenancy, the regionsToOsTypeToContext
     * keeps track of the what osTypes and contexts that are in a region.
     *  @param entity                     topology entity DTO that is a VM.
     * @param cloudTopology               the cloud topology to find source template.
     * @param cloudCostData               where to find costs and RI related info.  E.g. RI coverage for a VM.
     * @param smaContextToVMs             map from SMA context to set of SMA virtual machines, to be updated
     * @param regionIdToOsTypeToContexts  table from region ID  to osType to set of SMAContexts, defined
     * @param contextToBusinessAccountIds map from context to set of business account IDs, defined.
     * @param consistentScalingHelper     used to figure out the consistent scaling information.
     */
    private void processVirtualMachine(@Nonnull TopologyEntityDTO entity,
                                       @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                       @Nonnull CloudCostData cloudCostData,
                                       Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs,
                                       Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts,
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
        String tenancyName = vmInfo.getTenancy().name();
        Tenancy tenancy = Tenancy.valueOf(tenancyName);
        String osName = vmInfo.getGuestOsInfo().getGuestOsType().name();
        OSType osType = OSType.valueOf(osName);
        if (osType == OSType.UNKNOWN_OS && !osName.equalsIgnoreCase(OSType.UNKNOWN_OS.name())) {
            logger.warn("processVirtualMachine: osName={} converted to UNKNOWN_OS", osName);
        }
        /*
         * create Context
         */

        long regionId = getVMRegionId(oid, cloudTopology);
        SMACSP csp = cspFromRegion.lookupWithRegionId(regionId);
        Optional<TopologyEntityDTO> zoneOptional = cloudTopology.getConnectedAvailabilityZone(oid);
        long zoneId = SMAUtils.NO_ZONE;
        if (zoneOptional.isPresent()) {
            zoneId = zoneOptional.get().getOid();
        } else if (csp != SMACSP.AZURE) {
            // Azure currently does not support availability zones.
            logger.error("processVirtualMachine: VM ID={} can't find availabilty zone",
                oid);
        }

        // Because RIs are OS agnostic in Azure, if Azure, use OSType UNKNOWN.
        OSType osTypeForContext = (csp == SMACSP.AZURE ? OSType.UNKNOWN_OS : osType);
        long businessAccountId = getBusinessAccountId(oid, cloudTopology, "VM");
        long masterAccountId = getMasterAccountId(businessAccountId, cloudTopology, "VM");
        logger.info("processVirtualMachine: new SMAContext csp={} osType={} region={} masterAccount={} account={} tenancy={}",
            csp.name(), osTypeForContext, regionId, masterAccountId, businessAccountId, tenancy);
        SMAContext context = new SMAContext(csp, osTypeForContext, regionId, masterAccountId, tenancy);

        // Add business account to context.
        Set<Long> accounts = contextToBusinessAccountIds.getOrDefault(context, new HashSet<>());
        accounts.add(businessAccountId);
        contextToBusinessAccountIds.put(context, accounts);

        Optional<String> groupIdOptional = consistentScalingHelper.getScalingGroupId(oid);
        String groupId = SMAUtils.NO_GROUP_ID;
        if (groupIdOptional.isPresent()) {
            groupId = groupIdOptional.get();
        }
        /*
         * Create Virtual Machine.
         */
        SMAVirtualMachine vm = new SMAVirtualMachine(oid,
            name,
            groupId,
            businessAccountId,
            null,
            new ArrayList<SMATemplate>(),
            SMAUtils.NO_RI_COVERAGE,
            zoneId,
            SMAUtils.NO_CURRENT_RI,
            osType);
        if (vm == null) {
            logger.error("processVirtualMachine: createSMAVirtualMachine failed for VM ID={}", vm.getOid());
            return;
        }
        Set<SMAVirtualMachine> contextVMs = smaContextToVMs.getOrDefault(context, new HashSet<>());
        contextVMs.add(vm);
        smaContextToVMs.put(context, contextVMs);
        // Update table from region to osType, to Set of contexts
        updateRegionIdToOsTypeToContexts(regionIdToOsTypeToContexts, regionId, osTypeForContext, context);
    }

    /**
     * For each VM, updates its current template, currentRICoverage, and providers.
     * This must be run after the compute tiers are processed and the SMATemplates are created.
     *
     * @param vms the set of VMs that must be updated.
     * @param computeTierOidToContextToTemplate map from computeTier ID to context to template.
     * @param providersList map from VM ID to its set of computeTier IDs.
     * @param cloudTopology dictionary.
     * @param context the input context the VM belongs to.
     * @param reservedInstanceKeyIDGenerator ID generator for ReservedInstanceKey
     * @param cloudCostData             where to find costs and RI related info.  E.g. RI coverage for a VM.
     */
    private void updateVirtualMachines(Set<SMAVirtualMachine> vms,
                                       Table<Long, SMAContext, SMATemplate> computeTierOidToContextToTemplate,
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
                currentTemplate = computeTierOidToContextToTemplate.get(computeTierID, context);
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
                    SMATemplate template = computeTierOidToContextToTemplate.get(providerId, context);
                    if (template == null) {
                        logger.error("updateVirtualMachines: VM ID={} name={} could not find providerID={} in computeTierToContextToTemplateMap with context={}",
                            oid, name, providerId, context);      // currently expected
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
     * @param computeTiers                Set of compute tiers in this cloud topology.
     * @param cloudTopology               cloud topology dictionary.
     * @param cloudCostData               cost dictionary.
     * @param regionIdToOsTypeToContexts  table from region ID to osType to set of contexts.
     * @param contextToBusinessAccountIds map from context to set of business accounts in this context
     * @param computeTierIdToContextToTemplateMap compute tier ID to context to Template map, to be updated
     * @param smaContextToTemplates       map from context to template, to be updated
     * @param marketPriceTable            price table to compute on-demand cost.
     * @return true if a template is created, else false
     */
    private int processComputeTiers(List<TopologyEntityDTO> computeTiers,
                                    CloudTopology<TopologyEntityDTO> cloudTopology,
                                    CloudCostData cloudCostData,
                                    Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts,
                                    Map<SMAContext, Set<Long>> contextToBusinessAccountIds,
                                    Table<Long, SMAContext, SMATemplate> computeTierIdToContextToTemplateMap,
                                    Map<SMAContext, Set<SMATemplate>> smaContextToTemplates,
                                    MarketPriceTable marketPriceTable) {
        int numberTemplatesCreated = 0;
        // set of valid regions
        for (Long regionId : regionIdToOsTypeToContexts.rowKeySet()) {
            // set of valid compute tiers in that region.
            for (TopologyEntityDTO computeTier : computeTiers) {
                boolean created = processComputeTier(computeTier, cloudTopology, cloudCostData,
                    regionIdToOsTypeToContexts, contextToBusinessAccountIds, regionId,
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
     * Given an topology entity that is a compute tier, generate one or more SMATemplates.
     * A compute tier does not specify either a tenancy, a master or a business account.
     * A compute tier may support multiple OSTypes, which are found as sold commodities.
     * A compute tier may not be supported in a region, which can be determined by checking the
     * compute tier's connected entities.
     * Only generate SMATemplates that are needed by the Virtual machines that may be scaled.
     * For Azure, do not generate templates for different OSTypes, because Azure RIs are OS agnostic.
     *
     * @param computeTier                   Entity that is expected to be a computeTier.
     * @param cloudTopology                 entity dictionary.
     * @param cloudCostData                 cost dictionary.
     * @param regionIdToOsTypeToContexts    table from region ID to osTypeForCSP to set of contexts.
     * @param contextToBusinessAccountIds   set of business accounts in a context's billing family.
     * @param regionId                      the ID of the region we are in.
     * @param tierOidToContextToTemplateMap computeTier ID to context to template map, to be updated
     * @param smaContextToTemplates         map from context to template, to be updated
     * @param marketPriceTable              price table to compute on-demand cost.
     * @return true if a template is created, else false
     */
    private boolean processComputeTier(TopologyEntityDTO computeTier,
                                       CloudTopology<TopologyEntityDTO> cloudTopology,
                                       CloudCostData cloudCostData,
                                       Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts,
                                       Map<SMAContext, Set<Long>> contextToBusinessAccountIds,
                                       long regionId,
                                       Table<Long, SMAContext, SMATemplate> tierOidToContextToTemplateMap,
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
        SMACSP csp = cspFromRegion.lookupWithRegionId(regionId);

        /*
         * Find all the contexts in which this template belongs.  Iterate threw osTypes and lookup
         * contexts.  Because Azure is OS agnostic, templates do not distinguish by OS Type, but us
         * UNKNOWN_OS as place holder.
         */
        Set<OSType> osTypes = computeOsTypes(computeTier);
        Set<OSType> osTypesForContext = UNKNOWN_OS_SINGLETON_SET;
        if (csp != SMACSP.AZURE) {
            osTypesForContext = osTypes;
        }
        for (OSType  osType : osTypesForContext) {
            Set<SMAContext> contexts = regionIdToOsTypeToContexts.get(regionId, osType);
            if (CollectionUtils.isEmpty(contexts)) {
                logger.trace("processComputeTier: ID={} name={}: regionId={} osType={} NOT found in regionToOsTypeToContext",
                    oid, name, regionId, osType.name(), regionId);
                continue;
            }
            logger.debug("processComputeTier: ID={} name={}: regionId={} osType={} found in regionToOsTypeToContext.",
                oid, name, regionId, osType.name());

            for (SMAContext context: contexts) {
                /*
                 * For each osType create template with osType specific cost.
                 */
                SMATemplate template = new SMATemplate(oid, name, family, coupons, context, computeTier);
                Set<SMATemplate> templates = smaContextToTemplates.getOrDefault(context, new HashSet<>());
                templates.add(template);
                smaContextToTemplates.put(context, templates);
                /*
                 * For each business account in this context's master account, compute cost.
                 */
                for (long businessAccountId: contextToBusinessAccountIds.get(context)) {
                    if (csp == SMACSP.AZURE) {
                        for (OSType osTypeInner: osTypes) {
                            updateTemplateRate(template, osTypeInner, regionId, businessAccountId, cloudCostData,
                                cloudTopology, marketPriceTable);
                        }
                    } else {
                        updateTemplateRate(template, osType, regionId, businessAccountId, cloudCostData,
                            cloudTopology, marketPriceTable);
                    }
                }
                logger.debug("processComputeTier: region={} new SMATemplate: oid={} name={} family={} coupons={}",
                        regionId, oid, name, family, coupons);
                tierOidToContextToTemplateMap.put(oid, context, template);
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
        return nameToOSType.getOrDefault(value.toUpperCase(), OSType.UNKNOWN_OS);
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
            logger.debug("updateTemplateRate: template ID={} name={} doesn't have pricingData for accountId={}",
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
                logger.error("updateTemplateRate: could not find hourlyRate for tier ID={}:name={} in accountId={} regionId={} osType={}",
                    oid, name, businessAccountId, regionId, osType.name());
            } else if (osType != OSType.UNKNOWN_OS) {
                logger.debug("Template on-demand Rate={}: name={} ID={}  accountId={} regionId={} osType={}",
                    hourlyRate, name, oid, businessAccountId, regionId, osType.name());
            }
            template.setOnDemandCost(businessAccountId, osType, new SMACost((float)hourlyRate, 0f));

            /*
             * compute discounted costs
             * For AWS, there are no discounted costs.
             * Accessing the accounting price data is not working.
            Optional<Integer> numCoresOptional = marketPriceTable.getComputeTierNumCores(template.getComputeTier());
            if (numCoresOptional.isPresent()) {
                logger.trace("updateTemplateRate() number of cores {} for template OID={} name={}",
                    template.getOid(), template.getName(), numCoresOptional.get());
                Optional<LicensePrice> riLicensePriceOptional = accountPricingData.getReservedLicensePrice(osType,
                    numCoresOptional.get(), false);
                if (riLicensePriceOptional.isPresent()) {
                    LicensePrice riLicensePrice = riLicensePriceOptional.get();
                    Price price = riLicensePrice.getPrice();
                    if (price != null) {
                        template.setDiscountedCost(businessAccountId, osType,
                            new SMACost(0.0f, (long)price.getPriceAmount().getAmount()));
                        return;
                    }
                }
            }*/
            template.setDiscountedCost(businessAccountId, osType, SMAUtils.zeroCost);
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
     * @param computeTierOidToContextToTemplate used to look up SMATemplate given the computeTier ID
     * @param regionIdToOsTypeToContexts map from regionID to OSType to context.
     * @param smaContextToRIs map from context to set of RIs, to be updated
     * @param reservedInstanceKeyIDGenerator ID generator for ReservedInstanceKey
     * @return true if RI is created
     */
    private boolean processReservedInstance(ReservedInstanceData data,
                                            CloudTopology<TopologyEntityDTO> cloudTopology,
                                            Table<Long, SMAContext, SMATemplate> computeTierOidToContextToTemplate,
                                            Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts,
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
        long riBoughtId = riBought.getId();
        ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();
        // Can't find template until after processComputeTier

        ReservedInstanceType type = riSpecInfo.getType();
        int years = type.getTermYears();
        double riRate = computeHourlyRIRate(boughtCost, years);
        String tenancyName = riSpecInfo.getTenancy().name();
        Tenancy tenancy = Tenancy.valueOf(tenancyName);
        String osTypeName = riSpecInfo.getOs().name();
        // for Azure RIs, OSType is always UNKNOWN, because the RI is OSType agnostic.
        OSType  osType = OSType.valueOf(osTypeName);
        long regionId = riSpecInfo.getRegionId();
        SMACSP csp = cspFromRegion.lookupWithRegionId(regionId);
        OSType osTypeForContext = osType;
        if (csp == SMACSP.AZURE) {
            osTypeForContext = OSType.UNKNOWN_OS;
        }

        boolean found = contextExists(regionIdToOsTypeToContexts, masterAccountId, regionId, osType, tenancy);
        if (found == false) {
            logger.info("no context billingAccountId={} regionId={} OSType={} Tenancy={} for RI name={}",
                 masterAccountId, regionId, osType.name(), tenancy.name(), name);
            return false;
        }

        SMAContext context = new SMAContext(csp, osTypeForContext, regionId, masterAccountId, tenancy);
        long computeTierOid = riSpecInfo.getTierId();
        // for Azure, this RI covers a set of templates, one for each OSType.
        SMATemplate template = computeTierOidToContextToTemplate.get(computeTierOid, context);
        if (template == null) {
            logger.error("processReservedInstance: can't find template with ID={} in templateMap for RI boughtID={} name={}",
                    computeTierOid, riBoughtId, name);
            template = SMAUtils.BOGUS_TEMPLATE;
        }
        ReservedInstanceKey reservedInstanceKey = new ReservedInstanceKey(data,
                template.getFamily());
        long riKeyId = reservedInstanceKeyIDGenerator.lookUpRIKey(reservedInstanceKey, riBoughtId);
        String templateName = template.getName();
        SMAReservedInstance ri = new SMAReservedInstance(riBoughtId,
            riKeyId,
            name,
            businessAccountId,
            template,
            zoneId,
            count,
            riSpecInfo.getSizeFlexible());
        if (ri == null) {
            logger.info("processReservedInstance: regionId={} template={} new SMA_RI FAILED: oid={} name={} accountId={} template={} zondId={} OS={} tenancy={} count={} rate RI={}",
                regionId, templateName, riBoughtId, name, businessAccountId, templateName, zoneId,
                osType.name(), tenancy.name(), count, riRate);
        } else {
            logger.info("processReservedInstance: {} riRate={} IPF={} {}", ri, riRate, riSpecInfo.getPlatformFlexible(), context);
            Set<SMAReservedInstance> smaRIs = smaContextToRIs.getOrDefault(context, new HashSet<>());
            smaRIs.add(ri);
            smaContextToRIs.put(context, smaRIs);
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
     * Side effect: update cspFromRegion with region.
     *
     * @param oid           ID  of topology entity
     * @param cloudTopology dictionary of cloud topoolgy
     * @return region ID
     */
    private long getVMRegionId(long oid,
                               @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
        long regionId = -1;
        Optional<TopologyEntityDTO> regionOpt = cloudTopology.getConnectedRegion(oid);
        if (!regionOpt.isPresent()) {
            logger.error("getRegionID: can't find region for VM ID={}", oid);
        } else {
            TopologyEntityDTO region = regionOpt.get();
            regionId = region.getOid();
            cspFromRegion.updateWithRegion(region);

        }
        return regionId;
    }

    /**
     * Find the business account.
     *
     * @param oid           ID  of topology entity
     * @param cloudTopology where to look for the business account
     * @param msg           who is
     * @return master account ID
     */
    private long getBusinessAccountId(long oid,
                                      @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                      String msg) {
        long businessAccont = -1;
        Optional<TopologyEntityDTO> accountOpt = cloudTopology.getOwner(oid);
        if (!accountOpt.isPresent()) {
            logger.error("getBusinessAccount{}: can't find business account for ID={}", msg, oid);
        } else {
            businessAccont = accountOpt.get().getOid();
        }
        return businessAccont;
    }

    /**
     * Given a business acount, find the master account.
     * In AWS, the master account is the billing family.
     * In Azure, the master account is the subscription.
     *
     * @param oid           business account OID.
     * @param cloudTopology dictionary of cloud topology
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
     * @param vmOid         oid of virtualMachine
     * @param cloudTopology  the source cloud topology to get a VM's the source compute tier.
     * @param cloudCostData  cost dictionary
     * @param reservedInstanceKeyIDGenerator ID generator for ReservedInstanceKey
     * @return RI coverage of the VM
     */
    @Nonnull
    private Pair<Long, Float> computeRiCoverage(Long vmOid,
                                    CloudTopology<TopologyEntityDTO> cloudTopology,
                                    CloudCostData cloudCostData,
                                    SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator) {
        Pair<Long, Float> currentRICoverage = new Pair(SMAUtils.NO_CURRENT_RI, SMAUtils.NO_RI_COVERAGE);
        Optional<EntityReservedInstanceCoverage> coverageOptional = cloudCostData.getRiCoverageForEntity(vmOid);
        if (coverageOptional.isPresent()) {
            EntityReservedInstanceCoverage coverage = coverageOptional.get();
            currentRICoverage = computeCoverage(coverage, reservedInstanceKeyIDGenerator);
        } else {
            logger.error("processVirtualMachine: could not coverage VM ID={}",
                vmOid);
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
                        .getRIKeyIDFromRIBoughtID(coupons.getKey());
            }
        }
        return new Pair(riKeyID, utilization);
    }

    private double computeHourlyRIRate(ReservedInstanceBoughtCost boughtCost, int years) {
        double fixedCost = boughtCost.getFixedCost().getAmount();
        double recurringCostPerHour = boughtCost.getRecurringCostPerHour().getAmount();
        double usageCostPerHour = boughtCost.getUsageCostPerHour().getAmount();
        double amortizedFixedCost = fixedCost / (float)(years * 365 * 24);
        return amortizedFixedCost + recurringCostPerHour + usageCostPerHour;
    }

    /**
     * Update regionToOsTypeToContexts table.
     *
     * @param regionIdToOsTypeToContexts table to capture the relationship from region to osType to set of contexts.
     * @param regionId region ID.
     * @param osType osType.
     * @param context context.
     */
    private void updateRegionIdToOsTypeToContexts(Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts,
                                                long regionId, OSType  osType, SMAContext context) {
        Map<OSType, Set<SMAContext>> map = (Map<OSType, Set<SMAContext>>)regionIdToOsTypeToContexts.row(regionId);
        if (map == null) {
            map = new HashMap<>();
            regionIdToOsTypeToContexts.put(regionId, osType, new HashSet<>());
        }
        Set<SMAContext> contexts = regionIdToOsTypeToContexts.get(regionId, osType);
        if (contexts == null) {
            contexts = new HashSet<>();
            regionIdToOsTypeToContexts.put(regionId, osType, contexts);
        }
        contexts.add(context);
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

    /*
     * Debug code
     */
    private void dumpContextToVMs(Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs) {
        logger.info("dump contexts to VMS for {} contexts", smaContextToVMs.keySet().size());
        for (SMAContext context : smaContextToVMs.keySet()) {
            logger.info("  {}", context);
            for (SMAVirtualMachine vm : smaContextToVMs.get(context)) {
                logger.info("    VM: ID={} name={} businessAccountId={} OS={}", vm.getOid(),
                    vm.getName(), vm.getBusinessAccountId(), vm.getOsType().name());
            }
        }
    }

    private void dumpContextToVMsFinal(Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs) {
        List<SMAVirtualMachine> vms = new ArrayList<>();
        smaContextToVMs.values().forEach(vms::addAll);
        logger.info("dump {} VMs after updated", vms.size());
        for (SMAVirtualMachine vm: vms) {
            logger.info("  {}", vm);
        }
    }

    private void dumpRegionIdToOsTypeToContexts(Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts) {
        logger.info("dump region to osType to context table for {} regions",
            regionIdToOsTypeToContexts.rowKeySet().size());
        for (long regionId : regionIdToOsTypeToContexts.rowKeySet()) {
            logger.info("  region={}", regionId);
            Map<OSType, Set<SMAContext>> map = regionIdToOsTypeToContexts.row(regionId);
            for (OSType  osType : map.keySet()) {
                logger.info("    osType={}", osType);
                for (SMAContext context: map.get(osType)) {
                    logger.info("      {}", context);
                }
            }
        }
    }

    private void dumpContextToBusinessAccountsIds(Map<SMAContext, Set<Long>> contextToBusinessAccountIds) {
        logger.info("dump context to business account IDs for {} contexts",
            contextToBusinessAccountIds.keySet().size());
        for (SMAContext context: contextToBusinessAccountIds.keySet()) {
            Set<Long> accountIds = contextToBusinessAccountIds.get(context);
            StringBuffer buffer = new StringBuffer();
            for (long id: accountIds) {
                buffer.append(" ").append(id);
            }
            logger.info("  {}: account IDs={}", context, buffer.toString());
        }
    }

    private void dumpSmaContextsToTemplates(Map<SMAContext, Set<SMATemplate>> smaContextToTemplates) {
        logger.info("dump context to templates for {} contexts", smaContextToTemplates.keySet().size());
        for (SMAContext context: smaContextToTemplates.keySet()) {
            logger.debug("  context={}", context);
            for (SMATemplate template: smaContextToTemplates.get(context)) {
                logger.debug("     {}", template.toStringWithOutCost());
            }
        }
    }

    private void dumpComputeTierOidToContextToTemplate(Table<Long, SMAContext, SMATemplate>
                                                           computeTierOidToContextToTemplate) {
        if (logger.isTraceEnabled()) {
            logger.trace("dump compute tier OID to context to templates for {} compute tiers",
                computeTierOidToContextToTemplate.rowKeySet().size());
            for (Long computeTierOid : computeTierOidToContextToTemplate.rowKeySet()) {
                logger.trace("  computer tier OID={}", computeTierOid);
                Map<SMAContext, SMATemplate> entry = computeTierOidToContextToTemplate.row(computeTierOid);
                for (SMAContext context : entry.keySet()) {
                    SMATemplate template = entry.get(context);
                    logger.trace("    context={}", context);
                    logger.trace("      template={}", template);
                }
                break;  // only do this once.
            }
        }
    }

    private void dumpSmaContextsToRIs(Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs) {
        logger.info("dump context to RIs for {} contexts", smaContextToRIs.keySet().size());
        for (SMAContext context: smaContextToRIs.keySet()) {
            logger.info("  context={}", context);
            for (SMAReservedInstance ri: smaContextToRIs.get(context)) {
                logger.info("     RI={}", ri);
            }
        }
    }


    /**
     * This class is to generate unique IDs for ReservedInstanceKey, which is used to aggregate
     * RIs together.
     */
    class SMAReservedInstanceKeyIDGenerator {
        // map from ReservedInstanceKey to riKeyID.  This is a one-to-one map.
        private Map<ReservedInstanceKey, Long> riKeyToOid = new HashMap();
        // map from RIBought OID to riKeyID.  Multiple RIBoughtIDs may map to a single riKeyID.
        private Map<Long, Long> riBoughtToRiKeyOID = new HashMap();
        // index for ID generator
        private AtomicLong riKeyIndex = new AtomicLong(0);

        /**
         * Create a unique id for the given ReservedInstanceKey. If id is already generated
         * then return it. Also associate the generated keyIDwith RIBoughtID
         *
         * @param riKey ReservedInstanceKey we are trying to find the riKey id for.
         * @param riBoughtID the id of the RI bought.
         * @return the id corresponding to the ReservedInstanceKey riKey.
         */
        public long lookUpRIKey(ReservedInstanceKey riKey, Long riBoughtID) {
            Long riKeyOID = riKeyToOid.get(riKey);
            if (riKeyOID == null) {
                riKeyOID = riKeyIndex.getAndIncrement();
                riKeyToOid.put(riKey, riKeyOID);
            }
            riBoughtToRiKeyOID.put(riBoughtID, riKeyOID);
            return riKeyOID;
        }

        public Long getRIKeyIDFromRIBoughtID(Long riBoughtID) {
            return riBoughtToRiKeyOID.get(riBoughtID);
        }
    }

    /**
     * Class to data encapsulate a mapping from region ID to SMACSP.
     * This map is needed, because the TopologyEntityDTO does not provide CSP.
     * The code looks at the region's display name to determine the CSP.
     */
    public static class CspFromRegion {
        /*
         * Map from region OID to SMACSP.  Driven by regions where VM are found.
         */
        private Map<Long, SMACSP> regionOidToCsp = new HashMap<>();

        /**
         * Given a region, update regionOidToCsp map.
         *
         * @param region region to process.
         */
        void updateWithRegion(TopologyEntityDTO region) {
            long regionId = region.getOid();
            if (regionOidToCsp.get(regionId) == null) {
                // not in the map
                String regionName = region.getDisplayName();
                // Determine CSP from region name.
                if (regionName.startsWith("aws")) {
                    regionOidToCsp.put(regionId, SMACSP.AWS);
                } else if (regionName.startsWith("azure")) {
                    regionOidToCsp.put(regionId, SMACSP.AZURE);
                } else {
                    logger.warn("getVMRegionId() region OID={} name={} has unknown CSP",
                        regionId, regionName);
                    regionOidToCsp.put(regionId, SMACSP.UNKNOWN);
                }
            }
        }

        /**
         * Given a region OID, return the SMACSP.
         * @param regionOid the region OID
         * @return if not found return UNKNOWN.
         */
        SMACSP lookupWithRegionId(long regionOid) {
            SMACSP csp = regionOidToCsp.get(regionOid);
            if (csp == null) {
                logger.warn("getCspFromRegionId no CSP found for region ID={}", regionOid);
                csp = SMACSP.UNKNOWN;
            }
            return csp;
        }
    }
}
