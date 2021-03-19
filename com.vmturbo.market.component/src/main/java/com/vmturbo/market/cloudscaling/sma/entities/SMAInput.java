package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle.ComputePrice;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.CoreBasedLicensePriceBundle;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.market.topology.conversions.ReservedInstanceKey;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * The input to the Stable marriage algorithm.
 * Integration with XL:
 *   add a constructor to convert market data structures into SMA data structures.
 */
public class SMAInput {

    private static final Logger logger = LogManager.getLogger();

    //price table to compute on-demand cost
    private final CloudRateExtractor marketCloudRateExtractor;

    /**
     * List of input contexts.
     */
    public final List<SMAInputContext> inputContexts;

    //keep retrieved compute price bundles
    private final Map<PriceTableKey, ComputePriceBundle> computePriceBundleMap = new HashMap<>();
    //keep retrieved reserved license price bundles
    private final Map<PriceTableKey, Set<CoreBasedLicensePriceBundle>> reservedLicenseMap = new HashMap<>();

    /**
     * Constructor for SMAInput.
     * @param contexts the input to SMA partioned into contexts.
     *                 Each element in list corresponds to a context.
     */
    public SMAInput(@Nonnull final List<SMAInputContext> contexts) {
        this.inputContexts = Objects.requireNonNull(contexts, "contexts is null!");
        this.marketCloudRateExtractor = null;
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
     * @param marketCloudRateExtractor used to figure out the discounts for business accounts
     * @param consistentScalingHelper used to figure out the consistent scaling information.
     * @param isPlan is true if plan otherwise real time.
     * @param reduceDependency if true will reduce relinquishing
     */
    public SMAInput(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull Map<Long, Set<Long>> providers,
            @Nonnull CloudCostData<TopologyEntityDTO> cloudCostData,
            @Nonnull CloudRateExtractor marketCloudRateExtractor,
            @Nonnull ConsistentScalingHelper consistentScalingHelper,
            boolean isPlan,
            boolean reduceDependency) {
        // check input parameters are not null
        Objects.requireNonNull(cloudTopology, "source cloud topology is null");
        Objects.requireNonNull(providers, "providers are null");
        Objects.requireNonNull(cloudCostData, "cloudCostData is null");
        cloudCostData.logMissingAccountPricingData();
        Objects.requireNonNull(marketCloudRateExtractor, "marketPriceTable is null");
        this.marketCloudRateExtractor = marketCloudRateExtractor;

        final Stopwatch stopWatch = Stopwatch.createStarted();
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
         * Dictionary for SMACSP by region.  Only regions where VMs were found.
         */
        CspFromRegion cspFromRegion = new CspFromRegion();
        /*
         * Map from computeTier OID to context to template, needed to convert provider list from
         * compute tier to template.  Need context, because there is a one to many relationship
         * between a compute tier and templates, one template per context for a compute tier.
         * The context is what is used to aggregate VMs, RIs, and Templates.
         */
        Table<Long, SMAContext, SMATemplate> computeTierOidToContextToTemplate = HashBasedTable.create();
        // Map from region ID to OSType to context: used to restrict template and RI creation.
        Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts = HashBasedTable.create();
        // keep track of business accounts in the context's billing family, used to restrict template creation.
        Map<SMAContext, Set<Long>> contextToBusinessAccountIds = new HashMap<>();
        // for each context in which VMs are found, collect the OSTypes, used to restrict template creation.
        Map<SMAContext, Set<OSType>> contextToOSTypes = new HashMap<>();
        // map from bought RI to SMA RI.
        Map<Long, SMAReservedInstance> riBoughtOidToRI = new HashMap<>();

        /*
         * For each virtual machines, create a VirtualMachine and partition into SMAContexts.
         */
        final Stopwatch stopWatchDetails = Stopwatch.createStarted();
        List<TopologyEntityDTO> virtualMachines =
            cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE);
        int numberVMsCreated = 0;
        logger.info("process {} VMs", () -> virtualMachines.size());
        for (TopologyEntityDTO vm : virtualMachines) {
            if (!(vm.getEntityState() == EntityState.POWERED_ON)) {
                logger.debug(" VM={} state={} != POWERED_ON", () -> vm.getOid(), () -> vm.getEntityState().name());
                continue;
            }
            numberVMsCreated++;
            processVirtualMachine(vm, cloudTopology, consistentScalingHelper,
                cspFromRegion, smaContextToVMs, regionIdToOsTypeToContexts,
                contextToBusinessAccountIds, contextToOSTypes);
        }
        logger.info("{}ms to create {} VMs from {} VirtualMachines in {} contexts",
            stopWatchDetails.elapsed(TimeUnit.MILLISECONDS), numberVMsCreated, virtualMachines.size(),
            smaContextToVMs.keySet().size());
        dumpContextToVMs(smaContextToVMs);
        dumpRegionIdToOsTypeToContexts(regionIdToOsTypeToContexts);
        dumpContextToBusinessAccountsIds(contextToBusinessAccountIds);

        /*
         * For each ComputeTier, create SMATemplates, but only in contexts where VMs exist.
         */
        stopWatchDetails.reset();
        stopWatchDetails.start();
        final List<TopologyEntityDTO> computeTiers =
            cloudTopology.getAllEntitiesOfType(EntityType.COMPUTE_TIER_VALUE);
        logger.info("process {} computeTiers", () -> computeTiers.size());
        int numberTemplatesCreated = processComputeTiers(computeTiers, cloudTopology, cloudCostData,
            regionIdToOsTypeToContexts, contextToBusinessAccountIds, contextToOSTypes, cspFromRegion,
            computeTierOidToContextToTemplate, smaContextToTemplates);

        logger.info("{}ms to create {} templates from {} compute tiers in {} contexts",
            () -> stopWatchDetails.elapsed(TimeUnit.MILLISECONDS), () -> numberTemplatesCreated,
            () -> computeTiers.size(), () -> smaContextToTemplates.keySet().size());
        dumpSmaContextsToTemplates(smaContextToTemplates);
        dumpComputeTierOidToContextToTemplate(computeTierOidToContextToTemplate);

        /*
         * For each the RI, create an SMAReservedInstance, but only in contexts where VMs exist.
         */
        stopWatchDetails.reset();
        stopWatchDetails.start();
        int numberRIsCreated = 0;
        Collection<ReservedInstanceData> allRIData;
        if (isPlan) {
            // include existing and bought RIs
            logger.debug("cloudCostData.getAllRiBought()");
            allRIData = cloudCostData.getAllRiBought();
        } else {
            // for realtime, only include existing RIs
            logger.debug("cloudCostData.getExistingRiBought()");
            allRIData = cloudCostData.getExistingRiBought();
        }
        logger.info("process {} RIs", () -> allRIData.size());
        for (ReservedInstanceData data :allRIData ) {
            if (processReservedInstance(data, cloudTopology, computeTierOidToContextToTemplate,
                regionIdToOsTypeToContexts, cspFromRegion,
                reservedInstanceKeyIDGenerator, smaContextToRIs, riBoughtOidToRI)) {
                numberRIsCreated++;
            }
        }
        logger.info("{}ms to create {} RIs from {} RI bought data in {} contexts",
            stopWatchDetails.elapsed(TimeUnit.MILLISECONDS), numberRIsCreated,
            cloudCostData.getAllRiBought().size(), smaContextToRIs.keySet().size());
        dumpSmaContextsToRIs(smaContextToRIs);

        /*
         * Update VM's current template and provider list.
         */
        stopWatchDetails.reset();
        stopWatchDetails.start();
        Set<SMAContext> smaContexts = smaContextToVMs.keySet();
        logger.info("update VMS for {} contexts", () -> smaContexts.size());
        for (SMAContext context :smaContexts) {
            Set<SMAVirtualMachine> vmsInContext = smaContextToVMs.get(context);
            updateVirtualMachines(vmsInContext, computeTierOidToContextToTemplate, providers,
                    cloudTopology, context, riBoughtOidToRI, cloudCostData);
        }
        dumpContextToVMsFinal(smaContextToVMs);
        logger.info("{}ms to update SMAVirtualMachines",
            stopWatchDetails.elapsed(TimeUnit.MILLISECONDS));

        /*
         * build input contexts.
         */
        stopWatchDetails.reset();
        stopWatchDetails.start();
        inputContexts = generateInputContexts(smaContextToVMs, smaContextToRIs, smaContextToTemplates, reduceDependency);
        logger.info("{}ms to generate SMAInputContexts",
            stopWatchDetails.elapsed(TimeUnit.MILLISECONDS));
        logger.info("total {}ms to convert to SMA data structures",
            stopWatch.elapsed(TimeUnit.MILLISECONDS));
    }

    /**
     * Generate the set of input contexts.
     * @param smaContextToVMs         Map from context to set of VMs
     * @param smaContextToRIs         Map from context to set of RIs
     * @param smaContextToTemplates   Map from context to set of Templates
     * @param reduceDependency if true will reduce relinquishing
     * @return list of input contexts
     */
    @Nonnull
    private static List<SMAInputContext> generateInputContexts(Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs,
                                                         Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs,
                                                         Map<SMAContext, Set<SMATemplate>> smaContextToTemplates,
                                                         boolean reduceDependency) {
        List<SMAInputContext> inputContexts = new ArrayList<>();
        Set<SMAContext> smaContexts = smaContextToVMs.keySet();
        logger.info("build input contexts for {} contexts", () -> smaContexts.size());
        for (SMAContext context : smaContexts) {
            Set<SMAVirtualMachine> smaVMs = smaContextToVMs.get(context);
            if (ObjectUtils.isEmpty(smaVMs)) {
                // there may be a RI context that has no VMs.
                logger.error(" no VM for context={}", context);
                continue;
            }
            smaVMs.removeIf(vm -> vm.getCurrentTemplate() == null);
            Set<SMAReservedInstance> smaRIs = smaContextToRIs.getOrDefault(context, Collections.emptySet());
            Set<SMATemplate> smaTemplates = smaContextToTemplates.get(context);
            if (ObjectUtils.isEmpty(smaTemplates)) {
                logger.error(" no template for context={}", context);
                continue;
            }
            smaTemplates.stream().forEach(t -> t.setComputeTier(null));
            logger.debug("{} #VMs={} #RIs={} #templates={}", () -> context,
                () -> smaVMs.size(), () -> smaRIs.size(), () -> smaTemplates.size());
            SMAInputContext inputContext = new SMAInputContext(context,
                Lists.newArrayList(smaVMs),
                (smaRIs == null ? new ArrayList<>() : Lists.newArrayList(smaRIs)),
                Lists.newArrayList(smaTemplates), new SMAConfig(reduceDependency));
            inputContexts.add(inputContext);
        }
        return inputContexts;
    }

    /**
     * Create a SMA Virtual Machine and SMA context from a VM topology entity DTO.
     * Because scale actions do not modify region, osType or tenancy, the regionsToOsTypeToContext
     * keeps track of the what osTypes and contexts that are in a region.
     * @param entity                     topology entity DTO that is a VM.
     * @param cloudTopology               the cloud topology to find source template.
     * @param consistentScalingHelper     used to figure out the consistent scaling information.
     * @param cspFromRegion               keep track of CSP by region
     * @param smaContextToVMs             map from SMA context to set of SMA virtual machines, updated
     * @param regionIdToOsTypeToContexts  table from region ID  to osType to set of SMAContexts, defined
     * @param contextToBusinessAccountIds map from context to set of business account IDs, defined.
     * @param contextToOSTypes            map from context to set of OSTypes.  Limits compute Tier processing. defined.
     */
    private void processVirtualMachine(final TopologyEntityDTO entity,
                                      final CloudTopology<TopologyEntityDTO> cloudTopology,
                                      final ConsistentScalingHelper consistentScalingHelper,
                                      final CspFromRegion cspFromRegion,
                                      Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs,
                                      Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts,
                                      Map<SMAContext, Set<Long>> contextToBusinessAccountIds,
                                      Map<SMAContext, Set<OSType>> contextToOSTypes) {
        long oid = entity.getOid();
        String name = entity.getDisplayName();
        int entityType = entity.getEntityType();
        if (entityType != EntityType.VIRTUAL_MACHINE_VALUE) {
            logger.error("processVM: entity is not a VM: ID={} name={}",
                oid, name);
            return;
        }
        if (!entity.getTypeSpecificInfo().hasVirtualMachine()) {
            logger.error("processVM: entity ID={} name={} doesn't have VirutalMachineInfo",
                oid, name);
            return;
        }
        VirtualMachineInfo vmInfo = entity.getTypeSpecificInfo().getVirtualMachine();
        if (vmInfo.getBillingType() != VMBillingType.ONDEMAND) {
            logger.debug("processVM: skip VM OID={} name={}  billingType={} != ONDEMAND",
                () -> oid, () -> name, () -> vmInfo.getBillingType().name());
            return;
        }

        String tenancyName = vmInfo.getTenancy().name();
        Tenancy tenancy = Tenancy.valueOf(tenancyName);
        String osName = vmInfo.getGuestOsInfo().getGuestOsType().name();
        OSType osType = OSType.valueOf(osName);
        if (osType == OSType.UNKNOWN_OS && !osName.equalsIgnoreCase(OSType.UNKNOWN_OS.name())) {
            logger.warn("processVM: osName={} converted to UNKNOWN_OS", osName);
        }
        /*
         * create Context
         */
        long regionId = getVMRegionId(oid, cloudTopology, cspFromRegion);
        SMACSP csp = cspFromRegion.lookupWithRegionId(regionId);
        Optional<TopologyEntityDTO> zoneOptional = cloudTopology.getConnectedAvailabilityZone(oid);
        long zoneId = SMAUtils.NO_ZONE;
        if (zoneOptional.isPresent()) {
            zoneId = zoneOptional.get().getOid();
        } else if (csp != SMACSP.AZURE) {
            // Azure currently does not support availability zones.
            logger.error("processVM: VM OID={} name={} can't find availabilty zone",
                oid, name);
            return;
        }

        // Because RIs are OS agnostic in Azure, if Azure, use OSType UNKNOWN.
        OSType osTypeForContext = (csp == SMACSP.AZURE ? OSType.UNKNOWN_OS : osType);
        long businessAccountId = getBusinessAccountId(oid, cloudTopology, "VM");
        long billingFamilyId = getBillingFamilyId(businessAccountId, cloudTopology, "VM");
        SMAContext context = new SMAContext(csp, osTypeForContext, regionId, billingFamilyId, tenancy);
        logger.debug("processVM: new {}  osType={}  accountId={}", context, osType,
            businessAccountId);
        // Add business account to context.
        Set<Long> accounts = contextToBusinessAccountIds.getOrDefault(context, new HashSet<>());
        accounts.add(businessAccountId);
        contextToBusinessAccountIds.put(context, accounts);
        // Add OSType to context.
        Set<OSType> osTypes = contextToOSTypes.getOrDefault(context, new HashSet<>());
        osTypes.add(osType);
        contextToOSTypes.put(context, osTypes);

        Optional<String> groupIdOptional = consistentScalingHelper.getScalingGroupId(oid);
        String groupName = SMAUtils.NO_GROUP_ID;
        if (groupIdOptional.isPresent()) {
            groupName = groupIdOptional.get();
        }
        /*
         * Create Virtual Machine.
         */
        SMAVirtualMachine vm = new SMAVirtualMachine(oid,
            name,
            groupName,
            businessAccountId,
            null,
            new ArrayList<>(),
            SMAUtils.NO_RI_COVERAGE,
            zoneId,
            SMAUtils.BOGUS_RI,
            osType,
            vmInfo.getLicenseModel(),
                false);
        logger.debug("processVM: new VM {}", vm);

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
     * @param riBoughtOidToRI map from RI bought OID to created SMA RI
     * @param cloudCostData where to find costs and RI related info.  E.g. RI coverage for a VM.
     */
    private void updateVirtualMachines(final Set<SMAVirtualMachine> vms,
                                       final Table<Long, SMAContext, SMATemplate> computeTierOidToContextToTemplate,
                                       final Map<Long, Set<Long>> providersList,
                                       final CloudTopology<TopologyEntityDTO> cloudTopology,
                                       final SMAContext context,
                                       @Nonnull final Map<Long, SMAReservedInstance> riBoughtOidToRI,
                                       @Nonnull final CloudCostData cloudCostData) {
        for (SMAVirtualMachine vm : vms) {
            long oid = vm.getOid();
            String name = vm.getName();
            Optional<TopologyEntityDTO> currentTemplateOptional = cloudTopology.getComputeTier(oid);
            SMATemplate currentTemplate = null;
            long computeTierID = -1;
            if (!currentTemplateOptional.isPresent()) {
                logger.error("updateVMs: VM ID={} name={} no compute Tier",
                    oid, name);
            } else {
                computeTierID = currentTemplateOptional.get().getOid();
                currentTemplate = computeTierOidToContextToTemplate.get(computeTierID, context);
                if (currentTemplate == null) {
                    logger.error("updateVMs: VM ID={} name={} no template ID={} in list of templates",
                        oid, name, computeTierID);
                    continue;
                } else {
                    vm.setCurrentTemplate(currentTemplate);
                }
            }
            Set<Long> providerOids = providersList.get(oid);
            final List<SMATemplate> providers = new ArrayList<>();
            if (providerOids == null) {
                logger.debug("updateVMs: no providers for VM ID={} name={}", oid, name);
            } else if (providerOids.isEmpty()) {
                logger.warn("updateVMs: no providers for VM ID={} name={}", oid, name);
            } else {
                for (long providerId : providerOids) {
                    SMATemplate template = computeTierOidToContextToTemplate.get(providerId, context);
                    if (template == null) {
                        logger.error("updateVMs: VM ID={} name={} no providerID={} in computeTierToContextToTemplateMap with context={}",
                            oid, name, providerId, context);      // currently expected
                    } else {
                        providers.add(template);
                    }
                }
            }
            vm.setProviders(providers);

            Pair<SMAReservedInstance, Float> currentRICoverage = computeVmCoverage(oid, cloudCostData, riBoughtOidToRI);
            if (currentRICoverage != null) {
                logger.debug("updateVMs: ID={} name={} RI={} currentRICoverage={}", () -> oid,
                        () -> name, () -> currentRICoverage.getFirst(), () -> currentRICoverage.getSecond());
                vm.setCurrentRI(currentRICoverage.getFirst());
                vm.setCurrentRICoverage(currentRICoverage.getSecond());
            }
        }
    }

    /**
     * Given a set of computer tiers, generate the corresponding SMATemplates.
     * A compute tier does not specify either a tenancy, a billing family or a business account.
     * Only generate SMATemplates that are needed by the Virtual machines that may be scaled.
     * Partition compute tiers by region ID to ensure compute tiers match with the region's CSP.
     *
     * @param computeTiers                Set of compute tiers in this cloud topology.
     * @param cloudTopology               cloud topology dictionary.
     * @param cloudCostData               cost dictionary.
     * @param regionIdToOsTypeToContexts  table from region ID to osType to set of contexts.
     * @param contextToBusinessAccountIds map from context to set of business accounts in this context
     * @param contextToOSTypes            map from context to set of OSTypes in this context
     * @param cspFromRegion               keep track of CSP by region
     * @param computeTierIdToContextToTemplateMap compute tier ID to context to Template map, to be updated
     * @param smaContextToTemplates       map from context to template, to be updated
     * @return true if a template is created, else false
     */
    private int processComputeTiers(final List<TopologyEntityDTO> computeTiers,
                                    final CloudTopology<TopologyEntityDTO> cloudTopology,
                                    final CloudCostData cloudCostData,
                                    final Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts,
                                    final Map<SMAContext, Set<Long>> contextToBusinessAccountIds,
                                    final Map<SMAContext, Set<OSType>> contextToOSTypes,
                                    final CspFromRegion cspFromRegion,
                                    Table<Long, SMAContext, SMATemplate> computeTierIdToContextToTemplateMap,
                                    Map<SMAContext, Set<SMATemplate>> smaContextToTemplates) {
        int numberTemplatesCreated = 0;
        // collection of valid region IDs
        Set<Long> validRegionIds = regionIdToOsTypeToContexts.rowKeySet();
        // partition compute tiers by valid region ID
        Map<Long, List<TopologyEntityDTO>> regionIdToTier = partitionComputeTiersByRegionId(computeTiers,
                                                                                            validRegionIds,
                                                                                            cloudTopology);
        for (Long regionId : validRegionIds) {
            SMACSP csp = cspFromRegion.lookupWithRegionId(regionId);
            // set of compute tiers in the valid region ID
            for (TopologyEntityDTO computeTier : regionIdToTier.get(regionId)) {
                boolean created = processComputeTier(computeTier, cloudTopology, cloudCostData,
                    regionIdToOsTypeToContexts, contextToBusinessAccountIds, contextToOSTypes, regionId,
                    csp, computeTierIdToContextToTemplateMap, smaContextToTemplates);
                if (created) {
                    numberTemplatesCreated++;
                }
            }
        }
        return numberTemplatesCreated;
    }

    /**
     * Given a list of compute tiers, return a map that partitions the compute tiers by valid region Ids.
     * @param computeTiers list of compute tiers
     * @param validRegionIds set of valid region IDs.
     * @param cloudTopology dictionary
     * @return compute tiers partitioned by region ID.
     */
    private Map<Long, List<TopologyEntityDTO>> partitionComputeTiersByRegionId(List<TopologyEntityDTO> computeTiers,
                                                                               Set<Long> validRegionIds,
                                                                               CloudTopology<TopologyEntityDTO> cloudTopology) {
        // map from region ID to list of compute tiers in that region
        Map<Long, List<TopologyEntityDTO>> map = new HashMap<>();
        for (TopologyEntityDTO dto: computeTiers) {
            // list of regions this compute tier belongs to
            List<Long> regionIds = dto.getConnectedEntityListList().stream()
            .filter(connEntity -> connEntity.hasConnectedEntityType()
                && (connEntity.getConnectedEntityType() == EntityType.REGION_VALUE))
                .map(connEntity -> connEntity.getConnectedEntityId())
                .collect(Collectors.toList());
            for (Long oid: regionIds) {
                if (validRegionIds.contains(oid)) {
                    // Only interested in valid regions
                    List<TopologyEntityDTO> list = map.getOrDefault(oid, new ArrayList<>());
                    list.add(dto);
                    map.put(oid, list);
                }
            }
        }
        return map;
    }
    /**
     * Given an topology entity that is a compute tier, generate one or more SMATemplates.
     * A compute tier does not specify either a tenancy, a billing family or a business account.
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
     * @param contextToBusinessAccountIds   map from context to set of business accounts in this context
     * @param contextToOSTypes              map from context to set of OSTypes in this context
     * @param regionId                      the ID of the region we are in.
     * @param csp                           cloud service provider; e.g. Azure.
     * @param computeTierOidToContextToTemplate computeTier ID to context to template map, to be updated
     * @param smaContextToTemplates         map from context to template, to be updated
     * @return true if a template is created, else false
     */
    private boolean processComputeTier(TopologyEntityDTO computeTier,
                                       CloudTopology<TopologyEntityDTO> cloudTopology,
                                       CloudCostData cloudCostData,
                                       Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts,
                                       Map<SMAContext, Set<Long>> contextToBusinessAccountIds,
                                       Map<SMAContext, Set<OSType>> contextToOSTypes,
                                       long regionId,
                                       SMACSP csp,
                                       Table<Long, SMAContext, SMATemplate> computeTierOidToContextToTemplate,
                                       Map<SMAContext, Set<SMATemplate>> smaContextToTemplates) {
        long oid = computeTier.getOid();
        String name = computeTier.getDisplayName();
        if (computeTier.getEntityType() != EntityType.COMPUTE_TIER_VALUE) {
            logger.error("processComputeTier: entity ID={} name={} is not a computeTier: ",
                oid, name);
            return false;
        }
        if (!computeTier.getTypeSpecificInfo().hasComputeTier()) {
            logger.error("processComputeTier: entity ID={} name={} doesn't have ComputeTierInfo",
                oid, name);
            return false;
        }
        ComputeTierInfo computeTierInfo = computeTier.getTypeSpecificInfo().getComputeTier();
        String family = computeTierInfo.getFamily();
        float coupons = (float)computeTierInfo.getNumCoupons();
        final float penalty =
                (computeTierInfo.hasScalePenalty()) ? computeTierInfo.getScalePenalty().getPenalty() : 0F;

        /*
         * Find all the contexts in which this template belongs.  Iterate threw osTypes and lookup
         * contexts.  Because Azure is platform flexible, templates are not distinguish by OS Type,
         * but use UNKNOWN_OS as place holder.
         */
        Set<OSType> osTypes = computeOsTypes(computeTier);
        Set<OSType> osTypesForContext = SMAUtils.UNKNOWN_OS_SINGLETON_SET;
        if (csp != SMACSP.AZURE) {
            osTypesForContext = osTypes;
        }
        for (OSType  osType : osTypes) {
            Set<SMAContext> contexts = regionIdToOsTypeToContexts.get(regionId, osType);
            if (CollectionUtils.isEmpty(contexts)) {
                logger.trace("processComputeTier: not in a context ID={} name={} regionId={} osType={}",
                    () -> oid, () -> name, () -> regionId, () -> osType.name(), () -> regionId);
                continue;
            }
            logger.trace("processComputeTier: in {} contexts: ID={} name={} regionId={} osType={}",
                () -> contexts.size(), () -> oid, () -> name, () -> regionId, () -> osType.name());

            for (SMAContext context: contexts) {
                if (context.getCsp() != csp) {
                    logger.trace("processComputeTier: ID={} name={} csp={} != context CSP={} {}",
                        () -> oid, () -> name, () -> csp, () -> context.getCsp().name(), () -> context);
                    break;
                }
                Set<OSType> osTypesInContext = contextToOSTypes.getOrDefault(context, new HashSet<>());
                /*
                 * For each osType create template with osType specific cost.
                 */
                SMATemplate template = new SMATemplate(oid, name, family, coupons, computeTier, penalty);
                logger.trace("processComputeTier: new {} in {}", template, context);
                Set<SMATemplate> templates = smaContextToTemplates.getOrDefault(context, new HashSet<>());
                templates.add(template);
                smaContextToTemplates.put(context, templates);
                /*
                 * For each business account in this context's billing family and OSType, compute cost.
                 */
                for (long businessAccountId : contextToBusinessAccountIds.get(context)) {
                    if (csp == SMACSP.AZURE) {
                        // If Azure, because of platform flexible, have to iterate threw all the os types explicitly.
                        for (OSType osTypeInner : osTypes) {
                            if (osTypesInContext.contains(osTypeInner)) {
                                updateTemplateRate(template, osTypeInner, context, businessAccountId,
                                    regionId, cloudCostData, cloudTopology);
                            } else {
                                logger.trace("processComputeTier: ID={} name={} no VMs with OSType={} in {}",
                                    oid, name, osTypeInner, context);
                            }
                        }
                    } else {
                        if (osTypesForContext.contains(osType)) {
                            updateTemplateRate(template, osType, context, businessAccountId,
                                regionId, cloudCostData, cloudTopology);
                        } else {
                            logger.trace("processComputeTier: ID={} name={} no VMs with OSType={} in {}",
                                oid, name, osType, context);
                        }
                    }
                }
                computeTierOidToContextToTemplate.put(oid, context, template);
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
        return entity.getCommoditySoldListList().stream()
                .filter(c -> c.getCommodityType().getType() == CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .map(c -> CloudRateExtractor.OS_TYPE_MAP.getOrDefault(c.getCommodityType().getKey(), OSType.UNKNOWN_OS))
                .collect(Collectors.toSet());
    }

    /**
     * Get compute price bundle.
     * @param accountPricingData account pricing data
     * @param regionOid region oid
     * @param computeTier compute tier
     * @return compute price bundle.
     */
    private ComputePriceBundle getComputeBundle(@NotNull AccountPricingData accountPricingData,
            long regionOid, @NotNull TopologyEntityDTO computeTier) {
        final PriceTableKey key = new PriceTableKey(accountPricingData.getAccountPricingDataOid(),
                regionOid, computeTier.getOid());
        return computePriceBundleMap.computeIfAbsent(key,
                k -> marketCloudRateExtractor.getComputePriceBundle(computeTier, regionOid,
                        accountPricingData));
    }

    /**
     * Get reserved license price bundle.
     * @param accountPricingData account pricing data
     * @param region The region, used only for the PriceTableKey. It is not a constraint on the reserved
     *               license price
     * @param computeTier compute tier
     * @return reserved license price bundle.
     */
    private Set<CoreBasedLicensePriceBundle> getReservedLicenseBundle(
            @NotNull AccountPricingData accountPricingData,
            @Nonnull TopologyEntityDTO region,
            @NotNull TopologyEntityDTO computeTier) {
        final PriceTableKey key = new PriceTableKey(accountPricingData.getAccountPricingDataOid(),
                region.getOid(), computeTier.getOid());
        return reservedLicenseMap.computeIfAbsent(key,
                k -> marketCloudRateExtractor.getReservedLicensePriceBundles(accountPricingData, computeTier));
    }

    /**
     * Derived a template's on-demand cost from cloud cost data.
     *
     * @param template the template, whose cost will be updated.
     * @param osType the OS type.
     * @param context the context.
     * @param businessAccountId business account of the template.
     * @param regionId regionId of the template.
     * @param cloudCostData cost dictionary.
     * @param cloudTopology entity dictionary.
     */
    private void updateTemplateRate(SMATemplate template, OSType osType, SMAContext context,
                                    long businessAccountId, long regionId, CloudCostData cloudCostData,
                                    CloudTopology<TopologyEntityDTO> cloudTopology) {
        long oid = template.getOid();
        String name = template.getName();
        /*
         * compute on-demand rate
         */
        // business account specific pricing.
        Optional<AccountPricingData> pricingDataOptional = cloudCostData.getAccountPricingData(businessAccountId);
        if (!pricingDataOptional.isPresent()) {
            logger.debug("updateTemplateRate: template ID={}:name={} doesn't have pricingData for accountId={} in {}",
                oid, name, businessAccountId, context);
            return;
        } else {
            AccountPricingData accountPricingData = pricingDataOptional.get();
            ComputePriceBundle bundle = getComputeBundle(accountPricingData,
                    context.getRegionId(), template.getComputeTier());
            List<ComputePrice> computePrices = bundle.getPrices();
            int computePriceSize = computePrices.size();
            if (computePriceSize == 0) {
                logger.trace("updateTemplateRate: template ID={}:name={} has computePrice.size() == 0 for accountId={} in {}",
                    oid, name, businessAccountId, context);
                return;
            }
            final ComputePrice computePrice = computePrices.stream()
                .filter(price -> price.osType() == osType)
                .findAny().orElse(null);

            if (computePrice == null) {
                logger.error("updateTemplateRate: no on-demand hourlyRate in {} computePrices for template ID={}:name={} in accountId={} osType={} {}",
                    computePriceSize, oid, name, businessAccountId, osType.name(), context);
                return;
            } else if (osType != OSType.UNKNOWN_OS) {
                logger.trace("updateTemplateRate: on-demand Rate={}: template ID={}:name={} in accountId={} osType={} {}",
                    computePrice.hourlyPrice(), oid, name, businessAccountId, osType.name(),
                    context);
            }
            template.setOnDemandCost(businessAccountId, osType, new SMACost(
                (float)computePrice.hourlyComputeRate(), (float)computePrice.hourlyLicenseRate()));

            /*
             * compute discounted costs
             * For AWS, there are no discounted costs.
             * Accessing the accounting price data is not working.
             */
            TopologyEntityDTO computeTier = template.getComputeTier();
            Optional<TopologyEntityDTO> regionOptional = cloudTopology.getEntity(regionId);
            if (!regionOptional.isPresent()) {
                logger.error("updateTemplateRate: can't find region for ID={} for template ID={}:name={} in accountId={} osType={} {}",
                    regionId, oid, name, businessAccountId, osType.name(), context);
                return;
            } else {
                TopologyEntityDTO region = regionOptional.get();
                final Set<CoreBasedLicensePriceBundle> reservedLicensePrices =
                        getReservedLicenseBundle(accountPricingData, region, computeTier);

                if (reservedLicensePrices == null || reservedLicensePrices.isEmpty()) {
                    logger.trace("updateTemplateRate: template ID={}:name={} has discount license computePrice.size() == 0 for accountId={} in {}",
                        oid, name, businessAccountId, context);
                    return;
                }

                final double hourlyRate = reservedLicensePrices.stream()
                        .filter(CoreBasedLicensePriceBundle::hasPrice)
                        .filter(priceBundle -> priceBundle.osType() == osType)
                        .map(priceBundle -> priceBundle.price().get())
                        .findFirst()
                        .orElse(0.0);
                if (osType != OSType.UNKNOWN_OS) {
                    logger.trace("updateTemplateRate: template ID={}:name={} discount license Rate={} accountId={} in {}",
                        oid, name, hourlyRate, businessAccountId, context);
                }
                // For AWS, hourly rate is zero.
                template.setDiscountedCost(businessAccountId, osType, new SMACost(SMAUtils.NO_COST, (float)hourlyRate));
            }
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

     * @param data            TopologyEntityDTO that is an RI
     * @param cloudTopology   topology to get business account and region.
     * @param computeTierOidToContextToTemplate used to look up SMATemplate given the computeTier ID
     * @param regionIdToOsTypeToContexts map from regionID to OSType to context.
     * @param cspFromRegion keep track of CSP and regions
     * @param reservedInstanceKeyIDGenerator ID generator for ReservedInstanceKey
     * @param smaContextToRIs map from context to set of RIs, to be updated
     * @param riBoughtOidToRI map from RI bought OID to created SMA RI, to be updated
     * @return true if RI is created
     */
    private boolean processReservedInstance(final ReservedInstanceData data,
                                           final CloudTopology<TopologyEntityDTO> cloudTopology,
                                           final Table<Long, SMAContext, SMATemplate> computeTierOidToContextToTemplate,
                                           final Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts,
                                           final CspFromRegion cspFromRegion,
                                           final SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator,
                                           Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs,
                                           Map<Long, SMAReservedInstance> riBoughtOidToRI) {
        ReservedInstanceBought riBought = data.getReservedInstanceBought();
        long riBoughtId = riBought.getId();
        ReservedInstanceBoughtInfo riBoughtInfo = riBought.getReservedInstanceBoughtInfo();
        long businessAccountId = riBoughtInfo.getBusinessAccountId();
        long billingFamilyId = getBillingFamilyId(businessAccountId, cloudTopology, "RI");
        String name = riBoughtInfo.getProbeReservedInstanceId();  // getDisplayName();
        long zoneId = riBoughtInfo.getAvailabilityZoneId();
        zoneId = (zoneId == 0 ? SMAUtils.NO_ZONE : zoneId);
        ReservedInstanceBoughtCost boughtCost = riBoughtInfo.getReservedInstanceBoughtCost();
        boolean shared = riBoughtInfo.getReservedInstanceScopeInfo().getShared();

        ReservedInstanceSpec riSpec = data.getReservedInstanceSpec();
        ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();
        // Can't find template until after processComputeTier

        ReservedInstanceType type = riSpecInfo.getType();
        int years = type.getTermYears();
        String tenancyName = riSpecInfo.getTenancy().name();
        Tenancy tenancy = Tenancy.valueOf(tenancyName);
        String osTypeName = riSpecInfo.getOs().name();
        // for Azure RIs, OSType is always UNKNOWN, because the RI is OSType agnostic.
        OSType  osType = OSType.valueOf(osTypeName);
        long regionId = riSpecInfo.getRegionId();
        SMACSP csp = cspFromRegion.lookupWithRegionId(regionId);
        if (csp == null) {
            // no VMs found in this region, skip this RI.
            logger.trace("processRI: skip riBoughtId={} name={} no VMs in regionID={}",
                riBoughtId, name, regionId);
            return false;
        }
        final OSType osTypeForContext = csp == SMACSP.AZURE ? OSType.UNKNOWN_OS : osType;

        boolean found = contextExists(regionIdToOsTypeToContexts, billingFamilyId, regionId, osTypeForContext, tenancy);
        if (found == false) {
            final String nameFinal = name;
            logger.debug("processRI: no context exits for RI name={} with billingFamilyId={} regionId={} OSType={} Tenancy={} ISF={} shared={} platformFlexible={}",
                () -> nameFinal, () -> billingFamilyId, () -> regionId, () -> osType.name(),
                () -> tenancy.name(), () -> riSpecInfo.getSizeFlexible(), () -> shared,
                () -> riSpecInfo.getPlatformFlexible());
            return false;
        }

        SMAContext context = new SMAContext(csp, osTypeForContext, regionId, billingFamilyId, tenancy);
        long computeTierOid = riSpecInfo.getTierId();
        if (Strings.isNullOrEmpty(name)) {
            logger.trace("RI name is null or empty for riBoughtID={}", riBoughtId);
            name = constructRIName(cloudTopology, regionId, riBoughtInfo);

        }
        // for Azure, this RI covers a set of templates, one for each OSType.
        SMATemplate template = computeTierOidToContextToTemplate.get(computeTierOid, context);
        if (template == null) {
            logger.error("processRI: can't find template with ID={} in templateMap for RI boughtID={} name={}",
                    computeTierOid, riBoughtId, name);
            return false;
        }
        ReservedInstanceKey reservedInstanceKey = new ReservedInstanceKey(data,
                template.getFamily(), billingFamilyId);
        long riKeyId = reservedInstanceKeyIDGenerator.lookUpRIKey(reservedInstanceKey, riBoughtId);

        String templateName = template.getName();

        Set<Long> scopedAccountsIds = shared ? Collections.emptySet() : ImmutableSet.copyOf(
                riBoughtInfo.getReservedInstanceScopeInfo().getApplicableBusinessAccountIdList());

        // adjust count of RI to reflect partial RI
        final float count = (float)riBoughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons()
                / template.getCoupons();

        // Unfortunately, the probe returns ISF=true for metal templates.
        SMAReservedInstance ri = new SMAReservedInstance(riBoughtId,
            riKeyId,
            name,
            businessAccountId,
            scopedAccountsIds,
            template,
            zoneId,
            count,
            riSpecInfo.getSizeFlexible(),
            shared,
            riSpecInfo.getPlatformFlexible());
        if (ri == null) {
            logger.error("processRI: regionId={} new SMA_RI FAILED: oid={} name={} accountId={} template={} zondId={} OS={} tenancy={} count={}",
                regionId, riBoughtId, name, businessAccountId, templateName, zoneId, osType.name(),
                tenancy.name(), count);
        } else {
            if (logger.isDebugEnabled()) {
                double riRate = computeHourlyRIRate(boughtCost, years);
                logger.debug("processRI: new {} with riRate={} {}", ri, riRate, context);
            }
            Set<SMAReservedInstance> smaRIs = smaContextToRIs.getOrDefault(context, new HashSet<>());
            smaRIs.add(ri);
            smaContextToRIs.put(context, smaRIs);
            riBoughtOidToRI.put(riBoughtId, ri);
        }
        return true;
    }

    /**
     * For new RIs that have been bought, their probeReservedInstanceId is undefined, but used as
     * the name; therefore, construct an RI name.
     *
     * @param cloudTopology      cloud topology dictionary
     * @param regionId           region OID
     * @param riBoughtInfo       RI's bought information
     * @return constructed RI name
     */
    public static String constructRIName(final CloudTopology<TopologyEntityDTO> cloudTopology,
                                          long regionId, ReservedInstanceBoughtInfo riBoughtInfo) {
        Optional<TopologyEntityDTO> optionalDTO = cloudTopology.getEntity(regionId);
        String regionName = SMAUtils.UNKNOWN_NAME;
        if (optionalDTO.isPresent()) {
            regionName = optionalDTO.get().getDisplayName();
        }
        optionalDTO = cloudTopology.getEntity(riBoughtInfo.getBusinessAccountId());
        String businessName = SMAUtils.UNKNOWN_NAME;
        if (optionalDTO.isPresent()) {
            businessName = optionalDTO.get().getDisplayName();
        }
        float fixedCost = (float)riBoughtInfo.getReservedInstanceBoughtCost().getFixedCost().getAmount();
        float usagePerHourCost = (float)riBoughtInfo.getReservedInstanceBoughtCost().getUsageCostPerHour().getAmount();
        return "buyRI_" + businessName + "_" + regionName + "_" + SMAUtils.format4Digits(fixedCost) +
            "_" + SMAUtils.format4Digits(usagePerHourCost);
    }

    /**
     * Determine if there exists a context for csp, billing family DI, regionId, osType and Tenancy.
     * @param regionToOsTypeToContext Table containing contexts.
     * @param billingFamilyId billing family ID.
     * @param regionId region ID
     * @param osType OS
     * @param tenancy Tenancy
     * @return true if exists.
     */
    private static boolean contextExists(Table<Long, OSType, Set<SMAContext>> regionToOsTypeToContext,
                                         long billingFamilyId, long regionId, OSType osType, Tenancy tenancy) {
        Set<SMAContext> contexts = regionToOsTypeToContext.get(regionId, osType);
        if (contexts == null) {
            return false;
        }
        boolean found = false;
        // verify  exists a context with CSP, billing account and tenancy.
        for (SMAContext context : contexts) {
            if (context.getTenancy() == tenancy &&
                context.getBillingFamilyId() == billingFamilyId
            ) {
                found = true;
                break;
            }
        }
        return found;
    }

    /**
     * Find the region ID.
     * Side effect: update cspFromRegion with region.
     *
     * @param oid           ID  of topology entity
     * @param cloudTopology dictionary of cloud topoolgy
     * @param cspFromRegion keep track of CSP by region
     * @return region ID
     */
    private static long getVMRegionId(final long oid,
                                      final @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                      CspFromRegion cspFromRegion) {
        long regionId = -1;
        TopologyEntityDTO region = null;
        Optional<TopologyEntityDTO> regionOpt = cloudTopology.getConnectedRegion(oid);
        if (!regionOpt.isPresent()) {
            logger.error("getRegion: can't find region for OID={}", oid);
        } else {
            region = regionOpt.get();
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
     * @return business account ID
     */
     public static long getBusinessAccountId(long oid,
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
     * Given a business acount, find the billing family.
     * In AWS, the master account is the billing family.
     * In Azure, the Enterprise Account is the billing family.
     *
     * @param oid           business account OID.
     * @param cloudTopology dictionary of cloud topology
     * @param msg           who is this.
     * @return billing family ID
     */
    private static long getBillingFamilyId(long oid,
                                           @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                           String msg) {
        long billingFamilyId = oid;
        Optional<GroupAndMembers> optional = cloudTopology.getBillingFamilyForEntity(oid);
        if (!optional.isPresent()) {
            // if ID  is a master account, expect accountOpt to be empty
            logger.trace("getBillingFamilyId{}: can't find billing family ID for {} OID={}", msg, oid);
        } else {
            GroupAndMembers groupAndMembers = optional.get();
            billingFamilyId = groupAndMembers.group().getId();
        }
        return billingFamilyId;
    }

    /**
     * Compute the RI coverage of a VM.  If VM has multiple RIs, then ISF, and only choose one RI.
     * @param vmOid           OID of virtualMachine
     * @param cloudCostData   cost dictionary
     * @param riBoughtOidToRI map from RI bought OID to SMA RI
     * @return Pair SMA RI to coupons covered.
     */
    @Nonnull
    private Pair<SMAReservedInstance, Float> computeVmCoverage(final Long vmOid,
                                                               final CloudCostData cloudCostData,
                                                               final Map<Long, SMAReservedInstance> riBoughtOidToRI) {
        Pair<SMAReservedInstance, Float> currentRICoverage = null;
        Optional<EntityReservedInstanceCoverage> riCoverageOptional = cloudCostData.getRiCoverageForEntity(vmOid);
        if (riCoverageOptional.isPresent()) {
            EntityReservedInstanceCoverage riCoverage = riCoverageOptional.get();
            currentRICoverage = computeVmCoverage(riCoverage, riBoughtOidToRI);
        } else {
            logger.error("processVirtualMachine: could not coverage VM ID={}", vmOid);
        }
        if (currentRICoverage == null) {
            return null;
        }
        return currentRICoverage;
    }

        /**
     * Compute the current RI Coverage and the SMA specific RI Key ID.
     * @param riCoverage RI coverage information: RI ID -> # coupons covered
     * @return ReservedInstanceCoverage
     */
    @Nullable
    private Pair<SMAReservedInstance, Float> computeVmCoverage(EntityReservedInstanceCoverage riCoverage,
                                                               Map<Long, SMAReservedInstance> riBoughtOidToRI) {
        Map<Long, Double> riOidToCoupons = riCoverage.getCouponsCoveredByRi();
        float coverage = SMAUtils.NO_RI_COVERAGE;
        SMAReservedInstance ri = null;
        for (Entry<Long, Double> coupons : riOidToCoupons.entrySet()) {
            if (coupons.getValue() > SMAUtils.EPSILON) {
                coverage += coupons.getValue();
                long riOID = coupons.getKey();
                ri = riBoughtOidToRI.get(riOID);
                if (ri == null) {
                    logger.error("computeVmCoverage RI bought OID={} not found in riBoughtOidToRI",
                        riOID);
                    return null;
                }
            }
        }
        return new Pair(ri, coverage);
    }

    private static double computeHourlyRIRate(ReservedInstanceBoughtCost boughtCost, int years) {
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
    private static void
    updateRegionIdToOsTypeToContexts(Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts,
                                     long regionId, OSType osType, SMAContext context) {
        Map<OSType, Set<SMAContext>> map = regionIdToOsTypeToContexts.row(regionId);
        if (map == null) {
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
        logger.info("dump contexts to VMS for {} contexts",
            () -> smaContextToVMs.keySet().size());
        if (logger.isDebugEnabled()) {
            for (SMAContext context : smaContextToVMs.keySet()) {
                logger.info("  {}", context);
                for (SMAVirtualMachine vm : smaContextToVMs.get(context)) {
                    logger.info("    VM: ID={} name={} businessAccountId={} OS={}", () -> vm.getOid(),
                        () -> vm.getName(), () -> vm.getBusinessAccountId(), () -> vm.getOsType().name());
                }
            }
        }
    }

    private void dumpContextToVMsFinal(Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs) {
        if (logger.isDebugEnabled()) {
            List<SMAVirtualMachine> vms = new ArrayList<>();
            smaContextToVMs.values().forEach(vms::addAll);
            logger.debug("dump {} VMs after updated", () -> vms.size());
            for (SMAVirtualMachine vm : vms) {
                logger.debug("  {}", vm);
            }
        }
    }

    private void dumpRegionIdToOsTypeToContexts(Table<Long, OSType, Set<SMAContext>> regionIdToOsTypeToContexts) {
        if (logger.isDebugEnabled()) {
            logger.debug("dump region to osType to context table for {} regions",
                () -> regionIdToOsTypeToContexts.rowKeySet().size());
            for (long regionId : regionIdToOsTypeToContexts.rowKeySet()) {
                logger.debug("  region={}", regionId);
                Map<OSType, Set<SMAContext>> map = regionIdToOsTypeToContexts.row(regionId);
                for (OSType osType : map.keySet()) {
                    logger.debug("    osType={}", osType);
                    for (SMAContext context : map.get(osType)) {
                        logger.debug("      {}", context);
                    }
                }
            }
        }
    }

    private void dumpContextToBusinessAccountsIds(Map<SMAContext, Set<Long>> contextToBusinessAccountIds) {
        if (logger.isDebugEnabled()) {
            logger.debug("dump context to business account IDs for {} contexts",
                () -> contextToBusinessAccountIds.keySet().size());
            for (SMAContext context : contextToBusinessAccountIds.keySet()) {
                Set<Long> accountIds = contextToBusinessAccountIds.get(context);
                StringBuffer buffer = new StringBuffer();
                for (long id : accountIds) {
                    buffer.append(" ").append(id);
                }
                logger.debug("  {}: account IDs={}", () -> context, () -> buffer.toString());
            }
        }
    }

    private void dumpSmaContextsToTemplates(Map<SMAContext, Set<SMATemplate>> smaContextToTemplates) {
        if (logger.isDebugEnabled()) {
            logger.debug("{} context for templates",
                () -> smaContextToTemplates.keySet().size());
            for (SMAContext context : smaContextToTemplates.keySet()) {
                logger.trace("  context={}", context);
                for (SMATemplate template : smaContextToTemplates.get(context)) {
                    logger.trace("     {}", () -> template);
                }
            }
        }
    }

    private void dumpComputeTierOidToContextToTemplate(Table<Long, SMAContext, SMATemplate>
                                                           computeTierOidToContextToTemplate) {
        if (logger.isTraceEnabled()) {
            int counter = 0;
            logger.trace("dump compute tier OID to context to templates for {} compute tiers",
                () -> computeTierOidToContextToTemplate.rowKeySet().size());
            for (Long computeTierOid : computeTierOidToContextToTemplate.rowKeySet()) {
                logger.trace("  computer tier OID={}", computeTierOid);
                Map<SMAContext, SMATemplate> entry = computeTierOidToContextToTemplate.row(computeTierOid);
                for (SMAContext context : entry.keySet()) {
                    SMATemplate template = entry.get(context);
                    logger.trace("    context={}", context);
                    logger.trace("      template={}", template);
                }
                if (counter++ > 20) {
                    break;
                }
            }
        }
    }

    private void dumpSmaContextsToRIs(Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs) {
        if (logger.isDebugEnabled()) {
            logger.debug("context to RIs for {} contexts", () -> smaContextToRIs.keySet().size());
            for (SMAContext context : smaContextToRIs.keySet()) {
                logger.debug("  context={}", context);
                for (SMAReservedInstance ri : smaContextToRIs.get(context)) {
                    logger.debug("     RI={}", ri);
                }
            }
        }
    }

    /**
     * This class is to generate unique IDs for ReservedInstanceKey, which is used to aggregate
     * RIs together.
     */
    static class SMAReservedInstanceKeyIDGenerator {
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
     * Only VMs update the cache.  Compute tiers and RIs lookup the CSP in the cache.
     */
    public static class CspFromRegion {
        /*
         * Map from region OID to SMACSP.  Driven by regions where VM are found.
         */
        private Map<Long, SMACSP> regionIdToCspCache = new HashMap<>();

        /**
         * Given a region, update regionOidToCsp map.
         *
         * @param region region to process.
         */
        public void updateWithRegion(TopologyEntityDTO region) {
            long regionId = region.getOid();
            if (regionIdToCspCache.get(regionId) == null) {
                // not in the map
                String regionName = region.getDisplayName();
                // Determine CSP from region name.
                if (regionName.startsWith("aws")) {
                    regionIdToCspCache.put(regionId, SMACSP.AWS);
                } else if (regionName.startsWith("azure")) {
                    regionIdToCspCache.put(regionId, SMACSP.AZURE);
                } else {
                    logger.warn("getVMRegionId() region OID={} name={} has unknown CSP",
                        regionId, regionName);
                    regionIdToCspCache.put(regionId, SMACSP.UNKNOWN);
                }
            }
        }

        /**
         * Given a region OID, return the SMACSP from the cache.
         *
         * @param regionOid the region OID
         * @return if not found return UNKNOWN.
         */
        public SMACSP lookupWithRegionId(long regionOid) {
            SMACSP csp = regionIdToCspCache.get(regionOid);
            if (csp == null) {
                logger.trace("lookupWithRegionId no CSP found for region ID={}", regionOid);
                csp = SMACSP.UNKNOWN;
            }
            return csp;
        }
    }

    /**
     * Key to cache prices.
     */
    private static class PriceTableKey {
        private final long accountPricingKey;
        private final long regionKey;
        private final long computeTierKey;

        public PriceTableKey(long accountPricingKey, long regionKey, long computeTierKey) {
            this.accountPricingKey = accountPricingKey;
            this.regionKey = regionKey;
            this.computeTierKey = computeTierKey;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PriceTableKey that = (PriceTableKey)o;
            return accountPricingKey == that.accountPricingKey && regionKey == that.regionKey
                    && computeTierKey == that.computeTierKey;
        }

        @Override
        public int hashCode() {
            return Objects.hash(accountPricingKey, regionKey, computeTierKey);
        }
    }
}
