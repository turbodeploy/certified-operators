package com.vmturbo.market.cloudscaling.sma.entities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.Stream.Builder;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.ObjectUtils;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.cloud.common.topology.SimulatedTopologyEntityCloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.SimulatedCloudCostData;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.market.topology.conversions.ReservedInstanceKey;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * The input to the Stable marriage algorithm.
 * Integration with XL:
 *   add a constructor to convert market data structures into SMA data structures.
 */
public class SMAInput {

    private static final Logger logger = LogManager.getLogger();

    private static final Map<SMACSP, Function<TopologyEntityDTO, CloudCommitmentAmount>>
            CSP_TO_COMMITMENT_AMOUNT_GETTER = Collections.singletonMap(SMACSP.GCP,
            tier -> Commitments.getCommoditiesCommitmentAmountByTier(
                    TopologyEntityInfoExtractor::getComputeTierPricingCommoditiesStatic, tier)
                    .orElseGet(CloudCommitmentAmount::getDefaultInstance));

    private static final Function<SMACSP, Function<TopologyEntityDTO, CloudCommitmentAmount>>
            COMMITMENT_AMOUNT_BY_CSP_GETTER = csp -> CSP_TO_COMMITMENT_AMOUNT_GETTER.getOrDefault(
            csp, Commitments::getCouponsCommitmentAmountByTier);

    /**
     * List of input contexts.
     */
    public final List<SMAInputContext> inputContexts;

    private final SMACloudCostCalculator smaCloudCostCalculator;

    public SMACloudCostCalculator getSmaCloudCostCalculator() {
        return smaCloudCostCalculator;
    }

    /**
     * Constructor for SMAInput.
     *
     * @param contexts the input to SMA partioned into contexts. Each element in list
     *         corresponds to a context.
     * @param smaCloudCostCalculator the {@link SMACloudCostCalculator}.
     */
    public SMAInput(@Nonnull final List<SMAInputContext> contexts,
            @Nonnull final SMACloudCostCalculator smaCloudCostCalculator) {
        this.inputContexts = Objects.requireNonNull(contexts, "contexts is null!");
        this.smaCloudCostCalculator = smaCloudCostCalculator;
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
     * @param consistentScalingHelper used to figure out the consistent scaling information.
     * @param isPlan is true if plan otherwise real time.
     * @param reduceDependency if true will reduce relinquishing
     * @param groupMemberRetriever the group memeber retriver
     */
    public SMAInput(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull Map<Long, Set<Long>> providers,
            @Nonnull CloudCostData<TopologyEntityDTO> cloudCostData,
            @Nonnull ConsistentScalingHelper consistentScalingHelper,
            boolean isPlan,
            boolean reduceDependency,
            @Nonnull GroupMemberRetriever groupMemberRetriever) {
        // check input parameters are not null
        Objects.requireNonNull(cloudTopology, "source cloud topology is null");
        Objects.requireNonNull(providers, "providers are null");
        Objects.requireNonNull(cloudCostData, "cloudCostData is null");
        cloudCostData.logMissingAccountPricingData();
        final Builder<TopologyEntityDTO> streamBuilder = Stream.builder();
        cloudTopology.getEntities().values().stream().forEach(streamBuilder);
        SimulatedTopologyEntityCloudTopology simulatedTopologyEntityCloudTopology =
                new SimulatedTopologyEntityCloudTopology(streamBuilder.build(),
                        groupMemberRetriever);
        SimulatedCloudCostData simulatedCloudCostData = new SimulatedCloudCostData<>(cloudCostData);
        this.smaCloudCostCalculator = new SMACloudCostCalculator(simulatedTopologyEntityCloudTopology, simulatedCloudCostData);
        final Stopwatch stopWatch = Stopwatch.createStarted();
        /*
         * maps from SMAContext to entities.  Needed to build SMAInputContexts.
         * The OSType in the context is OSTypeForContext, which is UNKNOWN for Azure.
         */
        Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs = new HashMap<>();
        Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs = new HashMap<>();
        Map<Long, SMATemplate> computeTierOidToSMATemplate = new HashMap<>();

        // data encapsulation of RI key ID generation.
        SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator =
                new SMAReservedInstanceKeyIDGenerator();

        // map from bought RI to SMA RI.
        Map<Long, SMAReservedInstance> riBoughtOidToRI = new HashMap<>();

        /*
         * For each virtual machines, create a VirtualMachine and partition into SMAContexts.
         */
        final Stopwatch stopWatchDetails = Stopwatch.createStarted();
        List<TopologyEntityDTO> virtualMachines =
                cloudTopology.getAllEntitiesOfType(EntityType.VIRTUAL_MACHINE_VALUE);
        int numberVMsCreated = 0;
        logger.info("process {} VMs", virtualMachines::size);
        for (TopologyEntityDTO vm : virtualMachines) {
            if (!(vm.getEntityState() == EntityState.POWERED_ON)) {
                logger.debug(" VM={} state={} != POWERED_ON", vm::getOid, () -> vm.getEntityState().name());
                continue;
            }
            numberVMsCreated++;
            processVirtualMachine(vm, cloudTopology, consistentScalingHelper,
                    smaContextToVMs, cloudCostData);
        }
        logger.info("{}ms to create {} VMs from {} VirtualMachines in {} contexts",
                stopWatchDetails.elapsed(TimeUnit.MILLISECONDS), numberVMsCreated, virtualMachines.size(),
                smaContextToVMs.keySet().size());

        /*
         * For each ComputeTier, create SMATemplates, but only in contexts where VMs exist.
         */
        stopWatchDetails.reset();
        stopWatchDetails.start();
        final List<TopologyEntityDTO> computeTiers = cloudTopology.getAllEntitiesOfType(
                EntityType.COMPUTE_TIER_VALUE);
        logger.info("process {} computeTiers", computeTiers::size);
        int numberTemplatesCreated = processComputeTiers(computeTiers,
                computeTierOidToSMATemplate, cloudTopology);

        logger.info("{}ms to create {} templates from {} compute tiers in {} contexts",
                () -> stopWatchDetails.elapsed(TimeUnit.MILLISECONDS), () -> numberTemplatesCreated,
                computeTiers::size, () -> computeTierOidToSMATemplate.keySet().size());


        /*
         * For each the RI, create an SMAReservedInstance, but only in contexts where VMs exist.
         */
        stopWatchDetails.reset();
        stopWatchDetails.start();

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

        final Map<Long, CloudCommitmentData<TopologyEntityDTO>>
                cloudCommitmentDataByCloudCommitmentId =
                cloudCostData.getCloudCommitmentDataByCloudCommitmentId();

        // Lazy computation of family name to random tier mapping.
        // Select any template, late it will be normalized to the smallest in the family.
        final Supplier<Map<String, Long>> familyToRandomTierOid = Suppliers.memoize(
                () -> computeTiers.stream()
                        .collect(Collectors.toMap(
                                e -> e.getTypeSpecificInfo().getComputeTier().getFamily(),
                                TopologyEntityDTO::getOid, (a, b) -> a)));
        final Function<String, Optional<SMATemplate>>
                randomTemplateByFamilyAndContext = (family) -> Optional.ofNullable(
                familyToRandomTierOid.get().get(family)).flatMap(
                computeTierOid -> Optional.ofNullable(
                        computeTierOidToSMATemplate.get(computeTierOid)));

        final Function<CloudCommitmentData<TopologyEntityDTO>, Optional<Pair<SMAContext, SMAReservedInstance>>>
                processCommitment = commitment -> CommitmentProcessor.processCommitment(
                commitmentOid -> getRegionId(commitmentOid, cloudTopology),
                commitmentOid -> cloudTopology.getConnectedAvailabilityZone(commitmentOid)
                        .map(TopologyEntityDTO::getOid),
                commitmentOid -> entityToSMACSP(commitmentOid, cloudTopology) ,
                commitmentOid -> getBusinessAccountId(commitmentOid, cloudTopology, "CUD"),
                accountOid -> getBillingFamilyId(accountOid, cloudTopology, "CUD"),
                randomTemplateByFamilyAndContext,
                commitmentOid -> cloudTopology.getEntity(commitmentOid)
                        .map(Commitments::resolveCoveredAccountsByCommitment),
                reservedInstanceKeyIDGenerator::lookUpRIKey, commitment);

        final Stream<Pair<SMAContext, SMAReservedInstance>> riStream = allRIData.stream()
                .map(data -> processReservedInstance(data, cloudTopology, computeTierOidToSMATemplate,
                        reservedInstanceKeyIDGenerator))
                .filter(Optional::isPresent)
                .map(Optional::get);

        // Filtering out only supported GCP commitments.
        final Stream<Pair<SMAContext, SMAReservedInstance>> cudStream =
                cloudCommitmentDataByCloudCommitmentId.values()
                        .stream()
                        .map(processCommitment)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .filter(e -> e.getFirst().getCsp() == SMACSP.GCP);

        Stream.concat(riStream, cudStream).forEach(r -> {
            final SMAReservedInstance ri = r.getSecond();
            smaContextToRIs.computeIfAbsent(r.getFirst(), k -> new HashSet<>()).add(ri);
            riBoughtOidToRI.put(ri.getOid(), ri);
        });

        logger.info(
                "{}ms to create {} RIs from {} RI bought data, {} commitment data in {} contexts",
                stopWatchDetails.elapsed(TimeUnit.MILLISECONDS), riBoughtOidToRI.size(),
                allRIData.size(), cloudCommitmentDataByCloudCommitmentId.size(),
                smaContextToRIs.values().stream().mapToInt(Set::size).sum());

        /*
         * Update VM's current template and provider list.
         */
        stopWatchDetails.reset();
        stopWatchDetails.start();

        final LongFunction<Optional<Pair<SMAReservedInstance, CloudCommitmentAmount>>>
                entityCoverageGetter = entityOid -> Commitments.computeVmCoverage(
                CommitmentAmountCalculator::sum,
                Commitments.getRIOidToEntityCoverage(CommitmentAmountCalculator::sum,
                        cloudCostData.getFilteredRiCoverage(entityOid)
                                .orElseGet(EntityReservedInstanceCoverage::getDefaultInstance)
                                .getCouponsCoveredByRiMap(), Optional.ofNullable(
                                cloudCostData.getCloudCommitmentMappingByEntityId().get(entityOid))
                                .orElseGet(Collections::emptySet)), riBoughtOidToRI);

        Set<SMAContext> smaContexts = smaContextToVMs.keySet();
        logger.info("update VMS for {} contexts", smaContexts::size);
        for (SMAContext context : smaContexts) {
            Set<SMAVirtualMachine> vmsInContext = smaContextToVMs.get(context);
            updateVirtualMachines(entityCoverageGetter, vmsInContext, computeTierOidToSMATemplate,
                    providers,
                    cloudTopology);
        }
        logger.info("{}ms to update SMAVirtualMachines",
                stopWatchDetails.elapsed(TimeUnit.MILLISECONDS));

        /*
         * build input contexts.
         */
        stopWatchDetails.reset();
        stopWatchDetails.start();
        inputContexts = generateInputContexts(smaContextToVMs, smaContextToRIs,
                new HashSet<>(computeTierOidToSMATemplate.values()), reduceDependency);
        logger.info("{}ms to generate SMAInputContexts",
                stopWatchDetails.elapsed(TimeUnit.MILLISECONDS));
        logger.info("total {}ms to convert to SMA data structures",
                stopWatch.elapsed(TimeUnit.MILLISECONDS));
    }

    /**
     * Generate the set of input contexts.
     * @param smaContextToVMs         Map from context to set of VMs
     * @param smaContextToRIs         Map from context to set of RIs
     * @param smaTemplates            set of Templates
     * @param reduceDependency if true will reduce relinquishing
     * @return list of input contexts
     */
    @Nonnull
    private static List<SMAInputContext> generateInputContexts(Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs,
            Map<SMAContext, Set<SMAReservedInstance>> smaContextToRIs,
            Set<SMATemplate> smaTemplates,
            boolean reduceDependency) {
        List<SMAInputContext> inputContexts = new ArrayList<>();
        Set<SMAContext> smaContexts = smaContextToVMs.keySet();
        logger.info("build input contexts for {} contexts", smaContexts::size);
        for (SMAContext context : smaContexts) {
            Set<SMAVirtualMachine> smaVMs = smaContextToVMs.get(context);
            if (ObjectUtils.isEmpty(smaVMs)) {
                // there may be a RI context that has no VMs.
                logger.error(" no VM for context={}", context);
                continue;
            }
            smaVMs.removeIf(vm -> vm.getCurrentTemplate() == null);
            Set<SMAReservedInstance> smaRIs = smaContextToRIs.getOrDefault(context, Collections.emptySet());
            if (ObjectUtils.isEmpty(smaTemplates)) {
                logger.error(" no template for context={}", context);
                continue;
            }
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
     * @param smaContextToVMs             map from SMA context to set of SMA virtual machines, updated
     * @param cloudCostData               the cloud cost data
     */
    private void processVirtualMachine(final TopologyEntityDTO entity,
            final CloudTopology<TopologyEntityDTO> cloudTopology,
            final ConsistentScalingHelper consistentScalingHelper,
            Map<SMAContext, Set<SMAVirtualMachine>> smaContextToVMs,
            @Nonnull CloudCostData<TopologyEntityDTO> cloudCostData) {
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
        long regionId = getRegionId(oid, cloudTopology);
        SMACSP csp = entityToSMACSP(regionId, cloudTopology);
        Optional<TopologyEntityDTO> zoneOptional = cloudTopology.getConnectedAvailabilityZone(oid);
        long zoneId = SMAUtils.NO_ZONE;
        if (zoneOptional.isPresent()) {
            zoneId = zoneOptional.get().getOid();
        } else if (csp != SMACSP.AZURE) {
            // Azure currently does not support availability zones.
            logger.error("processVM: VM OID={} name={} can't find availabilty zone", oid, name);
            return;
        }

        final OSType osTypeForContext = getOsTypeForContext(osType, csp);
        final Tenancy tenancyForContext = getTenancyForContext(tenancy, csp);
        long businessAccountId = getBusinessAccountId(oid, cloudTopology, "VM");
        long billingFamilyId = getBillingFamilyId(businessAccountId, cloudTopology, "VM");
        final SMAContext context = new SMAContext(csp, osTypeForContext, regionId, billingFamilyId,
                tenancyForContext);
        logger.debug("processVM: new {}  osType={}  accountId={}", context, osType,
                businessAccountId);

        Optional<String> groupIdOptional = consistentScalingHelper.getScalingGroupId(oid);
        String groupName = SMAUtils.NO_GROUP_ID;
        if (groupIdOptional.isPresent()) {
            groupName = groupIdOptional.get();
        }

        Optional<AccountPricingData<TopologyEntityDTO>> accountPricingData =
                cloudCostData.getAccountPricingData(businessAccountId);
        if (!accountPricingData.isPresent()) {
            logger.error("processVM: can't find accountPricing data for ID={}", businessAccountId);
            return;
        }
        long accountPricingDataOid = accountPricingData.get().getAccountPricingDataOid();
        /*
         * Create Virtual Machine.
         */
        SMAVirtualMachine vm = new SMAVirtualMachine(oid,
                name,
                groupName,
                businessAccountId,
                null,
                new ArrayList<>(),
                CommitmentAmountCalculator.ZERO_COVERAGE,
                zoneId,
                SMAUtils.BOGUS_RI,
                osType,
                vmInfo.getLicenseModel(),
                false,
                new ArrayList<>(),
                null,
                new HashMap<>(),
                regionId,
                accountPricingDataOid);
        logger.debug("processVM: new VM {}", vm);

        smaContextToVMs.computeIfAbsent(context, k -> new HashSet<>()).add(vm);
    }

    private static OSType getOsTypeForContext(@Nonnull final OSType originalOsType,
            @Nonnull final SMACSP csp) {
        return SMAUtils.CSP_WITH_UNKNOWN_OS.contains(csp) ? SMAUtils.UNKNOWN_OS_TYPE_PLACEHOLDER
                : originalOsType;
    }

    private static Tenancy getTenancyForContext(@Nonnull final Tenancy originalTenancy,
            @Nonnull final SMACSP csp) {
        return SMAUtils.CSP_WITH_UNKNOWN_TENANCY.contains(csp)
                ? SMAUtils.UNKNOWN_TENANCY_PLACEHOLDER : originalTenancy;
    }

    /**
     * For each VM, updates its current template, currentRICoverage, and providers.
     * This must be run after the compute tiers are processed and the SMATemplates are created.
     *
     * @param entityCoverageGetter the entity coverage getter.
     * @param vms the set of VMs that must be updated.
     * @param computeTierOidToSMATemplate map from computeTier ID to context to
     *         template.
     * @param providersList map from VM ID to its set of computeTier IDs.
     * @param cloudTopology dictionary.
     */
    private void updateVirtualMachines(
            @Nonnull final LongFunction<Optional<Pair<SMAReservedInstance, CloudCommitmentAmount>>> entityCoverageGetter,
            final Set<SMAVirtualMachine> vms,
            final Map<Long, SMATemplate> computeTierOidToSMATemplate,
            final Map<Long, Set<Long>> providersList,
            final CloudTopology<TopologyEntityDTO> cloudTopology) {
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
                currentTemplate = computeTierOidToSMATemplate.get(computeTierID);
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
                    SMATemplate template = computeTierOidToSMATemplate.get(providerId);
                    if (template == null) {
                        logger.error(
                                "updateVMs: VM ID={} name={} no providerID={} in computeTierToContextToTemplateMap",
                                oid, name, providerId);      // currently expected
                    } else {
                        providers.add(template);
                    }
                }
            }
            SMAVirtualMachineProvider smaVirtualMachineProvider =
                    smaCloudCostCalculator.updateProvidersOfVirtualMachine(providers,
                            vm.getCurrentTemplate(), vm);
            vm.setVirtualMachineProviderInfo(smaVirtualMachineProvider);
            entityCoverageGetter.apply(oid).ifPresent(currentRICoverage -> {
                vm.setCurrentRI(currentRICoverage.getFirst());
                vm.setCurrentRICoverage(currentRICoverage.getSecond());
            });
        }
    }

    /**
     * Given a set of computer tiers, generate the corresponding SMATemplates.
     * A compute tier does not specify either a tenancy, a billing family or a business account.
     * Only generate SMATemplates that are needed by the Virtual machines that may be scaled.
     * Partition compute tiers by region ID to ensure compute tiers match with the region's CSP.
     *
     * @param computeTiers Set of compute tiers in this cloud topology.
     * @param computeTierOidToSMATemplate compute tier ID to context to Template
     *         map, to be updated
     * @return true if a template is created, else false
     */
    private static int processComputeTiers(final List<TopologyEntityDTO> computeTiers,
            final Map<Long, SMATemplate> computeTierOidToSMATemplate,
            CloudTopology<TopologyEntityDTO> cloudTopology) {
            int numberTemplatesCreated = 0;
            // set of compute tiers in the valid region ID
            for (TopologyEntityDTO computeTier : computeTiers) {
                boolean created = processComputeTier(computeTier,
                        computeTierOidToSMATemplate, cloudTopology);
                if (created) {
                    numberTemplatesCreated++;
                }
            }
            return numberTemplatesCreated;
    }



    /**
     * Given an topology entity that is a compute tier, generate SMATemplates.
     * @param computeTier Entity that is expected to be a computeTier.
     * @param computeTierOidTemplate the map to keep track of the SMATemplates created
     * @return true if a template is created, else false
     */
    private static boolean processComputeTier(TopologyEntityDTO computeTier,
            Map<Long, SMATemplate> computeTierOidTemplate,
            CloudTopology<TopologyEntityDTO> cloudTopology) {
        long oid = computeTier.getOid();
        SMACSP csp = entityToSMACSP(oid, cloudTopology);
        final Function<TopologyEntityDTO, CloudCommitmentAmount> commitmentAmountGetter =
                COMMITMENT_AMOUNT_BY_CSP_GETTER.apply(csp);
        String name = computeTier.getDisplayName();
        if (computeTier.getEntityType() != EntityType.COMPUTE_TIER_VALUE) {
            logger.error("processComputeTier: entity ID={} name={} is not a computeTier: ", oid,
                    name);
            return false;
        }
        if (!computeTier.getTypeSpecificInfo().hasComputeTier()) {
            logger.error("processComputeTier: entity ID={} name={} doesn't have ComputeTierInfo",
                    oid, name);
            return false;
        }
        ComputeTierInfo computeTierInfo = computeTier.getTypeSpecificInfo().getComputeTier();
        String family = computeTierInfo.getFamily();
        final CloudCommitmentAmount commitmentAmount = commitmentAmountGetter.apply(computeTier);

        final float penalty =
                (computeTierInfo.hasScalePenalty()) ? computeTierInfo.getScalePenalty().getPenalty()
                        : 0F;
        final SMATemplate template = new SMATemplate(oid, name, family, commitmentAmount,
                penalty);
        computeTierOidTemplate.put(oid, template);
        return true;
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
     * @param data TopologyEntityDTO that is an RI
     * @param cloudTopology topology to get business account and region.
     * @param reservedInstanceKeyIDGenerator ID generator for ReservedInstanceKey
     * @param computeTierOidToTemplate compute tier id to SMATemplate.
     *
     * @return optional of context and reserved instance
     */
    private static Optional<Pair<SMAContext, SMAReservedInstance>> processReservedInstance(
            final ReservedInstanceData data, final CloudTopology<TopologyEntityDTO> cloudTopology,
            final Map<Long, SMATemplate> computeTierOidToTemplate,
            final SMAReservedInstanceKeyIDGenerator reservedInstanceKeyIDGenerator) {
        ReservedInstanceBought riBought = data.getReservedInstanceBought();
        long riBoughtId = riBought.getId();
        ReservedInstanceBoughtInfo riBoughtInfo = riBought.getReservedInstanceBoughtInfo();
        long businessAccountId = riBoughtInfo.getBusinessAccountId();
        long billingFamilyId = getBillingFamilyId(businessAccountId, cloudTopology, "RI");
        String name = riBoughtInfo.getProbeReservedInstanceId();
        long zoneId = riBoughtInfo.getAvailabilityZoneId();
        zoneId = (zoneId == 0 ? SMAUtils.NO_ZONE : zoneId);
        boolean shared = riBoughtInfo.getReservedInstanceScopeInfo().getShared();

        ReservedInstanceSpec riSpec = data.getReservedInstanceSpec();
        ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();
        // Can't find template until after processComputeTier

        String tenancyName = riSpecInfo.getTenancy().name();
        Tenancy tenancy = Tenancy.valueOf(tenancyName);
        String osTypeName = riSpecInfo.getOs().name();
        // for Azure RIs, OSType is always UNKNOWN, because the RI is OSType agnostic.
        OSType osType = OSType.valueOf(osTypeName);
        long regionId = riSpecInfo.getRegionId();
        SMACSP csp = entityToSMACSP(regionId, cloudTopology);
        if (csp == null) {
            // no VMs found in this region, skip this RI.
            logger.trace("processRI: skip riBoughtId={} name={} no VMs in regionID={}", riBoughtId,
                    name, regionId);
            return Optional.empty();
        }
        final OSType osTypeForContext = getOsTypeForContext(osType, csp);
        final Tenancy tenancyForContext = getTenancyForContext(tenancy, csp);
        final SMAContext context = new SMAContext(csp, osTypeForContext, regionId, billingFamilyId,
                tenancyForContext);
        long computeTierOid = riSpecInfo.getTierId();
        if (Strings.isNullOrEmpty(name)) {
            logger.trace("RI name is null or empty for riBoughtID={}", riBoughtId);
            name = constructRIName(cloudTopology, regionId, riBoughtInfo);
        }
        // for Azure, this RI covers a set of templates, one for each OSType.
        SMATemplate template = computeTierOidToTemplate.get(computeTierOid);
        if (template == null) {
            logger.error(
                    "processRI: can't find template with ID={} in templateMap for RI boughtID={} name={}",
                    computeTierOid, riBoughtId, name);
            return Optional.empty();
        }
        ReservedInstanceKey reservedInstanceKey = new ReservedInstanceKey(data,
                template.getFamily(), billingFamilyId);
        long riKeyId = reservedInstanceKeyIDGenerator.lookUpRIKey(reservedInstanceKey, riBoughtId);

        Set<Long> scopedAccountsIds = shared ? Collections.emptySet() : ImmutableSet.copyOf(
                riBoughtInfo.getReservedInstanceScopeInfo().getApplicableBusinessAccountIdList());

        final double numberOfCoupons =
                riBoughtInfo.getReservedInstanceBoughtCoupons().getNumberOfCoupons();

        // adjust count of RI to reflect partial RI
        final float count = (float)(numberOfCoupons / template.getCommitmentAmount().getCoupons());

        final CloudCommitmentAmount commitmentAmount =
                CloudCommitmentAmount.newBuilder().setCoupons(numberOfCoupons).build();

        // Unfortunately, the probe returns ISF=true for metal templates.
        final SMAReservedInstance ri = new SMAReservedInstance(riBoughtId, riKeyId, name,
                businessAccountId, scopedAccountsIds, template, zoneId, count,
                riSpecInfo.getSizeFlexible(), shared, riSpecInfo.getPlatformFlexible(),
                commitmentAmount);
        return Optional.of(Pair.create(context, ri));
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
     * Find the region ID.
     * Side effect: update cspFromRegion with region.
     *
     * @param oid           ID  of topology entity
     * @param cloudTopology dictionary of cloud topoolgy
     * @return region ID
     */
    private static long getRegionId(final long oid,
            final @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
        long regionId = -1;
        TopologyEntityDTO region = null;
        Optional<TopologyEntityDTO> regionOpt = cloudTopology.getConnectedRegion(oid);
        if (!regionOpt.isPresent()) {
            logger.error("getRegion: can't find region for OID={}", oid);
        } else {
            region = regionOpt.get();
            regionId = region.getOid();
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
            logger.trace("getBillingFamilyId: can't find billing family ID for {} OID={}", msg, oid);
        } else {
            GroupAndMembers groupAndMembers = optional.get();
            billingFamilyId = groupAndMembers.group().getId();
        }
        return billingFamilyId;
    }

    public static  SMACSP entityToSMACSP(long entityId,
            final CloudTopology<TopologyEntityDTO> cloudTopology) {
        String cspDisplayName = cloudTopology.getServiceProvider(entityId)
                .map(TopologyEntityDTO::getDisplayName).orElse("");
        if (cspDisplayName.equalsIgnoreCase("AWS")) {
            return SMACSP.AWS;
        } else if (cspDisplayName.equalsIgnoreCase("Azure")) {
            return SMACSP.AZURE;
        } else if (cspDisplayName.equalsIgnoreCase("GCP")) {
            return SMACSP.GCP;
        } else {
            logger.warn("entity  OID={} name={} has unknown CSP",
                    entityId, cspDisplayName);
            return SMACSP.UNKNOWN;
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

    /**
     * This class is to generate unique IDs for ReservedInstanceKey, which is used to aggregate
     * RIs together.
     */
    static class SMAReservedInstanceKeyIDGenerator {
        // map from ReservedInstanceKey to riKeyID.  This is a one-to-one map.
        private Map<ReservedInstanceKey, Long> riKeyToOid = new HashMap<>();
        // map from RIBought OID to riKeyID.  Multiple RIBoughtIDs may map to a single riKeyID.
        private Map<Long, Long> riBoughtToRiKeyOID = new HashMap<>();
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
}
