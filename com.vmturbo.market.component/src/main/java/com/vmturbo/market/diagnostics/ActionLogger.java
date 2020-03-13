package com.vmturbo.market.diagnostics;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ChangeProvider;
import com.vmturbo.common.protobuf.action.ActionDTO.Move;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.market.cloudscaling.sma.analysis.SMAUtils;
import com.vmturbo.market.cloudscaling.sma.entities.SMACSP;
import com.vmturbo.market.cloudscaling.sma.entities.SMAContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAOutputContext;
import com.vmturbo.market.cloudscaling.sma.entities.SMAReservedInstance;
import com.vmturbo.market.cloudscaling.sma.entities.SMATemplate;
import com.vmturbo.market.cloudscaling.sma.entities.SMAVirtualMachine;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput.CspFromRegion;
import com.vmturbo.market.cloudscaling.sma.entities.SMAMatch;
import com.vmturbo.market.runner.MarketMode;
import com.vmturbo.market.topology.conversions.ConsistentScalingHelper;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * This class externalizes a list of actions or the SMA output to the log in common separated value (CSV) format.
 *
 * <p>The externalization only occurs if 1) the log level of this class is set to debug, or market
 * mode is M2WithSMAActions.  If M2withSMAActions market mode is chosen, then both M2 and SMA actions
 * are logged in CSV format, allowing comparisons.
 *
 *
 */
public class ActionLogger {

    private static final Logger logger = LogManager.getLogger();

    // to easily find the data in the log
    private static final String prefix = "loggedActions";

    private static final String header = "market," + prefix + ",engine,CSP,billingFamily,businessAccount,region," +
        "OSType,Tenancy,vmName,vmOid,vmGroupName,savingsPerHour," +
        "sourceTemplate,sourceCoupons,natrualTemplate,naturalCoupons,sourceRI,RITemplate,RICoupons," +
        "projectedTemplate,projectedCoupons,projectedRI,RITemplate,RICoupons," +
        "templateChange,familyChange";
    /*
     * What is written to the log for each action
     */
    private String engine;
    private SMACSP csp;
    private String billingFamilyName;
    private String businessAccountName;
    private String regionName;
    private String platformName;
    private String tenancyName;
    private String virtualMachineName;
    private String virtualMachineGroupName;
    private long virtualMachineOid;
    private float savingsPerHour;
    private String sourceTemplateName;
    private String sourceFamilyName;
    private int sourceTemplateCoupons;
    private String naturalTemplateName;
    private int naturalTemplateCoupons;
    private String sourceReservedInstanceName;
    private String sourceReservedInstanceTemplateName;
    private float sourceReservedInstanceCouponsApplied;
    private String projectedTemplateName;
    private String projectedFamilyName;
    private int projectedTemplateCoupons;
    private String projectedReservedInstanceName;
    private String projectedReservedInstanceTemplateName;
    private float projectedReservedInstanceCouponsApplied;
    private int templateChange;
    private int familyChange;

    private CspFromRegion cspFromRegion = new CspFromRegion();

    /**
     * Given a list of actions, log each action in CSV format.
     *
     * @param actions                           list of actions to be logged
     * @param enabledSMA                        true if SMA is the engine, otherwise M2 is the engine
     * @param marketMode                        mode that market is running in.
     * @param sourceCloudTopology               source cloud topology
     * @param projectedCloudTopology            cloud dictionary for projected topology
     * @param cloudCostData                     dictionary of cloud costs, determine source templates RI coverage
     * @param projectedReservedInstanceCoverage projected RI coverage
     * @param consistentScalingHelper           used to figure out the consistent scaling information.
     */
    public void logActions(List<Action> actions,
                           boolean enabledSMA,
                           MarketMode marketMode,
                           @Nonnull final CloudTopology<TopologyEntityDTO> sourceCloudTopology,
                           @Nonnull final CloudTopology<TopologyEntityDTO> projectedCloudTopology,
                           @Nonnull final CloudCostData cloudCostData,
                           @Nonnull final Map<Long, EntityReservedInstanceCoverage> projectedReservedInstanceCoverage,
                           @Nonnull ConsistentScalingHelper consistentScalingHelper) {
        Objects.requireNonNull(sourceCloudTopology, "sourceCloudTopology == null");
        Objects.requireNonNull(projectedCloudTopology, "projectedCloudTopology == null");
        Objects.requireNonNull(cloudCostData, "cloudCostData == null");
        Objects.requireNonNull(projectedReservedInstanceCoverage, "projectedReservedInstanceCoverage == null");
        Objects.requireNonNull(consistentScalingHelper, "consistentScalingHelper == null");

        if (!(marketMode == MarketMode.M2withSMAActions || logger.isDebugEnabled())) {
            return;
        }
        final Stopwatch stopWatchRefined = Stopwatch.createStarted();
        StringBuffer buffer = new StringBuffer();
        buffer.append(header).append("\n");
        if (enabledSMA) {
            engine = "SMA";
        } else {
            engine = "M2";
        }
        for (Action action : actions) {
            initialize();
            ActionInfo info = action.getInfo();
            if (!info.hasMove()) {
                logger.trace("logActions: not a MOVE action");
                continue;
            }
            Move move = info.getMove();
            savingsPerHour = SMAUtils.format4Digits((float)action.getSavingsPerHour().getAmount());

            // Virtual Machine and RI coverage
            ActionEntity actionEntity = move.getTarget();
            processVM(actionEntity, sourceCloudTopology, cloudCostData, projectedReservedInstanceCoverage,
                consistentScalingHelper);
            if (virtualMachineOid == LONG_UNKNOWN) {
                logger.trace("logActions: ActionEntity OID={} is not a VM", actionEntity.getId());
                continue;
            }

            ChangeProvider changeProvider = move.getChanges(0);
            // Source Template
            ActionEntity sourceActionEntity = changeProvider.getSource();
            processTemplate(sourceActionEntity, sourceCloudTopology, true);

            // Projected Template
            ActionEntity projectedActionEntity = changeProvider.getDestination();
            processTemplate(projectedActionEntity, projectedCloudTopology, false);
            setChange();
            addRow(buffer);
        }
        logger.info(buffer.toString());
        logger.info("logActions: time to dump actions {}ms", stopWatchRefined.elapsed(TimeUnit.MILLISECONDS));
    }

    /**
     * Given the SMAOutput, log each match in CSV format.
     *
     * @param smaOutput                         Output of SMA
     * @param sourceCloudTopology               source cloud topology
     * @param projectedCloudTopology            projected cloud topology
     * @param cloudCostData                     dictionary of cloud costs, determine source templates RI coverage
     * @param projectedReservedInstanceCoverage projected RI coverage
     * @param consistentScalingHelper           used to figure out the consistent scaling information.
     */
    public void logSMAOutput(SMAOutput smaOutput,
                           @Nonnull final CloudTopology<TopologyEntityDTO> sourceCloudTopology,
                           @Nonnull final CloudTopology<TopologyEntityDTO> projectedCloudTopology,
                           @Nonnull final CloudCostData cloudCostData,
                           @Nonnull final Map<Long, EntityReservedInstanceCoverage> projectedReservedInstanceCoverage,
                           @Nonnull ConsistentScalingHelper consistentScalingHelper) {
        Objects.requireNonNull(sourceCloudTopology, "sourceCloudTopology == null");
        Objects.requireNonNull(projectedCloudTopology, "projectedCloudTopology == null");
        Objects.requireNonNull(cloudCostData, "cloudCostData == null");
        Objects.requireNonNull(projectedReservedInstanceCoverage, "projectedReservedInstanceCoverage == null");
        Objects.requireNonNull(consistentScalingHelper, "consistentScalingHelper == null");
        final Stopwatch stopWatchRefined = Stopwatch.createStarted();
        StringBuffer buffer = new StringBuffer();
        buffer.append(header).append("\n");
        engine = "SMA";

        for (SMAOutputContext outputContext: smaOutput.getContexts()) {
            SMAContext context = outputContext.getContext();
            long regionId = context.getRegionId();
            Optional<TopologyEntityDTO> optionalDto = sourceCloudTopology.getEntity(regionId);
            if (!optionalDto.isPresent()) {
                logger.error("logSmaOutput: can't find region DTO for OID={} in context={}",
                    regionId, context);
                continue;
            }
            for (SMAMatch match : outputContext.getMatches()) {
                initialize();
                csp = context.getCsp();
                tenancyName = context.getTenancy().name();
                regionName = optionalDto.get().getDisplayName();
                processMatch(match, sourceCloudTopology, buffer);
            }
        }
        logger.info(buffer.toString());
        logger.info("logSMAOutput: time to dump actions {}ms", stopWatchRefined.elapsed(TimeUnit.MILLISECONDS));
    }

    /**
     * Given a match, log it in CSV format.
     * @param match              the match
     * @param sourceCloudTopology source cloud topology is a dictionary
     * @param buffer              output buffer to be written to the log
     */
    private void processMatch(SMAMatch match,
                              CloudTopology<TopologyEntityDTO> sourceCloudTopology,
                              StringBuffer buffer) {
        SMAVirtualMachine vm = match.getVirtualMachine();
        virtualMachineOid = vm.getOid();
        OSType osType = vm.getOsType();
        platformName = osType.name();
        virtualMachineName = vm.getName();
        virtualMachineGroupName = vm.getGroupName();
        SMATemplate naturalTemplate = vm.getNaturalTemplate();
        if (naturalTemplate != null) {
            naturalTemplateName = naturalTemplate.getName();
            naturalTemplateCoupons = naturalTemplate.getCoupons();
        }
        if (vm.getCurrentRI() != null && vm.getCurrentRI().getOid() != SMAUtils.UNKNOWN_OID) {
            SMAReservedInstance ri = vm.getCurrentRI();
            sourceReservedInstanceCouponsApplied = vm.getCurrentRICoverage();
            sourceReservedInstanceName = ri.getName();
            sourceReservedInstanceTemplateName = ri.getTemplate().getName();
        }
        long businessAccountId = vm.getBusinessAccountId();
        billingFamilyName = getBillingFamilyName(businessAccountId, sourceCloudTopology);
        Optional<TopologyEntityDTO> optionalDto = sourceCloudTopology.getEntity(businessAccountId);
        if (!optionalDto.isPresent()) {
            logger.error("logSmaOutput: can't find businessAccount DTO for OID={} in match={}",
                businessAccountId, match);
            return;
        }
        businessAccountName = optionalDto.get().getDisplayName();

        SMATemplate sourceTemplate = vm.getCurrentTemplate();
        sourceTemplateName = sourceTemplate.getName();
        sourceFamilyName = sourceTemplate.getFamily();
        sourceTemplateCoupons = sourceTemplate.getCoupons();

        SMATemplate projectedTemplate = match.getTemplate();
        projectedTemplateName = projectedTemplate.getName();
        projectedFamilyName = projectedTemplate.getFamily();
        projectedTemplateCoupons = projectedTemplate.getCoupons();

        SMAReservedInstance ri = match.getReservedInstance();
        if (ri != null) {
            setProjectedReservedInstanceAttributes(ri, match);
        }

        float sourceCost = sourceTemplate.getNetCost(businessAccountId, osType,
            (sourceReservedInstanceCouponsApplied == FLOAT_UNKNOWN ? 0 : sourceReservedInstanceCouponsApplied));
        float projectedCost = projectedTemplate.getNetCost(businessAccountId, osType,
            (projectedReservedInstanceCouponsApplied == FLOAT_UNKNOWN ? 0 : projectedReservedInstanceCouponsApplied));
        savingsPerHour = SMAUtils.format4Digits(sourceCost - projectedCost);

        setChange();
        if (!(sourceTemplate.equals(projectedTemplate) && savingsPerHour == 0.0f)) {
            // don't add row if VM is not scaled.
            addRow(buffer);
        }
    }

    private void setProjectedReservedInstanceAttributes(SMAReservedInstance ri, SMAMatch match) {
        projectedReservedInstanceName = ri.getName();
        projectedReservedInstanceTemplateName = ri.getTemplate().getName();
        projectedReservedInstanceCouponsApplied = match.getDiscountedCoupons();
    }

    /**
     * determine if template or family changes in the scale action.
     */
    private void setChange() {
        if (!sourceTemplateName.equals(projectedTemplateName)) {
            templateChange = 1;
        }
        if (!sourceFamilyName.equals(projectedFamilyName)) {
            familyChange = 1;
        }
    }

    private void addRow(StringBuffer buffer) {
        buffer.append(",")
            .append(prefix). append(",")
            .append(engine).append(",")
            .append(csp.name()).append(",")
            .append(billingFamilyName).append(",")
            .append(businessAccountName).append(",")
            .append(regionName).append(",")
            .append(platformName).append(",")
            .append(tenancyName).append(",")
            .append(virtualMachineName).append(",")
            .append(virtualMachineOid).append(",")
            .append(virtualMachineGroupName).append(",")
            .append(savingsPerHour).append(",")
            .append(sourceTemplateName).append(",")
            .append(sourceTemplateCoupons).append(",")
            .append(naturalTemplateName).append(",")
            .append(naturalTemplateCoupons == INT_UNKNOWN ? "-" : naturalTemplateCoupons).append(",")
            .append(sourceReservedInstanceName).append(",")
            .append(sourceReservedInstanceTemplateName).append(",")
            .append(sourceReservedInstanceCouponsApplied == FLOAT_UNKNOWN ? "-" : sourceReservedInstanceCouponsApplied).append(",")
            .append(projectedTemplateName).append(",")
            .append(projectedTemplateCoupons).append(",")
            .append(projectedReservedInstanceName).append(",")
            .append(projectedReservedInstanceTemplateName).append(",")
            .append(projectedReservedInstanceCouponsApplied == FLOAT_UNKNOWN ? "-" : projectedReservedInstanceCouponsApplied).append(",")
            .append(templateChange).append(",")
            .append(familyChange)
            .append("\n");
    }

    /**
     * Process the virtual machine action entity.
     * Update the instance variables.
     *
     * @param actionEntity                      the VM action entity.
     * @param sourceCloudTopology               source cloud topology.
     * @param cloudCostData                     cloud cost data, need to find the original RI coverage
     * @param projectedReservedInstanceCoverage used to find the projected RI coverage
     * @param consistentScalingHelper           used to figure out the consistent scaling information.
     */
    private void processVM(ActionEntity actionEntity,
                           CloudTopology<TopologyEntityDTO> sourceCloudTopology,
                           final CloudCostData cloudCostData,
                           final Map<Long, EntityReservedInstanceCoverage> projectedReservedInstanceCoverage,
                           ConsistentScalingHelper consistentScalingHelper) {
        long oid = actionEntity.getId();
        if (actionEntity.getEnvironmentType() != EnvironmentType.CLOUD) {
            logger.trace("logActions: ActionEntity OID={} environmentType={} != CLOUD={} skip",
                oid, actionEntity.getEnvironmentType(), EnvironmentType.CLOUD);
            return;
        }
        if (actionEntity.getType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            logger.trace("logActions: ActionEntity OID={} entityType={} != virtualMachineType={} skip",
                oid, actionEntity.getType(), EntityType.VIRTUAL_MACHINE_VALUE);
            return;
        }
        Optional<TopologyEntityDTO> optional = sourceCloudTopology.getEntity(oid);
        if (!optional.isPresent()) {
            logger.trace("logActions: ActionEntity OID={} not found in source cloud topology", oid);
            return;
        }
        TopologyEntityDTO vmEntity = optional.get();
        if (vmEntity.getEntityType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            logger.error("logActions: VM OID={} entityType={} is not a VM={}: ID={} name={}",
                oid, vmEntity.getEntityType(), EntityType.VIRTUAL_MACHINE_VALUE, oid, virtualMachineName);
            return;
        }
        if (!vmEntity.getTypeSpecificInfo().hasVirtualMachine()) {
            logger.error("logActions: VM OID={} name={} doesn't have VirutalMachineInfo",
                oid, virtualMachineName);
            return;
        }
        virtualMachineName = vmEntity.getDisplayName();
        virtualMachineOid = oid;
        Optional<String> optionalString = consistentScalingHelper.getScalingGroupId(oid);
        virtualMachineGroupName = SMAUtils.NO_GROUP_ID;
        if (optionalString.isPresent()) {
            virtualMachineGroupName = optionalString.get();
        }
        VirtualMachineInfo vmInfo = vmEntity.getTypeSpecificInfo().getVirtualMachine();
        tenancyName = vmInfo.getTenancy().name();
        String osName = vmInfo.getGuestOsInfo().getGuestOsType().name();
        OSType osType = OSType.valueOf(osName);
        platformName = osType.name();
        optional = sourceCloudTopology.getConnectedRegion(oid);
        if (!optional.isPresent()) {
            logger.error("logActions: can't find region for VM OID={} name={}",
                oid, virtualMachineName);
        } else {
            TopologyEntityDTO region = optional.get();
            cspFromRegion.updateWithRegion(region);
            regionName = region.getDisplayName();
            csp = cspFromRegion.lookupWithRegionId(region.getOid());
        }
        businessAccountName = getBusinessAccountName(oid, sourceCloudTopology);
        long businessAccountId = SMAInput.getBusinessAccountId(oid, sourceCloudTopology, "externalize");
        billingFamilyName = getBillingFamilyName(businessAccountId, sourceCloudTopology);


        // RI coverage of source template
        final Optional<EntityReservedInstanceCoverage> originalRiCoverage =
            cloudCostData.getRiCoverageForEntity(oid);
        if (originalRiCoverage.isPresent()) {
            Map<Long, Double> originalRiCoverageMap = originalRiCoverage.get().getCouponsCoveredByRiMap();
            updateRiCoverage(originalRiCoverageMap, cloudCostData, sourceCloudTopology, true);
        }

        // RI coverage of target template
        final EntityReservedInstanceCoverage projectedRiCoverage =
            projectedReservedInstanceCoverage.get(oid);
        if (projectedRiCoverage != null) {
            Map<Long, Double> projectedRiCoverageMap = projectedRiCoverage.getCouponsCoveredByRiMap();
            updateRiCoverage(projectedRiCoverageMap, cloudCostData, sourceCloudTopology, false);
        }
    }

    /**
     * Compute the RI coverage.
     *
     * @param riCoverageMap map from RI OID to coupons used
     * @param cloudCostData cost data dictionary
     * @param cloudTopology cloud topology
     * @param isSource      is source RI?
     */
    private void updateRiCoverage(final Map<Long, Double> riCoverageMap,
                                  final CloudCostData cloudCostData,
                                  final CloudTopology<TopologyEntityDTO> cloudTopology,
                                  final boolean isSource) {
        String type = isSource ? "source" : "projeced";
        if (riCoverageMap.size() <= 0) {
            logger.trace("logActions: VM OID={} name={} has no RI coverage for {} Template",
                virtualMachineOid, virtualMachineName, type);
            return;
        }
        // grab the first RI
        Long oid = riCoverageMap.keySet().iterator().next();
        if (oid == null) {
            logger.error("logActions: VM OID={} name={} {} RI coverage map has OID == null",
                virtualMachineOid, virtualMachineName, type);
            return;
        }
        // existing RI?
        Optional<ReservedInstanceData> optional = cloudCostData.getExistingRiBoughtData(oid);
        if (!optional.isPresent()) {
            // bought RI?
            optional = cloudCostData.getBuyRIData(oid);
        }
        if (!optional.isPresent()) {
            logger.error("logActions: could not find RI OID={} in either existing or bought RIs", oid);
            return;
        } else {
            ReservedInstanceData riData = optional.get();
            ReservedInstanceBought riBought = riData.getReservedInstanceBought();
            ReservedInstanceBoughtInfo riBoughtInfo = riBought.getReservedInstanceBoughtInfo();
            String reservedInstanceName = riBoughtInfo.getProbeReservedInstanceId();
            boolean shared = riBoughtInfo.getReservedInstanceScopeInfo().getShared();
            ReservedInstanceSpec riSpec = riData.getReservedInstanceSpec();
            ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();
            boolean platformFlexible = riSpecInfo.getPlatformFlexible();
            boolean isf = riSpecInfo.getSizeFlexible();
            long computeTierOid = riSpecInfo.getTierId();
            String templateName = STRING_UNKNOWN;
            Optional<TopologyEntityDTO> computeTier = cloudTopology.getEntity(computeTierOid);
            if (computeTier.isPresent()) {
                templateName = computeTier.get().getDisplayName();
            }
            float coupons = computeCouponsUsed(riCoverageMap, oid, type);
            float couponsApplied = SMAUtils.format4Digits(coupons);
            if (isSource) {
                sourceReservedInstanceName = reservedInstanceName;
                sourceReservedInstanceTemplateName = templateName;
                sourceReservedInstanceCouponsApplied = couponsApplied;
            } else {
                projectedReservedInstanceName = reservedInstanceName;
                projectedReservedInstanceTemplateName = templateName;
                projectedReservedInstanceCouponsApplied = couponsApplied;
            }
        }
    }

    /**
     * Given a RI coverage Map, sum up the coupons.
     *
     * @param riCoverageMap RI coverage map
     * @param oid           key to first entry
     * @param type          source or projected.
     * @return return the sum of the coupons.
     */
    @VisibleForTesting
    float computeCouponsUsed(final Map<Long, Double> riCoverageMap, long oid, String type) {
        Double coupons = new Double(0);
        if (riCoverageMap.size() > 1) {
            // this must be ISF and all the RIs are of the same instance type.  Add up coupons.
            Iterator iterator = riCoverageMap.keySet().iterator();
            while (iterator.hasNext()) {
                Long riOid = (Long)iterator.next();
                if (riOid == null) {
                    logger.error("logActions: VM OID={} name={} {} RI coverage map has OID == null",
                        virtualMachineOid, virtualMachineName, type);
                    continue;
                }
                Double value = riCoverageMap.get(riOid);
                if (value == null) {
                    logger.error("logActions: VM name={} {} RI coverage map has coupons == null",
                        virtualMachineName, type);
                } else {
                    coupons += value;
                }
            }
        } else {
            Double value = riCoverageMap.get(oid);
            if (value == null) {
                logger.error("logActions: VM name={} original RI coverage map has coupons == null",
                    virtualMachineName);
            } else {
                coupons = value;
            }
        }
        return SMAUtils.format4Digits(coupons.floatValue());
    }

    /**
     * Process the source template.
     *
     * @param sourceActionEntity  change provider source action entity.
     * @param sourceCloudTopology dictionary for projected cloud topology
     * @param isSource            true if source template, otherwise projected template
     */
    private void processTemplate(ActionEntity sourceActionEntity,
                                 CloudTopology<TopologyEntityDTO> sourceCloudTopology,
                                 boolean isSource) {
        long sourceTemplateOid = sourceActionEntity.getId();
        if (sourceActionEntity.getEnvironmentType() != EnvironmentType.CLOUD) {
            logger.trace("logActions: source ActionEntity OID={} environmentType={} != CLOUD={} skip",
                sourceTemplateOid, sourceActionEntity.getEnvironmentType(), EnvironmentType.CLOUD);
            return;
        }
        if (sourceActionEntity.getType() != EntityType.COMPUTE_TIER_VALUE) {
            logger.trace("logActions: sourceActionEntity OID={} entityType={} != computeTierType={} skip",
                sourceTemplateOid, sourceActionEntity.getType(), EntityType.COMPUTE_TIER_VALUE);
            return;
        }
        Optional<TopologyEntityDTO> optional = sourceCloudTopology.getEntity(sourceTemplateOid);
        TopologyEntityDTO templateEntity = optional.get();
        if (!templateEntity.getTypeSpecificInfo().hasComputeTier()) {
            logger.error("logActions: source template ID={} name={} doesn't have ComputeTierInfo",
                sourceTemplateOid, sourceTemplateName);
            return;
        }
        ComputeTierInfo computeTierInfo = templateEntity.getTypeSpecificInfo().getComputeTier();
        if (isSource) {
            sourceTemplateName = templateEntity.getDisplayName();
            sourceFamilyName = computeTierInfo.getFamily();
            sourceTemplateCoupons = computeTierInfo.getNumCoupons();
        } else {
            projectedTemplateName = templateEntity.getDisplayName();
            projectedFamilyName = computeTierInfo.getFamily();
            projectedTemplateCoupons = computeTierInfo.getNumCoupons();
        }
    }

    /*
     * Default values
     */
    private static String STRING_UNKNOWN = "-";
    private static long LONG_UNKNOWN = -1L;
    private static int INT_UNKNOWN = -1;
    private static float FLOAT_UNKNOWN = -1.0f;
    /**
     * initialize all the instance variables to unknown.
     */
    private void initialize() {
        csp = SMACSP.UNKNOWN;
        billingFamilyName = STRING_UNKNOWN;
        businessAccountName = STRING_UNKNOWN;
        regionName = STRING_UNKNOWN;
        platformName = OSType.UNKNOWN_OS.name();
        tenancyName = STRING_UNKNOWN;
        virtualMachineName = STRING_UNKNOWN;
        virtualMachineGroupName = STRING_UNKNOWN;
        savingsPerHour = FLOAT_UNKNOWN;
        sourceTemplateName = STRING_UNKNOWN;
        sourceFamilyName = STRING_UNKNOWN;
        sourceTemplateCoupons = INT_UNKNOWN;
        naturalTemplateName = STRING_UNKNOWN;
        naturalTemplateCoupons = INT_UNKNOWN;
        projectedTemplateName = STRING_UNKNOWN;
        projectedFamilyName = STRING_UNKNOWN;
        projectedTemplateCoupons = INT_UNKNOWN;
        sourceReservedInstanceName = STRING_UNKNOWN;
        sourceReservedInstanceTemplateName = STRING_UNKNOWN;
        sourceReservedInstanceCouponsApplied = FLOAT_UNKNOWN;
        projectedReservedInstanceName = STRING_UNKNOWN;
        projectedReservedInstanceTemplateName = STRING_UNKNOWN;
        projectedReservedInstanceCouponsApplied = FLOAT_UNKNOWN;
        virtualMachineName = STRING_UNKNOWN;
        virtualMachineOid = LONG_UNKNOWN;
        templateChange = 0;
        familyChange = 0;
    }

    /**
     * Given a business acount, find the billing family name.
     * In AWS, the master account is the billing family.
     * In Azure, the Enterprise Account is the billing family.
     *
     * @param oid           business account OID.
     * @param cloudTopology dictionary of cloud topology
     * @return billing family name
     */
    public static String getBillingFamilyName(long oid,
                                              @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
        Objects.requireNonNull(cloudTopology, "cloudTopology == null");
        String billingFamilyName = STRING_UNKNOWN;
        Optional<GroupAndMembers> optional = cloudTopology.getBillingFamilyForEntity(oid);
        if (!optional.isPresent()) {
            // if ID  is a master account, expect accountOpt to be empty
            logger.trace("getBillingFamilyName: can't find billing family ID for {} OID={}", oid);
        } else {
            GroupAndMembers groupAndMembers = optional.get();
            Grouping grouping = groupAndMembers.group();
            GroupDefinition groupDefinition = grouping.getDefinition();
            billingFamilyName = groupDefinition.getDisplayName();
        }
        return billingFamilyName;
    }

    /**
     * Find the business account's name.
     *
     * @param oid           ID  of topology entity
     * @param cloudTopology where to look for the business account
     * @return business account ID
     */
    public static String getBusinessAccountName(long oid,
                                                @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
        Objects.requireNonNull(cloudTopology, "cloudTopology == null");
        String businessAccountName = STRING_UNKNOWN;
        Optional<TopologyEntityDTO> optional = cloudTopology.getOwner(oid);
        if (!optional.isPresent()) {
            logger.error("getBusinessAccount: can't find owner for ID={}", oid);
        } else {
            long businessAccountId = optional.get().getOid();
            optional = cloudTopology.getEntity(businessAccountId);
            if (!optional.isPresent()) {
                logger.error("getBusinessAccount: can't find business account with ID={}", oid);
            } else {
                TopologyEntityDTO entity = optional.get();
                businessAccountName = entity.getDisplayName();
            }
        }
        return businessAccountName;
    }
}
