package com.vmturbo.market.cloudscaling.sma.analysis;

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
import com.vmturbo.market.cloudscaling.sma.entities.SMACSP;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;
import com.vmturbo.market.cloudscaling.sma.entities.SMAInput.CspFromRegion;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;

/**
 * This class externalizes a list of actions in the log in common separated value (CSV) format.
 */
public class SMAExternalizeActions {

    private static final Logger logger = LogManager.getLogger();

    private static final String header =
        ", engine,CSP,billingFamily,businessAccount,region,OSType,Tenancy, vmName,vmOid,savingsPerHour, " +
            "sourceTemplate,sourceCoupons," +
            "source RI,RITemplate,RICoupons,RIShared,RIISF,RIPF, " +
            "projectedTemplate,projectedCoupons, " +
            "projected RI,RITemplate,RICoupons,RIShared,RIISF,RIPF";

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
    private long virtualMachineOid;
    private float savingsPerHour;
    private String sourceTemplateName;
    private int sourceTemplateCoupons;
    private String sourceReservedInstanceName;
    private String sourceReservedInstanceTemplateName;
    private float sourceReservedInstanceCouponsApplied;
    private String sourceReservedInstanceShared;
    private String sourceReservedInstanceISF;
    private String sourceReservedInstancePF;
    private String projectedTemplateName;
    private int projectedTemplateCoupons;
    private String projectedReservedInstanceName;
    private String projectedReservedInstanceTemplateName;
    private float projectedReservedInstanceCouponsApplied;
    private String projectedReservedInstanceShared;
    private String projectedReservedInstanceISF;
    private String projectedReservedInstancePF;

    private CspFromRegion cspFromRegion = new CspFromRegion();

    /**
     * Given a list of actions, log each action in CSV format.
     *
     * @param actions                           list of actions to be logged
     * @param enabledSMA                        true if SMA is enabled, otherwise M2 is run
     * @param sourceCloudTopology               source cloud topology
     * @param projectedCloudTopology            projected cloud topology
     * @param cloudCostData                     dictionary of cloud costs, determine source templates RI coverage
     * @param projectedReservedInstanceCoverage projected RI coverage
     */
    public void logActions(List<Action> actions,
                           boolean enabledSMA,
                           @Nonnull final CloudTopology<TopologyEntityDTO> sourceCloudTopology,
                           @Nonnull final CloudTopology<TopologyEntityDTO> projectedCloudTopology,
                           @Nonnull final CloudCostData cloudCostData,
                           @Nonnull final Map<Long, EntityReservedInstanceCoverage> projectedReservedInstanceCoverage) {
        Objects.requireNonNull(sourceCloudTopology, "sourceCloudTopology == null");
        Objects.requireNonNull(projectedCloudTopology, "projectedCloudTopology == null");
        Objects.requireNonNull(cloudCostData, "cloudCostData == null");
        Objects.requireNonNull(projectedReservedInstanceCoverage, "projectedReservedInstanceCoverage == null");

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
                logger.info("logActions: not a MOVE action");
                continue;
            }
            Move move = info.getMove();
            savingsPerHour = SMAUtils.formatDigits((float)action.getSavingsPerHour().getAmount());

            // Virtual Machine and RI coverage
            ActionEntity actionEntity = move.getTarget();
            processVM(actionEntity, sourceCloudTopology, cloudCostData, projectedReservedInstanceCoverage);
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

            addRow(buffer);
        }
        logger.info(buffer.toString());
        logger.info("time to dump actions {}ms", stopWatchRefined.elapsed(TimeUnit.MILLISECONDS));
    }

    private void addRow(StringBuffer buffer) {
        buffer.append(",")
            .append(engine).append(",")
            .append(csp.name()).append(",")
            .append(billingFamilyName).append(",")
            .append(businessAccountName).append(",")
            .append(regionName).append(",")
            .append(platformName).append(",")
            .append(tenancyName).append(",")
            .append(virtualMachineName).append(",")
            .append(virtualMachineOid).append(",")
            .append(savingsPerHour).append(",")
            .append(sourceTemplateName).append(",")
            .append(sourceTemplateCoupons).append(",")
            .append(sourceReservedInstanceName).append(",")
            .append(sourceReservedInstanceTemplateName).append(",")
            .append(sourceReservedInstanceCouponsApplied == FLOAT_UNKNOWN ? "-" : sourceReservedInstanceCouponsApplied).append(",")
            .append(sourceReservedInstanceShared).append(",")
            .append(sourceReservedInstanceISF).append(",")
            .append(sourceReservedInstancePF).append(",")
            .append(projectedTemplateName).append(",")
            .append(projectedTemplateCoupons).append(",")
            .append(projectedReservedInstanceName).append(",")
            .append(projectedReservedInstanceTemplateName).append(",")
            .append(projectedReservedInstanceCouponsApplied == FLOAT_UNKNOWN ? "-" : projectedReservedInstanceCouponsApplied).append(",")
            .append(projectedReservedInstanceShared).append(",")
            .append(projectedReservedInstanceISF).append(",")
            .append(projectedReservedInstancePF)
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
     */
    private void processVM(ActionEntity actionEntity,
                           CloudTopology<TopologyEntityDTO> sourceCloudTopology,
                           final CloudCostData cloudCostData,
                           final Map<Long, EntityReservedInstanceCoverage> projectedReservedInstanceCoverage) {
        long oid = actionEntity.getId();
        if (actionEntity.getEnvironmentType() != EnvironmentType.CLOUD) {
            logger.info("logActions: ActionEntity OID={} environmentType={} != CLOUD={} skip",
                oid, actionEntity.getEnvironmentType(), EnvironmentType.CLOUD);
            return;
        }
        if (actionEntity.getType() != EntityType.VIRTUAL_MACHINE_VALUE) {
            logger.info("logActions: ActionEntity OID={} entityType={} != virtualMachineType={} skip",
                oid, actionEntity.getType(), EntityType.VIRTUAL_MACHINE_VALUE);
            return;
        }
        Optional<TopologyEntityDTO> optional = sourceCloudTopology.getEntity(oid);
        if (!optional.isPresent()) {
            logger.info("logActions: ActionEntity OID={} not found in source cloud topology", oid);
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
            String name = riBoughtInfo.getProbeReservedInstanceId();
            boolean shared = riBoughtInfo.getReservedInstanceScopeInfo().getShared();
            ReservedInstanceSpec riSpec = riData.getReservedInstanceSpec();
            ReservedInstanceSpecInfo riSpecInfo = riSpec.getReservedInstanceSpecInfo();
            boolean platformFlexible = riSpecInfo.getPlatformFlexible();
            boolean isf = riSpecInfo.getSizeFlexible();
            long computeTierOid = riSpecInfo.getTierId();
            String templateDisplayName = STRING_UNKNOWN;
            Optional<TopologyEntityDTO> computeTier = cloudTopology.getEntity(computeTierOid);
            if (computeTier.isPresent()) {
                templateDisplayName = computeTier.get().getDisplayName();
            }
            if (isSource) {
                sourceReservedInstanceName = name;
                sourceReservedInstanceTemplateName = templateDisplayName;
                sourceReservedInstanceShared = Boolean.toString(shared);
                sourceReservedInstancePF = Boolean.toString(platformFlexible);
                sourceReservedInstanceISF = Boolean.toString(isf);
            } else {
                projectedReservedInstanceName = name;
                projectedReservedInstanceTemplateName = templateDisplayName;
                projectedReservedInstanceShared = Boolean.toString(shared);
                projectedReservedInstancePF = Boolean.toString(platformFlexible);
                projectedReservedInstanceISF = Boolean.toString(isf);
            }
            float coupons = computeCouponsUsed(riCoverageMap, oid, type);
            float couponsUsed = SMAUtils.formatDigits(coupons);
            if (isSource) {
                sourceReservedInstanceCouponsApplied = couponsUsed;
            } else {
                projectedReservedInstanceCouponsApplied = couponsUsed;
            }
        }

    }

    /**
     * Given a RI coverage Map, sum up the coupons.
     * @param riCoverageMap RI coverage map
     * @param oid key to first entry
     * @param type source or projected.
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
        return SMAUtils.formatDigits(coupons.floatValue());
    }

    /**
     * Process the source template.
     *
     * @param sourceActionEntity  change provider source action entity.
     * @param sourceCloudTopology dictionary for projected cloud topology
     * @param isSource true if source template, otherwise projected template
     */
    private void processTemplate(ActionEntity sourceActionEntity,
                                 CloudTopology<TopologyEntityDTO> sourceCloudTopology,
                                 boolean isSource) {
        long sourceTemplateOid = sourceActionEntity.getId();
        if (sourceActionEntity.getEnvironmentType() != EnvironmentType.CLOUD) {
            logger.info("logActions: source ActionEntity OID={} environmentType={} != CLOUD={} skip",
                sourceTemplateOid, sourceActionEntity.getEnvironmentType(), EnvironmentType.CLOUD);
            return;
        }
        if (sourceActionEntity.getType() != EntityType.COMPUTE_TIER_VALUE) {
            logger.info("logActions: sourceActionEntity OID={} entityType={} != computeTierType={} skip",
                sourceTemplateOid, sourceActionEntity.getType(), EntityType.COMPUTE_TIER_VALUE);
            return;
        }
        Optional<TopologyEntityDTO> optional = sourceCloudTopology.getEntity(sourceTemplateOid);
        TopologyEntityDTO templateEntity = optional.get();
        if (isSource) {
            sourceTemplateName = templateEntity.getDisplayName();
        } else {
            projectedTemplateName = templateEntity.getDisplayName();
        }
        if (!templateEntity.getTypeSpecificInfo().hasComputeTier()) {
            logger.error("logActions: source template ID={} name={} doesn't have ComputeTierInfo",
                sourceTemplateOid, sourceTemplateName);
            return;
        }
        ComputeTierInfo computeTierInfo = templateEntity.getTypeSpecificInfo().getComputeTier();
        if (isSource) {
            sourceTemplateCoupons = computeTierInfo.getNumCoupons();
        } else {
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
        savingsPerHour = FLOAT_UNKNOWN;
        sourceTemplateName = STRING_UNKNOWN;
        sourceTemplateCoupons = INT_UNKNOWN;
        projectedTemplateName = STRING_UNKNOWN;
        projectedTemplateCoupons = INT_UNKNOWN;
        sourceReservedInstanceName = STRING_UNKNOWN;
        sourceReservedInstanceTemplateName = STRING_UNKNOWN;
        sourceReservedInstanceCouponsApplied = FLOAT_UNKNOWN;
        sourceReservedInstanceShared = STRING_UNKNOWN;
        sourceReservedInstanceISF = STRING_UNKNOWN;
        sourceReservedInstancePF = STRING_UNKNOWN;
        projectedReservedInstanceName = STRING_UNKNOWN;
        projectedReservedInstanceTemplateName = STRING_UNKNOWN;
        projectedReservedInstanceCouponsApplied = FLOAT_UNKNOWN;
        projectedReservedInstanceShared = STRING_UNKNOWN;
        projectedReservedInstanceISF = STRING_UNKNOWN;
        projectedReservedInstancePF = STRING_UNKNOWN;
        virtualMachineName = STRING_UNKNOWN;
        virtualMachineOid = LONG_UNKNOWN;
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
