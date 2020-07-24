package com.vmturbo.cost.component.reserved.instance;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceScopeInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * This class is used to validate RI coverage assignments. Validation is necessary in pulling
 * coverage from the cloud provider bill, given matching criteria may have changed after the
 * bill was published (e.g. a VM has moved to a new profile or powered off. This is especially
 * relevant in cases where entity state changed due to action execution. Validation can also
 * be applied to supplemental allocations and market output as a sanity check.
 */
public class ReservedInstanceCoverageValidator {

    private static final DataMetricSummary VALIDATION_DURATION_METRIC_SUMMARY =
            DataMetricSummary.builder()
                    .withName("cost_ri_coverage_validation_duration_seconds")
                    .withHelp("Time taken to validate RI coverage, based on the current cloud topology.")
                    .withQuantile(0.5, 0.05)   // Add 50th percentile (= median) with 5% tolerated error
                    .withQuantile(0.9, 0.01)   // Add 90th percentile with 1% tolerated error
                    .withQuantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                    .withMaxAgeSeconds(60 * 60) // 60 mins.
                    .withAgeBuckets(10) // 10 buckets, so buckets get switched every 6 minutes.
                    .build()
                    .register();


    private static final Logger logger = LogManager.getLogger();

    @Nonnull
    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    @Nonnull
    private final Map<Long, ReservedInstanceBought> reservedInstancesBoughtById;

    @Nonnull
    private final Map<Long, ReservedInstanceSpec> reservedInstanceSpecById;

    public ReservedInstanceCoverageValidator(
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore,
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology) {

        this.cloudTopology = cloudTopology;
        this.reservedInstancesBoughtById =
                reservedInstanceBoughtStore
                    .getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter.SELECT_ALL_FILTER)
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(
                            ReservedInstanceBought::getId, Function.identity()));

        // Query only for RI specs referenced from reservedInstancesBoughtById
        final Set<Long> riSpecIds = reservedInstancesBoughtById.values()
                .stream()
                .filter(ReservedInstanceBought::hasReservedInstanceBoughtInfo)
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .filter(ReservedInstanceBoughtInfo::hasReservedInstanceSpec)
                .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                .collect(ImmutableSet.toImmutableSet());
        this.reservedInstanceSpecById =
                reservedInstanceSpecStore.getReservedInstanceSpecByIds(riSpecIds)
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(
                            ReservedInstanceSpec::getId, Function.identity()));

    }

    /**
     * Validates RI coverage upload entries against the current cloud topology and stored
     * {@link ReservedInstanceBought} instances. If the entity does not exist within the cloud
     * topology, it will be be dropped. Individual {@link Coverage} entries within an
     * {@link EntityRICoverageUpload} instance will be dropped if the RI cannot be found in the
     * {@link ReservedInstanceBoughtStore} or the coverage does not pass
     * {@link #isCoverageValid(TopologyEntityDTO, ReservedInstanceBought)}. If the sum total of
     * coupons allocated through {@link Coverage} instances for an entity are greater than the
     * total coverage of the entity (determined through {@link Coverage#getTotalCouponsRequired()}),
     * all coverages will be dropped for that entity.
     *
     *
     * @param entityRICoverageEntries the list of coverages to validate
     * @return An immutable list of {@link EntityRICoverageUpload} in which the linked entity/RI exist and
     * coverage instances are valid. If an entity exists within the cloud topology and a
     * {@link EntityRICoverageUpload} references the entity, this method is guaranteed to return
     * an {@link EntityRICoverageUpload} instance for it.
     */
    public List<EntityRICoverageUpload> validateCoverageUploads(
            @Nonnull Collection<EntityRICoverageUpload> entityRICoverageEntries) {

        logger.info("Running RI coverage validation on {} coverage uploads", entityRICoverageEntries.size());

        try (DataMetricTimer timer = VALIDATION_DURATION_METRIC_SUMMARY.startTimer()) {
            return entityRICoverageEntries.stream()
                    .map(this::validateCoverageUpload)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(ImmutableList.toImmutableList());
        }
    }

    /**
     * Determines whether <code>entityDTO</code> can be covered by <code>reservedInstance</code>.
     * Verifies the following criteria:
     * <ul>
     *     <li>The RI is not expired</li>
     *     <li>The entity is within an account within scope of the RI</li>
     *     <li>The entity tier is within scope of the RI. Either the tier matches. if the RI is not
     *     instance size flexible (ISF), or the family matches (if ISF)</li>
     *     <li>The RI is platform-flexible or the entity's OS matches the RI's OS</li>
     *     <li>The entity is within the same AZ (if available from the RI) and region as the RI</li>
     *     <li>The tenancy of the entity and RI match</li>
     * </ul>
     * @param entityDTO An instance of {@link TopologyEntityDTO}. Currently, any type other than
     *                  {@link EntityType#VIRTUAL_MACHINE} will always return false
     * @param reservedInstance An instance of {@link ReservedInstanceBought}
     * @return True if <code>entityDTO</code> can be covered by <code>reservedInstance</code>.
     * False otherwise
     */
    public boolean isCoverageValid(@Nonnull final TopologyEntityDTO entityDTO,
                                   @Nonnull final ReservedInstanceBought reservedInstance) {

        final boolean isReservedInstanceSpecAvailable =
                extractRISpecInfo(reservedInstance).isPresent();

        final boolean isReservedInstanceExpired = isReservedInstanceExpired(reservedInstance);
        final boolean isAccountScopeValid = isAccountScopeValidForCoverage(
                entityDTO, reservedInstance);
        final boolean isTierValid = isTierValidForCoverage(entityDTO, reservedInstance);
        final boolean isOSValid = isOSValidForCoverage(entityDTO, reservedInstance);
        final boolean isLocationValid = isLocationValidForCoverage(entityDTO, reservedInstance);
        final boolean isTenancyValid = isTenancyValidForCoverage(entityDTO, reservedInstance);

        final boolean isCoverageValid = !isReservedInstanceExpired && isAccountScopeValid &&
                isTierValid && isOSValid && isLocationValid && isTenancyValid;

        if (!isCoverageValid) {
            logger.warn("Coverage validated (EntityOid={}, RIOid={}, RISpecAvailable={}, RIExpired={}, " +
                            "AccountScopeValid={}, TierValid={}, OSValid={}, LocationValid={}, TenancyValid={})",
                    entityDTO.getOid(), reservedInstance.getId(), isReservedInstanceSpecAvailable,
                    isReservedInstanceExpired, isAccountScopeValid, isTierValid, isOSValid,
                    isLocationValid, isTenancyValid);
        }

        return isCoverageValid;
    }

    /**
     * Validates an individual instance of {@link EntityRICoverageUpload}, per the behavior described
     * in {@link #validateCoverageUploads(Collection)}.
     *
     * @param entityRICoverage An instance of {@link EntityRICoverageUpload} to validate
     * @return A new {@link EntityRICoverageUpload} instance, if the referenced entity can be found
     * in the cloud topology. An empty {@link Optional}, if the entity cannot be found.
     */
    private Optional<EntityRICoverageUpload> validateCoverageUpload(
            @Nonnull EntityRICoverageUpload entityRICoverage) {

        return cloudTopology.getEntity(entityRICoverage.getEntityId())
                .map(entity -> {
                    final EntityRICoverageUpload.Builder entityRiCoverageBuilder =
                            EntityRICoverageUpload.newBuilder(entityRICoverage)
                                    .clearCoverage();

                    if (entity.getEntityState() == EntityState.POWERED_ON) {
                        final List<Coverage> validCoverages = entityRICoverage.getCoverageList()
                                .stream()
                                .filter(coverage -> {
                                    final ReservedInstanceBought reservedInstance =
                                            reservedInstancesBoughtById.get(coverage.getReservedInstanceId());

                                    if (reservedInstance != null) {
                                        final boolean isCoverageValid = isCoverageValid(
                                                entity, reservedInstance);

                                        if (!isCoverageValid) {
                                            logger.warn("Dropping invalid coverage (Coverage={})",
                                                    coverage.toString());
                                        }

                                        return isCoverageValid;
                                    } else {

                                        logger.warn("Unable to find ReservedInstanceBought for entity Coverage" +
                                                        "(EntityOid={}, ReservedInstanceOid={})",
                                                entity.getOid(), coverage.getReservedInstanceId());
                                        return false;

                                    }
                                }).collect(Collectors.toList());

                        final double couponCapacity = entityRICoverage.getTotalCouponsRequired();
                        final double totalCoveredCoupons = validCoverages.stream()
                                .mapToDouble(Coverage::getCoveredCoupons)
                                .sum();

                        if (totalCoveredCoupons - couponCapacity <= ReservedInstanceUtil.COUPON_EPSILON) {
                            entityRiCoverageBuilder.addAllCoverage(validCoverages);
                        } else {
                            logger.warn("RI coverage exceeds capacity. Dropping coverage" +
                                            "(EntityOid={}, TotalCoverage={}, Capacity={})",
                                    entity.getOid(), totalCoveredCoupons, couponCapacity);
                        }

                    } else {
                        entityRiCoverageBuilder.setTotalCouponsRequired(0.0);
                    }

                    return Optional.of(entityRiCoverageBuilder.build());
                }).orElseGet(() -> {
                    logger.warn("Unable to find entity for RI coverage (EntityRICoverage={})",
                            entityRICoverage);
                    return Optional.empty();
                });
    }


    /**
     * Checks whether the RI is valid based on it's start time and term
     *
     * @param reservedInstance An instance of {@link ReservedInstanceBought}
     * @return True if the reserved instance is expired or the {@link ReservedInstanceSpec} could
     * not be found
     */
    private boolean isReservedInstanceExpired(@Nonnull ReservedInstanceBought reservedInstance) {

        return extractRISpecInfo(reservedInstance).map(riSpecInfo -> {

            final ReservedInstanceBoughtInfo riInfo = reservedInstance.getReservedInstanceBoughtInfo();
            final Instant reservedInstanceExpirationInstant;
            if (riInfo.hasEndTime()) {
                reservedInstanceExpirationInstant = Instant.ofEpochMilli(riInfo.getEndTime());
            } else {
                reservedInstanceExpirationInstant = Instant
                        .ofEpochMilli(riInfo.getStartTime())
                        // AWS definition of a year: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-reserved-instances.html
                        // Azure documentation is lacking. Therefore, we default to AWS definition
                        .plus(riSpecInfo.getType().getTermYears() * 365L, ChronoUnit.DAYS);
            }
            return Instant.now().isAfter(reservedInstanceExpirationInstant);
        }).orElse(false);
    }

    /**
     * Checks whether the <code>entityDTO</code> is within an account that's within scope of
     * the <code>reservedInstance</code>. If the RI is shared, the master/billing account will
     * compared for equality between the RI and entity. The master billing account is determined
     * by the root ownership in the connectedEntities list of each BusinessAccount. If the RI is
     * not shared, a check is performed ot see if the RI's applicable business account list contains
     * the entity's BA.
     *
     * @param entityDTO An instance of {@link TopologyEntityDTO}
     * @param reservedInstance An instance of {@link ReservedInstanceBought}
     * @return True, if the <code>entityDTO</code>'s account is within scope of the RI. False, if
     * the account is out of scope or the RI's {@link ReservedInstanceSpec} could not be found.
     */
    private boolean isAccountScopeValidForCoverage(
            @Nonnull final TopologyEntityDTO entityDTO,
            @Nonnull final ReservedInstanceBought reservedInstance) {

        final ReservedInstanceBoughtInfo riInfo = reservedInstance.getReservedInstanceBoughtInfo();
        final boolean isReservedInstanceShared = riInfo.getReservedInstanceScopeInfo().getShared();
        final long reservedInstancePurchasingAccount = riInfo.getBusinessAccountId();

        if (isReservedInstanceShared) {
            final Optional<GroupAndMembers> entityBillingAccountIdForVM = cloudTopology
                    .getBillingFamilyForEntity(entityDTO.getOid());
            final Optional<GroupAndMembers> entityBillingAccountIdForRi = cloudTopology
                    .getBillingFamilyForEntity(reservedInstancePurchasingAccount);
            if (entityBillingAccountIdForRi.isPresent() && entityBillingAccountIdForVM.isPresent()) {
                    Grouping entityBillingAccountForRIGroup = entityBillingAccountIdForRi.get().group();
                    Grouping entityBillingAccountForVMGroup = entityBillingAccountIdForVM.get().group();
                    if (entityBillingAccountForRIGroup != null && entityBillingAccountForVMGroup != null) {
                        long entityBillingAccountIdVM = entityBillingAccountForVMGroup.getId();
                        long entityBillingAccountIdRI = entityBillingAccountForRIGroup.getId();
                        if (entityBillingAccountIdRI == entityBillingAccountIdVM) {
                            return true;
                        } else {
                            logger.warn("The billing account id for the entity {} and the RI {} is not the same",
                                    entityDTO.getOid(), reservedInstance.getId());
                            return false;
                        }
                } else {
                    logger.warn("The billing account grouping is not present for id: {}",
                            entityBillingAccountForRIGroup == null ? reservedInstance.getId() : entityDTO.getOid());
                    return false;
                }
            } else {
                logger.warn("Billing family not found for entity {}", entityBillingAccountIdForRi.isPresent()
                        ? entityDTO.getOid() : reservedInstance.getId());
                return false;
            }
        } else {
            return cloudTopology.getOwner(entityDTO.getOid())
                    .map(entityAccount -> {
                        {
                            final ReservedInstanceScopeInfo riScopeInfo =
                                    riInfo.getReservedInstanceScopeInfo();
                            return riScopeInfo.getApplicableBusinessAccountIdList()
                                    .contains(entityAccount.getOid());
                        }
                    }).orElse(false);
        }
    }

    /**
     * Checks whether the compute tier matches between <code>entityDTO</code> and
     * <code>reservedInstance</code>. If the RI is instance-size flexible, only a match
     * of the family of the compute tier of both the RI and entity is required. If the RI
     * is not ISF, a match of the specific tier is required.
     * <p>
     * Note: Currently, the tier type is determined through ComputeTier::getDisplayName
     *
     * @param entityDTO An instance of {@link TopologyEntityDTO}
     * @param reservedInstance An instance of {@link ReservedInstanceBought}
     * @return True if the entity's tier is within scope of the RI. False if the the entity's
     * tier is out of scope of the RI or if the entity or RI's compute tier cannot be determined
     * by the cloud topology
     */
    private boolean isTierValidForCoverage(
            @Nonnull final TopologyEntityDTO entityDTO,
            @Nonnull final ReservedInstanceBought reservedInstance) {

        return extractRISpecInfo(reservedInstance).map(riSpecInfo ->
                cloudTopology.getComputeTier(entityDTO.getOid())
                        .map(entityComputeTier ->
                                cloudTopology.getEntity(riSpecInfo.getTierId())
                                        .filter(riComputeTier ->
                                                riComputeTier.getEntityType() ==
                                                        EntityType.COMPUTE_TIER_VALUE)
                                        .map(riComputeTier -> {

                                            final ComputeTierInfo entityComputeTierInfo =
                                            entityComputeTier.getTypeSpecificInfo().getComputeTier();
                                            final ComputeTierInfo riComputeTierInfo =
                                            riComputeTier.getTypeSpecificInfo().getComputeTier();
                                            final boolean isFamilyMatch =
                                            riComputeTierInfo.hasFamily() &&
                                                    entityComputeTierInfo.hasFamily() &&
                                                    riComputeTierInfo.getFamily().equals(
                                                            entityComputeTierInfo.getFamily());

                                            return isFamilyMatch &&
                                                    (riSpecInfo.getSizeFlexible() ||
                                                            riComputeTier.getOid() == entityComputeTier.getOid());

                                        }).orElse(false))
                        .orElse(false))
                .orElse(false);
    }

    /**
     * Checks whether the entity's OS is within scope of the RI. If the RI is platform
     * flexible, this method will always return true.
     *
     * @param entityDTO An instance of {@link TopologyEntityDTO}
     * @param reservedInstance An instance of {@link ReservedInstanceBought}
     * @return True if the RI is platform flexible or the RI and entity's OSes match. False if
     * the RI is not platform flexible and the entity OS does not match the RI OS, or if the RI's
     * spec cannot be found
     */
    private boolean isOSValidForCoverage(
            @Nonnull final TopologyEntityDTO entityDTO,
            @Nonnull final ReservedInstanceBought reservedInstance) {

        return extractRISpecInfo(reservedInstance).map(riSpecInfo -> {

            if (riSpecInfo.getPlatformFlexible()) {
                return true;
            } else {
                if (entityDTO.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                    final OS entityOSInfo = entityDTO.getTypeSpecificInfo().getVirtualMachine().getGuestOsInfo();
                    final OSType riOSType = riSpecInfo.getOs();

                    return entityOSInfo.getGuestOsType() == riOSType;
                } else {
                    return false;
                }
            }

        }).orElse(false);
    }

    /**
     * Checks whether the entity's region/zone matches the RI's region/zone. If the RI does not
     * have an availability zone (indicating a regional RI), only a match of the region is required.
     *
     * @param entityDTO An instance of {@link TopologyEntityDTO}
     * @param reservedInstance An instance of {@link ReservedInstanceBought}
     * @return True if the RI is regional and the region of the entity and RI match or if the
     * RI is zonal and the zone matches. False if the RI is regional and the region does not
     * match or if the RI is zonal and the zone does not match
     */
    private boolean isLocationValidForCoverage(
            @Nonnull final TopologyEntityDTO entityDTO,
            @Nonnull final ReservedInstanceBought reservedInstance) {

        final ReservedInstanceBoughtInfo riInfo = reservedInstance.getReservedInstanceBoughtInfo();
        if (riInfo.hasAvailabilityZoneId()) {
            final long riAvailabilityZoneId = riInfo.getAvailabilityZoneId();

            return cloudTopology.getConnectedAvailabilityZone(entityDTO.getOid())
                    .map(entityAvailabilityZone ->
                            entityAvailabilityZone.getOid() == riAvailabilityZoneId)
                    .orElse(false);
        } else {
            return extractRISpecInfo(reservedInstance)
                    .map(riSpecInfo ->
                            cloudTopology.getConnectedRegion(entityDTO.getOid())
                                    .map(entityRegion ->
                                            riSpecInfo.getRegionId() == entityRegion.getOid())
                                    .orElse(false))
                    .orElse(false);
        }

    }

    /**
     * Checks whether the {@link com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy} matches
     * between the RI and entity
     *
     * @param entityDTO An instance of {@link TopologyEntityDTO}
     * @param reservedInstance An instance of {@link ReservedInstanceBought}
     * @return True if the tenancy matches. False if they do not match or the RI's spec cannot
     * be found.
     */
    private boolean isTenancyValidForCoverage(
            @Nonnull final TopologyEntityDTO entityDTO,
            @Nonnull final ReservedInstanceBought reservedInstance) {

        return extractRISpecInfo(reservedInstance)
                .map(riSpecInfo -> {
                    if (entityDTO.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                        return riSpecInfo.getTenancy() == entityDTO.getTypeSpecificInfo()
                                .getVirtualMachine().getTenancy();
                    } else {
                        return false;
                    }
                }).orElse(false);
    }

    /**
     * A helper method to extract the RI's {@link ReservedInstanceSpecInfo} based on the
     * {@link ReservedInstanceSpec} Id of the RI.
     *
     * @param reservedInstance An instance of {@link ReservedInstanceBought}
     * @return An {@link Optional} of {@link ReservedInstanceSpecInfo}, if the RI specInfo
     * is contained within the {@link ReservedInstanceSpecStore}. An empty {@link Optional},
     * if the RI specInfo cannot be found
     */
    private Optional<ReservedInstanceSpecInfo> extractRISpecInfo(
            @Nonnull ReservedInstanceBought reservedInstance) {

        final ReservedInstanceBoughtInfo riInfo = reservedInstance.getReservedInstanceBoughtInfo();
        final ReservedInstanceSpec riSpec = reservedInstanceSpecById.get(riInfo.getReservedInstanceSpec());

        return (riSpec != null) ?
                Optional.of(riSpec.getReservedInstanceSpecInfo()) :
                Optional.empty();
    }

} // EntityRICoverageValidator
