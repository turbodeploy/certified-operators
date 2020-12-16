package com.vmturbo.cost.component.reserved.instance;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.CostDebug.GetBuyRIImpactCsvRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.OS;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualMachineInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.entity.cost.ProjectedEntityCostStore;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * A utility class for generating a report on buy RI impact. The report is meant to be used as a
 * debugging tool to verify buy RI discounts on the projected topology costs.
 */
public class BuyRIImpactReportGenerator {

    private static final String CSV_REPORT_GENERATION_ERROR_OUTPUT = "Error generating CSV report";

    private final Logger logger = LogManager.getLogger();

    private final RepositoryServiceBlockingStub repositoryService;

    private final BuyReservedInstanceStore buyReservedInstanceStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private final ProjectedRICoverageAndUtilStore projectedRICoverageAndUtilStore;

    private final ProjectedEntityCostStore projectedEntityCostStore;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final long realtimeTopologyContextId;

    public BuyRIImpactReportGenerator(@Nonnull RepositoryServiceBlockingStub repositoryService,
                             @Nonnull BuyReservedInstanceStore buyReservedInstanceStore,
                             @Nonnull ReservedInstanceSpecStore reservedInstanceSpecStore,
                             @Nonnull ProjectedRICoverageAndUtilStore projectedRICoverageAndUtilStore,
                             @Nonnull ProjectedEntityCostStore projectedEntityCostStore,
                             @Nonnull TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                             long realtimeTopologyContextId) {

        this.repositoryService = repositoryService;
        this.buyReservedInstanceStore = Objects.requireNonNull(buyReservedInstanceStore);
        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
        this.projectedRICoverageAndUtilStore = Objects.requireNonNull(projectedRICoverageAndUtilStore);
        this.projectedEntityCostStore = Objects.requireNonNull(projectedEntityCostStore);
        this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Generates a CSV-formatted report as a string. The report will include information on
     * the buy RI instance (e.g. instance type, region, count, account, etc) and the covered entities
     * (e.g. instance type, region, coverage percentage, discount from buy RI, etc). This information
     * can aid in debugging the validity of buy RI impact analysis.
     *
     * Note: while a target topology context ID is accepted as part of {@code request}, generation
     * for plan context IDs is currently not supported, as some of the required information is
     * not available.
     *
     * @param request The client request for the report.
     * @return The generated report in CSV format, as a string.
     * @throws UnsupportedOperationException Thrown if the topology context ID contained within the
     * request refers to a plan topology.
     */
    public String generateCsvReportAsString(GetBuyRIImpactCsvRequest request) {

        final StringBuilder csvStringBuilder = new StringBuilder();
        long topologyContextId = request.hasTopologyContextId() ?
                request.getTopologyContextId() :
                realtimeTopologyContextId;

        try {
            writeCsvReportForTopology(csvStringBuilder, topologyContextId);
        } catch (Exception e) {
            logger.error("Error generating Buy RI Impact CSV report", e);
            return CSV_REPORT_GENERATION_ERROR_OUTPUT;
        }

        return csvStringBuilder.toString();
    }

    private void writeCsvReportForTopology(Appendable reportOut,
                                           long topologyContextId) throws IOException {

        // First, load all required information for the report generator.
        final Map<Long, ReservedInstanceBought> buyRIsById =
                getBuyRIsForTopologyContext(topologyContextId);
        final Map<Long, ReservedInstanceSpec> riSpecsById =
                getRISpecsForRIs(buyRIsById.values());

        final Set<EntityReservedInstanceCoverage> entityRICoverageSet =
                getEntityRICoverageForTopologyContext(topologyContextId);
        final Set<Long> coveredEntities = entityRICoverageSet.stream()
                .map(EntityReservedInstanceCoverage::getEntityId)
                .collect(ImmutableSet.toImmutableSet());
        final Map<Long, EntityCost> entityCostsByEntityOid =
            getEntityCosts(topologyContextId, coveredEntities);

        final CloudTopology cloudTopology = createCloudTopology(topologyContextId, coveredEntities);

        // Create a report generator
        final CsvReportGenerator reportGenerator = new CsvReportGenerator(
                buyRIsById,
                riSpecsById,
                cloudTopology,
                entityRICoverageSet,
                entityCostsByEntityOid);

        reportGenerator.generateReport(reportOut);
    }

    private Map<Long, ReservedInstanceBought> getBuyRIsForTopologyContext(long topologyContextId) {
        final Collection<ReservedInstanceBought> buyRIs =
                buyReservedInstanceStore.getBuyReservedInstances(
                        BuyReservedInstanceFilter.newBuilder()
                                .addTopologyContextId(topologyContextId)
                                .build());

        return buyRIs.stream()
                .collect(ImmutableMap.toImmutableMap(
                        ReservedInstanceBought::getId,
                        Function.identity()));
    }

    private Map<Long, ReservedInstanceSpec> getRISpecsForRIs(
            @Nonnull Collection<ReservedInstanceBought> reservedInstances) {

        final Set<Long> riSpecIds = reservedInstances.stream()
                .map(ri -> ri.getReservedInstanceBoughtInfo().getReservedInstanceSpec())
                .collect(Collectors.toSet());
        return reservedInstanceSpecStore.getReservedInstanceSpecByIds(riSpecIds)
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(
                                ReservedInstanceSpec::getId,
                                Function.identity()));
    }

    private CloudTopology createCloudTopology(long topologyContextId,
                                              Set<Long> targetEntityOids) {
        // first load all regions, compute tiers, and accounts
        final RetrieveTopologyEntitiesRequest infraRetrievalRequest =
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyType(RepositoryDTO.TopologyType.PROJECTED)
                        .setTopologyContextId(topologyContextId)
                        .setReturnType(Type.FULL)
                        .addEntityType(EntityType.REGION_VALUE)
                        .addEntityType(EntityType.COMPUTE_TIER_VALUE)
                        .addEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                        .build();
        final Stream<TopologyEntityDTO> infrastructureEntities =
                RepositoryDTOUtil.topologyEntityStream(
                        repositoryService.retrieveTopologyEntities(infraRetrievalRequest))
                .map(PartialEntity::getFullEntity);

        // load covered entities
        RetrieveTopologyEntitiesRequest targetEntitieRetrievalRequest =
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setTopologyType(RepositoryDTO.TopologyType.PROJECTED)
                        .setTopologyContextId(topologyContextId)
                        .setReturnType(Type.FULL)
                        .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .addEntityType(EntityType.DATABASE_VALUE)
                        .addEntityType(EntityType.DATABASE_SERVER_VALUE)
                        .addAllEntityOids(targetEntityOids)
                        .build();
        final Stream<TopologyEntityDTO> targetEntities =
                RepositoryDTOUtil.topologyEntityStream(
                        repositoryService.retrieveTopologyEntities(targetEntitieRetrievalRequest))
                        .map(PartialEntity::getFullEntity);

        return cloudTopologyFactory.newCloudTopology(
                Stream.concat(infrastructureEntities, targetEntities));
    }

    private Set<EntityReservedInstanceCoverage> getEntityRICoverageForTopologyContext(long topologyContextId) {

        final Map<Long, EntityReservedInstanceCoverage> entityRICoverageMap;
        if (topologyContextId == realtimeTopologyContextId) {
            entityRICoverageMap = projectedRICoverageAndUtilStore.getAllProjectedEntitiesRICoverages();
        } else {
            // Currently, the PlanProjectedRICoverageAndUtilStore does not store the mapping of
            // Entity <-> ReservedInstance. Only entity coverage and RI utilization are stored
            // to plan tables
            throw new UnsupportedOperationException("Unable to query EntityReservedInstanceCoverages for plans");
        }

        return entityRICoverageMap.values()
                .stream()
                .filter(entityRICoverage -> entityRICoverage.getCouponsCoveredByBuyRiCount() > 0)
                .collect(ImmutableSet.toImmutableSet());
    }

    private Map<Long, EntityCost> getEntityCosts(long topologyContextId,
                                                 Set<Long> entityOids) {
        if (topologyContextId == realtimeTopologyContextId) {
            return projectedEntityCostStore.getProjectedEntityCosts(entityOids);
        } else {
            // PlanProjectedEntityCostStore does not currently supply a getter for entity costs
            throw new UnsupportedOperationException("Unable to query EntityCosts for plans");
        }
    }

    /**
     * Once all required data has been loaded from the proper sources (e.g. buy RI instances and
     * the cloud topology), this class is used to generate a CSV report, based on the entity -> RI
     * coverage input.
     */
    private static final class CsvReportGenerator {

        private static final List<String> CSV_REPORT_HEADERS = ImmutableList.of(
                "RI OID",
                "Instance Count",
                "RI Instance Type",
                "RI Region Name",
                "RI Region OID",
                "Purchasing Account Name",
                "Purchasing Account OID",
                "RI Operation System",
                "RI Tenancy",
                "VM Name",
                "VM Oid",
                "VM Instance Type (Projected)",
                "VM Operating System",
                "VM Account Name",
                "VM Account OID",
                "VM Buy RI Coverage",
                "VM Buy RI Discount");

        private final Logger logger = LogManager.getLogger();

        private final Map<Long, ReservedInstanceBought> buyReservedInstancesById;
        private final Map<Long, ReservedInstanceSpec> riSpecsById;
        private final CloudTopology<TopologyEntityDTO> cloudTopology;
        private final Set<EntityReservedInstanceCoverage> entityRICoverageSet;
        private final Map<Long, EntityCost> entityCostByEntityOid;


        private CsvReportGenerator(@Nonnull Map<Long, ReservedInstanceBought> buyReservedInstancesById,
                                   @Nonnull Map<Long, ReservedInstanceSpec> riSpecsById,
                                   @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                   @Nonnull Set<EntityReservedInstanceCoverage> entityRICoverageSet,
                                   @Nonnull Map<Long, EntityCost> entityCostByEntityOid) {
            this.buyReservedInstancesById = buyReservedInstancesById;
            this.riSpecsById = riSpecsById;
            this.cloudTopology = cloudTopology;
            this.entityRICoverageSet = entityRICoverageSet;
            this.entityCostByEntityOid = entityCostByEntityOid;

        }

        private void generateReport(@Nonnull Appendable reportOut) throws IOException {

            try (CSVPrinter csvPrinter = new CSVPrinter(reportOut, CSVFormat.DEFAULT)) {
                csvPrinter.printRecord(CSV_REPORT_HEADERS);

                entityRICoverageSet.forEach(entityRICoverage ->
                        entityRICoverage.getCouponsCoveredByBuyRiMap().forEach((riOid, coverageAmount) -> {
                                    try {
                                        csvPrinter.printRecord(
                                                createEntryForCoverage(entityRICoverage, riOid, coverageAmount));
                                        } catch (IOException e) {
                                            logger.error("Error creating csv record (Entity )ID={}, RI ID={})",
                                                    entityRICoverage.getEntityId(), riOid, e);
                                        }
                        }));
            }
        }


        /**
         * Creates a single row representing RI coverage for a specific (entity, RI) pair.
         *
         * @param entityRICoverage The target {@link EntityReservedInstanceCoverage}, representing
         *                         the entity and its coupon capacity
         * @param riOid The target RI
         * @param coverageAmount The coverage amount from the target RI, applied to the target entity
         * @return A list of entries for the CSV, mirroring the order of {@link #CSV_REPORT_HEADERS}.
         */
        private List<?> createEntryForCoverage(@Nonnull EntityReservedInstanceCoverage entityRICoverage,
                                                    long riOid,
                                                    double coverageAmount) {

            // A helper method for converting a topology entity to its display name
            final Function<Optional<TopologyEntityDTO>, String> entityToDisplayName = (optTopologyEntity) ->
                    optTopologyEntity.map(TopologyEntityDTO::getDisplayName).orElse("UNKNOWN");

            /*
            RI information
             */
            final long entityOid = entityRICoverage.getEntityId();
            final Optional<ReservedInstanceBoughtInfo> riInfo =
                    Optional.ofNullable(buyReservedInstancesById.get(riOid))
                            .map(ReservedInstanceBought::getReservedInstanceBoughtInfo);
            final Optional<ReservedInstanceSpecInfo> riSpecInfo = riInfo
                    .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                    .map(riSpecsById::get)
                    .map(ReservedInstanceSpec::getReservedInstanceSpecInfo);
            final Optional<TopologyEntityDTO> riRegion = riSpecInfo
                    .map(ReservedInstanceSpecInfo::getRegionId)
                    .flatMap(cloudTopology::getEntity);

            final Optional<TopologyEntityDTO> riComputeTier = riSpecInfo
                    .map(ReservedInstanceSpecInfo::getTierId)
                    .flatMap(cloudTopology::getEntity);
            final Optional<TopologyEntityDTO> riAccount = riInfo
                    .map(ReservedInstanceBoughtInfo::getBusinessAccountId)
                    .flatMap(cloudTopology::getEntity);

            /*
            Entity information
             */
            final Optional<TopologyEntityDTO> coveredEntity = cloudTopology.getEntity(entityOid);
            final Optional<TopologyEntityDTO> entityComputeTier =
                    cloudTopology.getPrimaryTier(entityOid);
            final Optional<TopologyEntityDTO> entityAccount = cloudTopology.getOwner(entityOid);
            final double coveragePercentage =
                    coverageAmount * 100 / entityRICoverage.getEntityCouponCapacity();
            final Optional<Double> coverageDiscount =
                    Optional.ofNullable(entityCostByEntityOid.get(entityOid))
                            .map(entityCost ->
                                    entityCost.getComponentCostList().stream()
                                        .filter(componentCost ->
                                                componentCost.getCostSource() == CostSource.BUY_RI_DISCOUNT)
                                        .map(ComponentCost::getAmount)
                                        .mapToDouble(CurrencyAmount::getAmount)
                                            .sum());

            // The returned list must follow the format of CSV_REPORT_HEADERS
            return ImmutableList.builder()
                    // RI oid
                    .add(riOid)
                    // instance count
                    .add(riInfo.map(ReservedInstanceBoughtInfo::getNumBought).orElse(-1))
                    // RI instance type
                    .add(entityToDisplayName.apply(riComputeTier))
                    // RI region name
                    .add(entityToDisplayName.apply(riRegion))
                    // RI region oid
                    .add(riSpecInfo.map(ReservedInstanceSpecInfo::getRegionId).orElse(-1L))
                    // Purchasing account name
                    .add(entityToDisplayName.apply(riAccount))
                    // Purchasing account oid
                    .add(riInfo.map(ReservedInstanceBoughtInfo::getBusinessAccountId).orElse(-1L))
                    // RI OS
                    .add(riSpecInfo.map(ReservedInstanceSpecInfo::getOs)
                            .map(OSType::toString).orElse("UNKNOWN"))
                    // RI tenancy
                    .add(riSpecInfo.map(ReservedInstanceSpecInfo::getTenancy)
                            .map(Tenancy::toString).orElse("UNKNOWN"))
                    // VM Name
                    .add(entityToDisplayName.apply(coveredEntity))
                    // VM oid
                    .add(entityOid)
                    // VM instance type
                    .add(entityToDisplayName.apply(entityComputeTier))
                    // VM OS
                    .add(coveredEntity.map(TopologyEntityDTO::getTypeSpecificInfo)
                            .map(TypeSpecificInfo::getVirtualMachine)
                            .map(VirtualMachineInfo::getGuestOsInfo)
                            .map(OS::getGuestOsName)
                            .orElse("UNKNOWN"))
                    // VM Account Name
                    .add(entityToDisplayName.apply(entityAccount))
                    // VM Account Oid
                    .add(entityAccount.map(TopologyEntityDTO::getOid).orElse(-1L))
                    // VM coverage percentage
                    .add(coveragePercentage)
                    // VM discount
                    .add(coverageDiscount.orElse(-1.0))
                    .build();

        }
    }
}
