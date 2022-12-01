package com.vmturbo.cost.component;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import com.google.common.collect.Lists;

import io.prometheus.client.CollectorRegistry;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.BilledCostServicesREST.BilledCostServiceController;
import com.vmturbo.common.protobuf.cost.CloudCostServicesREST.CloudCostDiagsServiceController;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;
import com.vmturbo.cost.component.billed.cost.CloudCostConfig;
import com.vmturbo.cost.component.billed.cost.CloudCostDiagsRpcService;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisStoreConfig;
import com.vmturbo.cost.component.cloud.commitment.CloudCommitmentStatsConfig;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.diags.CostDiagnosticsHandler;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;
import com.vmturbo.cost.component.reserved.instance.PlanReservedInstanceRpcService;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.stores.DiagnosableDataStoreCollector;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Class for handling cost diagnostics export and import.
 */
@Import({CloudCommitmentAnalysisStoreConfig.class,
        EntityCostConfig.class,
        CloudCostConfig.class,
        BilledCostConfig.class,
        ReservedInstanceConfig.class,
        ComputeTierDemandStatsConfig.class,
        DbAccessConfig.class,
        ReservedInstanceSpecConfig.class,
        CloudCommitmentStatsConfig.class})
@Configuration
public class CostDiagnosticsConfig {

    @Autowired
    public CloudCommitmentAnalysisStoreConfig cloudCommitmentAnalysisStoreConfig;

    @Autowired
    private EntityCostConfig entityCostConfig;

    @Autowired
    private CloudCostConfig cloudCostConfig;

    @Autowired
    private BilledCostConfig billedCostConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Autowired
    private DiagnosableDataStoreCollector sourceTopologyCommitmentCoverageStoreDiagnosable;

    @Autowired
    private DiagnosableDataStoreCollector projectedTopologyCommitmentCoverageStoreDiagnosable;

    @Autowired
    private DiagnosableDataStoreCollector sourceTopologyCommitmentUtilizationStoreDiagnosable;

    @Autowired
    private DiagnosableDataStoreCollector projectedTopologyCommitmentUtilizationStoreDiagnosable;

    @Autowired
    private DiagnosableDataStoreCollector sourceTopologyCommitmentMappingStoreDiagnosable;

    @Autowired
    private DiagnosableDataStoreCollector projectedTopologyCommitmentMappingStoreDiagnosable;

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Value("${saveAllocationDemandStores: true}")
    private boolean saveAllocationDemandDiags;

    @Value("${saveCostDiags: true}")
    private boolean saveCostDiags;

    @Value("${saveHistoricalStatsDiags: false}")
    private boolean saveHistoricalStatsDiags;

    @Bean
    public DiagsZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    @Bean
    public DiagnosticsHandlerImportable diagsHandler() {
        try {
            // Pass an unpooled DSLContext, so that diags restoring does not generate leak warnings
            return new CostDiagnosticsHandler(recursiveZipReaderFactory(), getStoresToDump(),
                        dbAccessConfig.unpooledDsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create DiagnosticsHandlerImportable bean",
                    e);
        }
    }

    @Bean
    public DiagnosticsControllerImportable diagnosticsController() {
        return new DiagnosticsControllerImportable(diagsHandler());
    }

    /**
     * Prometheus diagnostics provider.
     *
     * @return prometheus diagnostics provider
     */
    @Bean
    public PrometheusDiagnosticsProvider prometheusDiagnisticsProvider() {
        return new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry);
    }

    /**
     * Cloud cost diags Rpc service bean.
     *
     * @return The {@link CloudCostDiagsRpcService}
     */
    @Bean
    public CloudCostDiagsRpcService cloudCostDiagsRpcService() {
        return new CloudCostDiagsRpcService(cloudCostConfig.cloudCostStore(),
                billedCostConfig.tagStore(),
                billedCostConfig.tagGroupStore(),
                cloudCostConfig.cloudScopeIdentityStore());
    }

    /**
     * Creates a new {@link CloudCostDiagsServiceController} instance. The controller is required
     * for swagger functionality.
     * @return The newly created {@link CloudCostDiagsServiceController} instance.
     */
    @Bean
    public CloudCostDiagsServiceController cloudCostDiagsServiceController() {
        return new CloudCostDiagsServiceController(cloudCostDiagsRpcService());
    }

    /**
     * Gets the list of all stores we want to dump diags for.
     *
     * @return A collection of stores.
     */
    private Collection<Diagnosable> getStoresToDump() {
        Collection<Diagnosable> storesToSave = new ArrayList<>();
        if (saveCostDiags) {
            // Add the Prometheus dump
            storesToSave.add(prometheusDiagnisticsProvider());
            // Add realtime and plan related RI bought and entity to RI mapping stores
            storesToSave.addAll(Lists.newArrayList(reservedInstanceConfig.buyReservedInstanceStore(),
                    reservedInstanceConfig.entityReservedInstanceMappingStore(),
                    reservedInstanceConfig.reservedInstanceBoughtStore(), reservedInstanceConfig.planReservedInstanceStore(),
                    reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                    reservedInstanceConfig.accountRIMappingStore()));

            // Query the entity cost store. If saveHistoricalStatsDiags is true, we also dump rolled up tables.
            storesToSave.addAll(entityCostConfig.entityCostStore().getDiagnosables(saveHistoricalStatsDiags));

            // Query the cloud cost store.
            storesToSave.addAll(cloudCostConfig.cloudCostStore().getDiagnosables(true));
            storesToSave.addAll(billedCostConfig.tagStore().getDiagnosables(true));
            storesToSave.addAll(billedCostConfig.tagGroupStore().getDiagnosables(true));
            storesToSave.addAll(cloudCostConfig.cloudScopeIdentityStore().getDiagnosables(true));

            // Query the reserved instance coverage store. If saveHistoricalStatsDiags is true, we also dump rolled up tables.
            storesToSave.addAll(reservedInstanceConfig.reservedInstanceCoverageStore().getDiagnosables(saveHistoricalStatsDiags));

            // Query the reserved instance utilization store. If saveHistoricalStatsDiags is true, we also dump rolled up tables.
            storesToSave.addAll(reservedInstanceConfig.reservedInstanceUtilizationStore().getDiagnosables(saveHistoricalStatsDiags));

            // Query the Plan Reserved Instance Coverage and Utilization store. This is responsible for dumping
            // Plan Projected RI coverage, Plan Projected RI utilziation and Plan Projected entity to RI mapping.
            storesToSave.addAll(reservedInstanceConfig.planProjectedRICoverageAndUtilStore().getDiagnosables(saveHistoricalStatsDiags));

            // If true, add the allocation store for RI buy 2.0 and the demand recording store for RI buy 1.0.
            if (saveAllocationDemandDiags) {
                storesToSave.add(cloudCommitmentAnalysisStoreConfig.computeTierAllocationStore());
                storesToSave.add(cloudCommitmentAnalysisStoreConfig.cloudScopeStore());
                storesToSave.add(computeTierDemandStatsConfig.riDemandStatsStore());
            }

            storesToSave.addAll(Arrays.asList(sourceTopologyCommitmentCoverageStoreDiagnosable,
                    projectedTopologyCommitmentCoverageStoreDiagnosable,
                    sourceTopologyCommitmentUtilizationStoreDiagnosable,
                    projectedTopologyCommitmentUtilizationStoreDiagnosable,
                    sourceTopologyCommitmentMappingStoreDiagnosable,
                    projectedTopologyCommitmentMappingStoreDiagnosable));
        }
        return storesToSave;
    }
}