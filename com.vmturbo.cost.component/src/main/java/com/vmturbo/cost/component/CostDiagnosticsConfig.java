package com.vmturbo.cost.component;

import io.prometheus.client.CollectorRegistry;
import java.util.ArrayList;
import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.google.common.collect.Lists;

import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisStoreConfig;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;

/**
 * Class for handling cost diagnostics export and import.
 */
@Import({CloudCommitmentAnalysisStoreConfig.class,
        EntityCostConfig.class,
        ReservedInstanceConfig.class,
        ComputeTierDemandStatsConfig.class,
        CostDBConfig.class,
        ReservedInstanceSpecConfig.class})
public class CostDiagnosticsConfig {

    @Autowired
    public CloudCommitmentAnalysisStoreConfig cloudCommitmentAnalysisStoreConfig;

    @Autowired
    private EntityCostConfig entityCostConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

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
        return new DiagnosticsHandlerImportable(recursiveZipReaderFactory(),
                getStoresToDump());
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
                    reservedInstanceSpecConfig.reservedInstanceSpecStore()));

            // Query the entity cost store. If saveHistoricalStatsDiags is true, we also dump rolled up tables.
            storesToSave.addAll(entityCostConfig.entityCostStore().getDiagnosables(saveHistoricalStatsDiags));

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
        }
        return storesToSave;
    }
}
