package com.vmturbo.cost.component;

import java.util.ArrayList;
import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.cost.component.cca.CloudCommitmentEventDemandStatsConfig;

/**
 * Class for handling cost diagnostics export and import.
 */
@Import({CloudCommitmentEventDemandStatsConfig.class})
public class CostDiagnonsticsConfig {

    @Autowired
    public CloudCommitmentEventDemandStatsConfig cloudCommitmentEventDemandStatsConfig;

    @Value("${saveAllocationDemandStores: true}")
    private boolean saveHistoricalDiags;

    @Bean
    public DiagsZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    @Bean
    public DiagnosticsHandlerImportable diagsHandler() {
        return new DiagnosticsHandlerImportable(recursiveZipReaderFactory(),
                getAllocationDemandStores());
    }

    @Bean
    public DiagnosticsControllerImportable diagnosticsController() {
        return new DiagnosticsControllerImportable(diagsHandler());
    }

    /**
     * Based on the saveAllocationDemandStores flag, gets the list of allocation demand stores
     * we want to export diags for.
     *
     * @return A collection of stores.
     */
    private Collection<Diagnosable> getAllocationDemandStores() {
        Collection<Diagnosable> storesToSave = new ArrayList<>();
        if (saveHistoricalDiags) {
            storesToSave.add(cloudCommitmentEventDemandStatsConfig.sqlComputeTierAllocationStore());
            storesToSave.add(cloudCommitmentEventDemandStatsConfig.sqlCloudScopeStore());
        }
        return storesToSave;
    }
}
