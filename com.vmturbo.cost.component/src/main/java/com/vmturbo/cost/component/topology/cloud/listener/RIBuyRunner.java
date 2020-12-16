package com.vmturbo.cost.component.topology.cloud.listener;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;
import com.vmturbo.cost.component.util.BusinessAccountHelper;

/**
 * A class for storing RI Buy 1.0 demand and for running RI Buy analysis.
 */
public class RIBuyRunner implements LiveCloudTopologyListener {

    private final ReservedInstanceAnalysisInvoker invoker;

    private final BusinessAccountHelper businessAccountHelper;

    private final ComputeTierDemandStatsWriter computeTierDemandStatsWriter;

    /**
     * Constructor for the RI Buy runner.
     *
     * @param reservedInstanceAnalysisInvoker The RI analysis invoker to invoke RI analysis.
     * @param businessAccountHelper The business account helper.
     * @param computeTierDemandStatsWriter The compute tier demand stats writer to write demand to
     * the db.
     */
    public RIBuyRunner(@Nonnull final ReservedInstanceAnalysisInvoker reservedInstanceAnalysisInvoker,
            @Nonnull final BusinessAccountHelper businessAccountHelper,
            @Nonnull ComputeTierDemandStatsWriter computeTierDemandStatsWriter) {
        this.invoker = reservedInstanceAnalysisInvoker;
        this.businessAccountHelper = businessAccountHelper;
        this.computeTierDemandStatsWriter = computeTierDemandStatsWriter;
    }

    @Override
    public void process(CloudTopology cloudTopology, TopologyInfo topologyInfo) {
        // Store allocation demand in db
        computeTierDemandStatsWriter.calculateAndStoreRIDemandStats(topologyInfo, cloudTopology, false);

        invoker.invokeBuyRIAnalysis(cloudTopology, businessAccountHelper.getAllBusinessAccounts());
    }
}
