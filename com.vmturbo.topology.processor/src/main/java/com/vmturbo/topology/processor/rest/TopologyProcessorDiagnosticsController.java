package com.vmturbo.topology.processor.rest;

import javax.annotation.Nonnull;

import io.swagger.annotations.ApiOperation;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.IDiagnosticsHandlerImportable;
import com.vmturbo.topology.processor.history.HistoryCalculationException;
import com.vmturbo.topology.processor.history.percentile.PercentileEditor;

/**
 * The rest endpoints for topology processor internal state.
 */
public class TopologyProcessorDiagnosticsController extends DiagnosticsControllerImportable {
    private final PercentileEditor percentileEditor;

    /**
     * Construct the controller instance.
     *
     * @param importableHandler diagnostics handler to operate with
     * @param percentileEditor historical stage percentile editor
     */
    public TopologyProcessorDiagnosticsController(IDiagnosticsHandlerImportable importableHandler,
                    @Nonnull final PercentileEditor percentileEditor) {
        super(importableHandler);
        this.percentileEditor = percentileEditor;
    }

    /**
     * Re-assemble the percentile full blob from the daily blobs.
     *
     * @throws HistoryCalculationException when page reassembly failed
     * @throws InterruptedException when interrupted
     */
    @RequestMapping(value = "/reassemblePercentileFullPage",
                    method = RequestMethod.POST)
    @ApiOperation(value = "Re-assemble the percentile full blob from the daily blobs.",
        notes = "Triggers synchronous recalculation of percentile 'full' window memory cache "
                        + "by summarizing entries from the persisted daily blobs.")
    public void reassemblePercentileFullBlob()
            throws HistoryCalculationException, InterruptedException {
        percentileEditor.reassembleFullPage();
    }

}
