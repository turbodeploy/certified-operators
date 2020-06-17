package com.vmturbo.extractor.grafana;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * A REST controller for debugging/development. Allows forcing a "refresh" of Grafana data
 * without restarting the extractor component.
 */
@Api(value = "/grafanon")
@RequestMapping(value = "/grafanon")
@RestController
public class GrafanonController {

    private final Grafanon grafanon;

    GrafanonController(Grafanon grafanon) {
        this.grafanon = grafanon;
    }

    /**
     * Refresh grafanon.
     *
     * @param withDetails If true, return the full details of the performed operations (for debugging).
     * @return Summary of operations performed during the refresh.
     */
    @RequestMapping(value = "/refresh",
            method = RequestMethod.POST)
    @ApiOperation(value = "Refresh grafanon, reinject dashboards, all that good stuff.")
    public String refreshComponentContext(@RequestParam(name = "details", defaultValue = "false") final boolean withDetails) {
        RefreshSummary refreshSummary = new RefreshSummary();
        grafanon.refreshGrafana(refreshSummary);
        return refreshSummary.summarize(withDetails);
    }
}
