package com.vmturbo.repository.controller;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.vmturbo.repository.service.GraphTopologyService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api("GraphTopologyController")
@RequestMapping("/repository/topology")
public class GraphTopologyController {

    private final Logger logger = LoggerFactory.getLogger(GraphTopologyController.class);

    private final GraphTopologyService graphTopologyService;

    public GraphTopologyController(final GraphTopologyService graphTopologyService) {
        this.graphTopologyService = Objects.requireNonNull(graphTopologyService);
    }

    @RequestMapping(
            path="/{topologyID}",
            method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation("Delete a topology")
    public ResponseEntity<Void> deleteTopology(@PathVariable("topologyID") final String topologyID) {
        graphTopologyService.deleteTopology(topologyID);
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }
}
