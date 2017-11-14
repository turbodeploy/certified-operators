package com.vmturbo.repository.controller;

import static javaslang.API.$;
import static javaslang.API.Case;
import static javaslang.API.Match;
import static javaslang.Patterns.Left;
import static javaslang.Patterns.Right;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import javaslang.control.Either;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.repository.service.GraphDBService;
import com.vmturbo.repository.topology.TopologyID.TopologyType;

/**
 * Implements Service Entity endpoint.
 */
@RequestMapping("/repository")
public class GraphServiceEntityController {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphServiceEntityController.class);

    private final GraphDBService graphDBService;

    public GraphServiceEntityController(final GraphDBService graphDBService) {
        this.graphDBService = Objects.requireNonNull(graphDBService);
    }

    @RequestMapping(
            path = "/serviceentity/query/displayname",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    @ApiOperation(value = "Query the service entities in the repository by display name.")
    @ResponseBody
    public ResponseEntity<Collection<ServiceEntityApiDTO>> searchServiceEntities(
            @ApiParam(value = "The display name to search for.")
            @RequestParam(name = "q") final String displayName) {

        LOGGER.info("Searching service entity by name with query '{}'", displayName);

        final Either<String, Collection<ServiceEntityApiDTO>> result =
                graphDBService.searchServiceEntityByName(displayName);

        return Match(result).of(
            Case(Right($()), entities -> ResponseEntity.ok(entities)),
            Case(Left($()), err -> ResponseEntity.ok(Collections.emptyList())));
    }

    @RequestMapping(
            path = "/serviceentity/query/id",
            method = RequestMethod.POST,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    @ApiOperation(value = "Query the service entities in the repository by OID.")
    @ResponseBody
    public ResponseEntity<Collection<ServiceEntityApiDTO>> searchServiceEntities(
            @RequestParam(required = false, value = "contextId")
                final Long contextId,
            @RequestParam(required = false, value = "projected", defaultValue = "false")
                final boolean searchProjected,
            @ApiParam(value = "The list of IDs of service entities to retrieve.")
            @RequestBody final ImmutableSet<Long> requestedIds) {

        LOGGER.debug("Searching for service entities {}", requestedIds);
        final Either<String, Collection<ServiceEntityApiDTO>> result =
                graphDBService.findMultipleEntities(Optional.ofNullable(contextId),
                        Sets.newHashSet(requestedIds),
                        searchProjected ? TopologyType.PROJECTED : TopologyType.SOURCE);

        return Match(result).of(
            Case(Right($()), entities -> ResponseEntity.ok(entities)),
            Case(Left($()), err -> ResponseEntity.ok(Collections.emptyList())));
    }

    @RequestMapping(
            path = "/serviceentity/{id}",
            method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    @ResponseBody
    @ApiOperation("Search for a service entity by ID")
    public ResponseEntity<Collection<ServiceEntityApiDTO>> searchServiceEntityById(@PathVariable("id") final String id) {
        LOGGER.info("Searching service entity with id {}", id);
        Either<String, Collection<ServiceEntityApiDTO>> results = graphDBService.searchServiceEntityById(id);

        return Match(results).of(
                Case(Right($()), cs -> ResponseEntity.ok(cs)),
                Case(Left($()), err -> ResponseEntity.ok(Collections.emptyList()))
                                );
    }
}
