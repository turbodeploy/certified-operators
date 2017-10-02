package com.vmturbo.repository.controller;

import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.utils.ParamStrings;
import com.vmturbo.repository.service.SearchService;

@RequestMapping("/repository/search")
public class SearchController {

    private final Logger logger = LoggerFactory.getLogger(SearchController.class);

    private final SearchService searchService;

    public SearchController(final SearchService searchService) {
        this.searchService = searchService;
    }

    /**
     * Request a collection of {@link BaseApiDTO}s from the repository. Currently, only
     * support searching with entity types and scope.
     *
     * @param query Not yet used
     * @param types The types of entities, e.g., VirtualMachine, PhysicalMachine, ...
     * @param scope The scope used for searching, e.g., a single entity or the global environment
     * @param state Not yet used
     * @param groupType Not yet used
     * @param related Not yet used
     * @return
     * @throws Exception
     */
    @ApiOperation(value = "Get a list of search results")
    @RequestMapping(value = "", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public Collection<BaseApiDTO> getSearchResults(@ApiParam(value = "Query", required=false) @RequestParam(value = ParamStrings.QUERY, required=false) String query,
                                                   @ApiParam(value = "Object types", required=false) @RequestParam(value = ParamStrings.TYPES, required=false) List<String> types,
                                                   @ApiParam(value = "Scope", required=false) @RequestParam(value = ParamStrings.SCOPE, required=false) String scope,
                                                   @ApiParam(value = "State", required=false) @RequestParam(value = ParamStrings.STATE, required=false) String state,
                                                   @ApiParam(value = "GroupType", required=false) @RequestParam(value = ParamStrings.GROUP_TYPE, required=false) String groupType,
                                                   @ApiParam(value = "Get all related entities across supply chain", required=false, defaultValue="false")
                                                   @RequestParam(value=ParamStrings.RELATED, required=false, defaultValue="false") Boolean related) throws Exception {
        return searchService.getSearchResults(query, types, scope, state, related, groupType);
    }
}
