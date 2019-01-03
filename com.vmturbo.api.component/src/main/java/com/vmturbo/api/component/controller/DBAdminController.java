package com.vmturbo.api.component.controller;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.xml.Jaxb2RootElementHttpMessageConverter;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import com.google.common.collect.ImmutableList;

import com.vmturbo.auth.api.db.DBPasswordDTO;
import com.vmturbo.components.api.ComponentRestTemplate;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * The DBAdminController provides methods for the RDBMS administration.
 */
@RestController
@RequestMapping("/dbadmin")
@Api(value = "/dbadmin", description = "Methods for managing Secure Storage")
public class DBAdminController {
    @Value("${authHost:auth}")
    private String authHost;

    @Value("${serverHttpPort}")
    private int authPort;

    /**
     * The synchronous client-side HTTP access.
     */
    private final RestTemplate restTemplate_;

    /**
     * Constructs the DBAdminController.
     */
    public DBAdminController() {
        restTemplate_ = ComponentRestTemplate.create();
    }

    /**
     * Composes the HTTP headers for REST calls.
     *
     * @return The HTTP headers.
     */
    private HttpHeaders composeHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(ImmutableList.of(MediaType.APPLICATION_JSON));
        return headers;
    }

    /**
     * Returns DB root password.
     *
     * @return The user resource URL if successful.
     * @throws Exception In case of an error adding user.
     */
    @ApiOperation(value = "Sets DB root password")
    @RequestMapping(path = "setDBRootPassword",
                    method = RequestMethod.PUT,
                    consumes = {MediaType.APPLICATION_JSON_VALUE},
                    produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public ResponseEntity<String> setDBRootPassword(@RequestBody final @Nonnull DBPasswordDTO dto)
            throws Exception {
        final String request = UriComponentsBuilder.newInstance()
                                                   .scheme("http")
                                                   .host(authHost)
                                                   .port(authPort)
                                                   .path("/securestorage/setDBRootPassword")
                                                   .build().toUriString();
        HttpHeaders headers = composeHttpHeaders();
        HttpEntity<DBPasswordDTO> entity = new HttpEntity<>(dto, headers);
        return restTemplate_.exchange(request, HttpMethod.PUT, entity, String.class);
    }
}
