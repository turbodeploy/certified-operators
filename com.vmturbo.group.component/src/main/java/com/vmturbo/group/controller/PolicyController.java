package com.vmturbo.group.controller;

import com.vmturbo.api.dto.PolicyApiDTO;
import com.vmturbo.api.dto.input.PolicyApiInputDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Collection;
import java.util.Collections;

@RequestMapping("/policy")
@ResponseBody
public class PolicyController {

    private final static Logger LOGGER = LoggerFactory.getLogger(PolicyController.class);

    @GetMapping(
            path = "/",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public ResponseEntity<Collection<PolicyApiDTO>> getAllPolicies() {
        return ResponseEntity.ok(Collections.emptyList());
    }

    @GetMapping(
            path = "/{policyID}",
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public ResponseEntity<PolicyApiDTO> getPolicy(@PathVariable("policyID") final String policyID) {
        return null;
    }

    @PostMapping(
            path = "/",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public ResponseEntity<String> createPolicy(@RequestBody final PolicyApiInputDTO policy) {
        return ResponseEntity.ok(null);
    }

    @DeleteMapping("/{policyID}")
    public ResponseEntity<String> deletePolicy(@PathVariable("policyID") final String policyID) {
        return ResponseEntity.ok(null);
    }

    @PutMapping(
            path = "/{policyID}",
            consumes = MediaType.APPLICATION_JSON_UTF8_VALUE,
            produces = MediaType.APPLICATION_JSON_UTF8_VALUE
    )
    public ResponseEntity<PolicyApiDTO> updatePolicy(
            @PathVariable("policyID") final String policyID,
            @RequestBody final PolicyApiInputDTO newPolicy) {
        return ResponseEntity.ok(null);
    }
}
