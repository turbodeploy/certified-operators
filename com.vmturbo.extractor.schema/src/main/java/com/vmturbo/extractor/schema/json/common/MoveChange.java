package com.vmturbo.extractor.schema.json.common;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Describing a provider change in move action.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class MoveChange {

    private ActionImpactedEntity from;
    private ActionImpactedEntity to;
    // resources to move, like volumes of a VirtualMachine
    private List<ActionEntity> resource;

    public ActionImpactedEntity getFrom() {
        return from;
    }

    public void setFrom(ActionImpactedEntity from) {
        this.from = from;
    }

    public ActionImpactedEntity getTo() {
        return to;
    }

    public void setTo(ActionImpactedEntity to) {
        this.to = to;
    }

    public List<ActionEntity> getResource() {
        return resource;
    }

    public void setResource(List<ActionEntity> resource) {
        this.resource = resource;
    }
}