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

    private ActionEntity from;
    private ActionEntity to;
    // resources to move, like volumes of a VirtualMachine
    private List<ActionEntity> resource;

    public ActionEntity getFrom() {
        return from;
    }

    public void setFrom(ActionEntity from) {
        this.from = from;
    }

    public ActionEntity getTo() {
        return to;
    }

    public void setTo(ActionEntity to) {
        this.to = to;
    }

    public List<ActionEntity> getResource() {
        return resource;
    }

    public void setResource(List<ActionEntity> resource) {
        this.resource = resource;
    }
}