package com.vmturbo.extractor.export.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * The object to be exported to external system. Only one of the fields is set. Currently it either
 * contains entity or action.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class ExportedObject {

    private Entity entity;
    private Action action;

    @JsonIgnore
    private int serializedSize;

    public Entity getEntity() {
        return entity;
    }

    public void setEntity(Entity entity) {
        this.entity = entity;
    }

    public int getSerializedSize() {
        return serializedSize;
    }

    public void setSerializedSize(int serializedSize) {
        this.serializedSize = serializedSize;
    }

    @Override
    public String toString() {
        if (entity != null) {
            return entity.toString();
        } else if (action != null) {
            return action.toString();
        }
        return "";
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }
}
