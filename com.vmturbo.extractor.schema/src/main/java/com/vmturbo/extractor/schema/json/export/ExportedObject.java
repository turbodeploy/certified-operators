package com.vmturbo.extractor.schema.json.export;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;

import com.vmturbo.extractor.schema.json.common.Constants;

/**
 * The object to be exported to external system. Only one of the fields is set. Currently it either
 * contains entity or action.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class ExportedObject {

    // when the entities are broadcasted or actions are exported
    @ExporterField(format = Constants.TIMESTAMP_PATTERN)
    private String timestamp;
    private Entity entity;
    private Action action;
    private Group group;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Entity getEntity() {
        return entity;
    }

    public void setEntity(Entity entity) {
        this.entity = entity;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    @Override
    public String toString() {
        final ToStringHelper stringHelper = MoreObjects.toStringHelper("ExportedObject")
                .omitNullValues()
                .add("timestamp", timestamp);
        if (entity != null) {
            stringHelper.add("entity", entity.toString());
        } else if (action != null) {
            stringHelper.add("action", action.toString());
        } else if (group != null) {
            stringHelper.add("group", group.toString());
        }
        return stringHelper.toString();
    }
}
