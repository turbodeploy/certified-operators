package com.vmturbo.extractor.schema.json.export;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.MoreObjects;

import com.vmturbo.extractor.schema.json.common.ActionEntity;
import com.vmturbo.extractor.schema.json.common.CommodityChange;
import com.vmturbo.extractor.schema.json.common.DeleteInfo;
import com.vmturbo.extractor.schema.json.common.MoveChange;

/**
 * Class containing all the fields of an entity or action that need to be exported.
 * Note: Json property order is set to alphabetically to ensure all fields are in same order in
 * json doc to reduce storage space if customer uses Elasticsearch.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class Action {

    private Long oid;
    private String creationTime;
    private String type;
    private String state;
    private String mode;
    private String category;
    private String severity;
    private String description;
    private String explanation;
    private ActionSavings savings;
    private ActionEntity target;
    // mapping from related entity type key to list of related entities
    private Map<String, List<RelatedEntity>> related;

    // below are type specific info
    // mapping from entity type to provider change
    private Map<String, MoveChange> moveInfo;
    // mapping from commodity type to commodity change
    private Map<String, CommodityChange> resizeInfo;
    // info for delete action
    private DeleteInfo deleteInfo;

    public Long getOid() {
        return oid;
    }

    public void setOid(Long oid) {
        this.oid = oid;
    }

    public Map<String, List<RelatedEntity>> getRelated() {
        return related;
    }

    public void setRelated(Map<String, List<RelatedEntity>> related) {
        this.related = related;
    }

    public String getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(String creationTime) {
        this.creationTime = creationTime;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getSeverity() {
        return severity;
    }

    public void setSeverity(String severity) {
        this.severity = severity;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getExplanation() {
        return explanation;
    }

    public void setExplanation(String explanation) {
        this.explanation = explanation;
    }

    public ActionEntity getTarget() {
        return target;
    }

    public void setTarget(ActionEntity target) {
        this.target = target;
    }

    public Map<String, MoveChange> getMoveInfo() {
        return moveInfo;
    }

    public void setMoveInfo(Map<String, MoveChange> moveInfo) {
        this.moveInfo = moveInfo;
    }

    public Map<String, CommodityChange> getResizeInfo() {
        return resizeInfo;
    }

    public void setResizeInfo(Map<String, CommodityChange> resizeInfo) {
        this.resizeInfo = resizeInfo;
    }

    public ActionSavings getSavings() {
        return savings;
    }

    public void setSavings(ActionSavings savings) {
        this.savings = savings;
    }

    public DeleteInfo getDeleteInfo() {
        return deleteInfo;
    }

    public void setDeleteInfo(DeleteInfo deleteInfo) {
        this.deleteInfo = deleteInfo;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("Action")
                .omitNullValues()
                .add("type", type)
                .add("oid", oid)
                .toString();
    }
}

