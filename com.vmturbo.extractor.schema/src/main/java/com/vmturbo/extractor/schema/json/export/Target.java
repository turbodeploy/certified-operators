package com.vmturbo.extractor.schema.json.export;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Representing the target which discovers an entity or group.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class Target {
    private Long oid;
    private String name;
    private String type;
    private String category;
    private String entityVendorId;

    public Long getOid() {
        return oid;
    }

    public void setOid(Long oid) {
        this.oid = oid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getEntityVendorId() {
        return entityVendorId;
    }

    public void setEntityVendorId(String entityVendorId) {
        this.entityVendorId = entityVendorId;
    }
}
