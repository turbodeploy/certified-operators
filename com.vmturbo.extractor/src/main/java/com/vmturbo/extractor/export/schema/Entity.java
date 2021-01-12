package com.vmturbo.extractor.export.schema;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Class containing all the fields of an entity that need to be exported.
 * Note: Json property order is set to alphabetically to ensure all fields are in same order in
 * json doc to reduce storage space if customer uses Elasticsearch.
 * See https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-disk-usage.html#_put_fields_in_the_same_order_in_documents
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class Entity {

    private String timestamp;
    private Long id;
    private String name;
    private String type;
    private String environment;
    private String state;
    // mapping from attr key to attr value
    private Map<String, Object> attrs;
    // mapping from commodity type key to commodity value
    private Map<String, Commodity> metric;
    // mapping from related entity type key to list of related entities
    private Map<String, List<RelatedEntity>> related;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Map<String, Object> getAttrs() {
        return attrs;
    }

    public void setAttrs(Map<String, Object> attrs) {
        this.attrs = attrs;
    }

    public Map<String, Commodity> getMetric() {
        return metric;
    }

    public void setMetric(Map<String, Commodity> metric) {
        this.metric = metric;
    }

    public Map<String, List<RelatedEntity>> getRelated() {
        return related;
    }

    public void setRelated(Map<String, List<RelatedEntity>> related) {
        this.related = related;
    }
}

