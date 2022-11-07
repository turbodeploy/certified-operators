package com.vmturbo.extractor.schema.json.export;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.MoreObjects;

import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.MetricType;

/**
 * Class containing all the fields of an entity that need to be exported.
 * Note: Json property order is set to alphabetically to ensure all fields are in same order in
 * json doc to reduce storage space if customer uses Elasticsearch.
 * See https://www.elastic.co/guide/en/elasticsearch/reference/current/tune-for-disk-usage.html#_put_fields_in_the_same_order_in_documents
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class Entity {

    private Long oid;
    private String name;
    @ExporterField(valueEnum = EntityType.class)
    private String type;
    @ExporterField(valueEnum = EnvironmentType.class)
    private String environment;
    @ExporterField(valueEnum = EntityState.class)
    private String state;
    @ExporterField(basedOnMetadata = true)
    private Map<String, Object> attrs;
    // mapping from commodity type key to commodity value
    @ExporterField(mapKeyEnum = MetricType.class)
    private Map<String, Commodity> metric;
    // mapping from related entity type key to list of related entities
    @ExporterField(mapKeyEnum = EntityType.class)
    private Map<String, List<RelatedEntity>> related;

    private AccountExpenses accountExpenses;

    private EntityCost cost;

    private List<Commodity> newMetric;

    private List<RelatedEntity> newRelated;

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

    public List<Commodity> getNewMetric() {
        return newMetric;
    }

    public void setNewMetric(List<Commodity> newMetric) {
        this.newMetric = newMetric;
    }

    public Map<String, List<RelatedEntity>> getRelated() {
        return related;
    }

    public void setRelated(Map<String, List<RelatedEntity>> related) {
        this.related = related;
    }

    public List<RelatedEntity> getNewRelated() {
        return newRelated;
    }

    public void setNewRelated(List<RelatedEntity> newRelated) {
        this.newRelated = newRelated;
    }

    public AccountExpenses getAccountExpenses() {
        return accountExpenses;
    }

    public void setAccountExpenses(AccountExpenses expenses) {
        this.accountExpenses = expenses;
    }

    public EntityCost getCost() {
        return cost;
    }

    public void setCost(EntityCost cost) {
        this.cost = cost;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper("Entity")
                .omitNullValues()
                .add("type", type)
                .add("oid", oid)
                .add("name", name)
                .toString();
    }
}

