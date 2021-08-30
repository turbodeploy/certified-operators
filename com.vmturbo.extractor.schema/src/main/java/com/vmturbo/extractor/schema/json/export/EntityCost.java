package com.vmturbo.extractor.schema.json.export;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import com.vmturbo.extractor.schema.enums.CostCategory;
import com.vmturbo.extractor.schema.enums.CostSource;

/**
 * The cost for an entity with breakdown by category.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class EntityCost {
    private Float total;
    private String unit;
    // cost breakdown by category
    @ExporterField(mapKeyEnum = CostCategory.class)
    private Map<String, CostByCategory> category = new HashMap<>();

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public Float getTotal() {
        return total;
    }

    public void setTotal(Float total) {
        this.total = total;
    }

    public Map<String, CostByCategory> getCategory() {
        return category;
    }

    public void setCategory(Map<String, CostByCategory> category) {
        this.category = category;
    }

    /**
     * Set the total cost for given category.
     *
     * @param costCategory cost category
     * @param categoryCost total cost for given category
     */
    public void setCategoryCost(String costCategory, Float categoryCost) {
        category.computeIfAbsent(costCategory, k -> new CostByCategory()).setTotal(categoryCost);
    }

    /**
     * Set the cost for given category and source.
     *
     * @param costCategory cost category
     * @param costSource cost source
     * @param sourceCost cost for given category and source
     */
    public void setSourceCost(String costCategory, String costSource, Float sourceCost) {
        category.computeIfAbsent(costCategory, k -> new CostByCategory()).setSourceCost(costSource, sourceCost);
    }

    /**
     * The cost for a category with breakdown by source.
     */
    @JsonInclude(Include.NON_EMPTY)
    @JsonPropertyOrder(alphabetic = true)
    public static class CostByCategory {
        private Float total;
        // cost breakdown by source
        @ExporterField(mapKeyEnum = CostSource.class)
        private Map<String, Float> source = new HashMap<>();

        public Float getTotal() {
            return total;
        }

        public void setTotal(Float total) {
            this.total = total;
        }

        public Map<String, Float> getSource() {
            return source;
        }

        public void setSource(Map<String, Float> source) {
            this.source = source;
        }

        /**
         * Set the cost for given source.
         *
         * @param costSource cost source
         * @param costValue cost value
         */
        public void setSourceCost(String costSource, Float costValue) {
            source.put(costSource, costValue);
        }
    }
}