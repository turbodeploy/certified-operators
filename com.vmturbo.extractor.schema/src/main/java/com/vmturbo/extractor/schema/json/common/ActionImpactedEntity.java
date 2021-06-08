package com.vmturbo.extractor.schema.json.common;

import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * An {@link ActionEntity} that also records the impact of all actions on certain commodities
 * inside the entity.
 */
@JsonInclude(Include.NON_EMPTY)
@JsonPropertyOrder(alphabetic = true)
public class ActionImpactedEntity extends ActionEntity {

    /**
     * (commodity type) -> (source and projected values of the commodity for this entity).
     *
     * <p/>Not all {@link ActionImpactedEntity}s are guaranteed to have affected metrics.
     */
    private Map<String, ImpactedMetric> affectedMetrics;

    private EntitySettings settings;

    @Nullable
    public Map<String, ImpactedMetric> getAffectedMetrics() {
        return affectedMetrics;
    }

    public void setAffectedMetrics(Map<String, ImpactedMetric> affectedMetrics) {
        this.affectedMetrics = affectedMetrics;
    }

    public EntitySettings getSettings() {
        return settings;
    }

    public void setSettings(EntitySettings settings) {
        this.settings = settings;
    }

    /**
     * The difference between the source and projected topologies for a particular metric.
     */
    @JsonInclude(Include.NON_EMPTY)
    @JsonPropertyOrder(alphabetic = true)
    public static class ImpactedMetric {
        private ActionCommodity beforeActions;
        private ActionCommodity afterActions;

        public ActionCommodity getAfterActions() {
            return afterActions;
        }

        public void setAfterActions(ActionCommodity afterActions) {
            this.afterActions = afterActions;
        }

        public ActionCommodity getBeforeActions() {
            return beforeActions;
        }

        public void setBeforeActions(ActionCommodity beforeActions) {
            this.beforeActions = beforeActions;
        }
    }

    /**
     * Information about a single commodity.
     */
    @JsonInclude(Include.NON_EMPTY)
    @JsonPropertyOrder(alphabetic = true)
    public static class ActionCommodity {
        private float used = 0.0f;
        private float capacity = 0.0f;
        private Double percentileUtilization;

        /**
         * Add to the used value of this commodity.
         *
         * @param used Amount to add.
         */
        public void addUsed(float used) {
            this.used += used;
        }

        /**
         * Add to the capacity of this commodity.
         *
         * @param capacity Amount to add.
         */
        public void addCapacity(float capacity) {
            this.capacity += capacity;
        }

        /**
         * Return commodity's used value.
         *
         * @return commodity's used value
         */
        public float getUsed() {
            return used;
        }

        /**
         * Return commodity's capacity value.
         *
         * @return commodity's capacity value.
         */
        public float getCapacity() {
            return capacity;
        }

        /**
         * Return commodity's utilization value.
         *
         * @return commodity's utilization value.
         */
        public float getUtilization() {
            if (capacity == 0.0) {
                return 0.0f;
            }

            return Math.round(used / capacity * 10000.0f) / 100.00f;
        }

        public Double getPercentileUtilization() {
            return percentileUtilization;
        }

        public void setPercentileUtilization(Double percentileUtilization) {
            this.percentileUtilization = percentileUtilization;
        }
    }
}
