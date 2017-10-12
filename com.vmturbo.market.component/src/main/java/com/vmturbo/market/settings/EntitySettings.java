package com.vmturbo.market.settings;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.analysis.AnalysisUtil;

/**
 * Entity settings.
 *
 */
public class EntitySettings {

    private EntitySettings() {}

    /**
     * Entity settings with boolean values.
     */
    public enum BooleanKey {

        ENABLE_PROVISION(false) {
            @Override
            public boolean value(TopologyEntityDTO entity) {
                return AnalysisUtil.CLONABLE_TYPES.contains(entity.getEntityType());
            }
        },

        ENABLE_SUSPEND(true);

        private boolean defaultValue;

        BooleanKey(boolean val) {
            defaultValue = val;
        }

        public boolean value(TopologyEntityDTO entity) {
            return defaultValue;
        }
    }

    /**
     * Entity settings with numerical values.
     */
    public enum NumericKey {

        DESIRED_UTILIZATION_MIN(0.65f),
        DESIRED_UTILIZATION_MAX(0.75f);

        private float defaultValue;

        NumericKey(float val) {
            defaultValue = val;
        }

        public float value(TopologyEntityDTO entity) {
            return defaultValue;
        }
    }
}
