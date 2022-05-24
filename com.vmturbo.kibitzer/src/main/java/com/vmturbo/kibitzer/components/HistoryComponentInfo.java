package com.vmturbo.kibitzer.components;

import com.vmturbo.history.db.HistoryDbPropertyConfig;
import com.vmturbo.kibitzer.ComponentInfo;
import com.vmturbo.sql.utils.DbPropertyProvider;

/**
 * {@link DbPropertyProvider} implementation for history component.
 */
public class HistoryComponentInfo extends ComponentInfo {

    private static final String HISTORY_COMPONENT_NAME = "history";

    private HistoryComponentInfo() {}

    private static final HistoryComponentInfo INSTANCE = new HistoryComponentInfo();

    /**
     * Get the singleton instance for this property provider.
     *
     * @return property provider instance
     */
    public static HistoryComponentInfo get() {
        return INSTANCE;
    }

    public Class<? extends DbPropertyProvider> getDbPropertyProviderClass() {
        return HistoryDbPropertyConfig.class;
    }

    public String getName() {
        return HISTORY_COMPONENT_NAME;
    }
}
