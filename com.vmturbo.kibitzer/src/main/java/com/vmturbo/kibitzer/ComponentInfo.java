package com.vmturbo.kibitzer;

import com.google.common.base.CaseFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;

import com.vmturbo.sql.utils.DbPropertyProvider;

/**
 * Class that provides information needed by {@link Kibitzer} in order to run activities against a
 * given component.
 */
public abstract class ComponentInfo {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Obtain a {@link DbPropertyProvider} for the component, providing key property values needed
     * for DB provisioning and connection. These must be available as beans picked up by the Spring
     * component scan.
     *
     * @param context Spring application context
     * @return property provider instance
     */
    public final DbPropertyProvider getDbPropertyProvider(ApplicationContext context) {
        // class-level beans are named by default with a lower-camel rendering of the unqualified
        // class name. If the class for some component has a custom bean name, this method needs
        // to be overridden
        String simpleName = getDbPropertyProviderClass().getSimpleName();
        String beanName = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, simpleName);
        return (DbPropertyProvider)context.getBean(beanName);
    }

    protected abstract Class<? extends DbPropertyProvider> getDbPropertyProviderClass();

    protected abstract String getName();
}
