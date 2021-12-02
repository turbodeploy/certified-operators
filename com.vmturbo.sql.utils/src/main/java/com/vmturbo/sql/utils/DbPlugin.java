package com.vmturbo.sql.utils;

/**
 * Interface for defining database plugins that can be configured for DbEndpoints.
 */
public interface DbPlugin {

    /**
     * Return the name of the plugin, as it should appear in SQL to activate the plugin.
     *
     * @return plugin name
     */
    String getName();

    /**
     * Return the version of this plugin.
     *
     * @return version string
     */
    String getVersion();

    /**
     * Return SQL that should be executed to activate this plugin.
     *
     * @param schemaName name of schema where plugin will be used
     * @return SQL to activate plugin
     */
    String getInstallSQL(String schemaName);
}
