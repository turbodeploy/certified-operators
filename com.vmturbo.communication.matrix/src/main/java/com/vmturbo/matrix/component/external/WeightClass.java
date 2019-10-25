package com.vmturbo.matrix.component.external;

/**
 * The weight classes.
 * <ol>
 * <li> Blade - VMs inside a single Physical Host.
 * <li> Switch - Connected by a single switch
 * <li> Site - Inside a single site, connected by more than 1 switch/router.
 * <li> Cross-Site - Across sites. Hybrid cloud, different AWS regions,
 * multiple datacenter locations.
 * </ol>
 */
public enum WeightClass {
    /**
     * The weight class for a flow between two endpoints inside a single blade.
     */
    BLADE,

    /**
     * The weight class for a flow between two endpoints inside a single ToR switch.
     */
    SWITCH,

    /**
     * The weight class for a flow between two endpoints outside of a ToR switch, but withing the
     * same site.
     */
    SITE,

    /**
     * The weight class for a flow between two endpoints located in different sites, different
     * AWS regions, or hybrid-cloud.
     */
    CROSS_SITE
}
