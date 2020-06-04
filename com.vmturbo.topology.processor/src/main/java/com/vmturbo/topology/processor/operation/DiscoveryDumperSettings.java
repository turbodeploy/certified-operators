package com.vmturbo.topology.processor.operation;

import java.io.File;

public class DiscoveryDumperSettings {

    // location withiin component where discovery dumps will be saved
    public static final File DISCOVERY_DUMP_DIRECTORY = new File("/tmp/discovery");

    /**
     * Directory for binary files containing discovery responses.
     */
    public static final File DISCOVERY_CACHE_DIRECTORY =
        new File("/home/turbonomic/data/cached_responses");

    // name used for discover dump files when a proper target name cannot be constructed
    public static final String UNIDENTIFIED_TARGET_NAME = "unidentified-target";

}
