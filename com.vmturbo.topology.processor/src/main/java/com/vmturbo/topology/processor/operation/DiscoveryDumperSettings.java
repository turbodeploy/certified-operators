package com.vmturbo.topology.processor.operation;

import java.io.File;

public class DiscoveryDumperSettings {

    // location withiin component where discovery dumps will be saved
    public static final File DISCOVERY_DUMP_DIRECTORY = new File("/tmp/discovery");

    // name used for discover dump files when a proper target name cannot be constructed
    public static final String UNIDENTIFIED_TARGET_NAME = "unidentified-target";

}
