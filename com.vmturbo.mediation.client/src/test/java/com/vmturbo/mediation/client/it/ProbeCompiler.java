package com.vmturbo.mediation.client.it;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import com.vmturbo.mediation.common.ProbeProperties;
import com.vmturbo.mediation.common.ProbeXMLConfiguration;
import com.vmturbo.mediation.common.tests.util.SdkProbe;

/**
 * Utility class to create probe configuration.
 *
 */
public class ProbeCompiler {

    private static final JAXBContext PROBE_CONTEXT;

    static {
        try {
            PROBE_CONTEXT = JAXBContext.newInstance(ProbeXMLConfiguration.class);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    private ProbeCompiler() {}

    /**
     * Creates probe configuration file in the specified directory.
     *
     * @param outDir directory where to create the configuration files
     * @param probe probe to create configuration files for
     * @throws Exception on errors occur
     */
    public static void configureProbe(File outDir, SdkProbe probe) throws Exception {
        createConfigFile(outDir, probe.getProbeConfig().getProbeClass(),
                        probe.getProbeConfig().getExecutorClass(), probe.getType());
        createIdentityMetadataFile(outDir);
    }

    /**
     * Creates probe configuration file in the specified directory.
     *
     * @param outDir directory where to create the configuration file
     * @param probeClassName probe class name to use
     * @param executorClassName action executor class name to use
     * @param probeType probe type
     * @throws Exception on errors occur
     */
    private static void createConfigFile(File outDir, String probeClassName,
                    String executorClassName, String probeType) throws Exception {
        final ProbeXMLConfiguration config = new ProbeXMLConfiguration();
        config.setFullClassName(probeClassName);
        config.setExecutorClassName(executorClassName);
        config.setType(probeType);
        config.setCategory("CUSTOM");
        config.setVersion("1.0.0");
        final Marshaller marshaller = PROBE_CONTEXT.createMarshaller();
        try (final OutputStream fos =
                        new FileOutputStream(new File(outDir, ProbeProperties.PROBE_CONF_FILE))) {
            marshaller.marshal(config, fos);
        }
    }

    /**
     * Creates the identity metadata file for a probe.  This creates a simple default
     * file and puts it in the resources directory of the probe.
     *
     * @param outDir base directory of the probe. The file will be created in outDir + "/resources/identity-metadata.yml"
     * @throws Exception on errors occur
     */
    private static void createIdentityMetadataFile(File outDir) throws Exception {
        // This will create a default identity meta data file with a default list of classes
        List<String> classNames = Arrays.asList("VIRTUAL_MACHINE:", "DATACENTER:", "PHYSICAL_MACHINE:",
                                                "DISK_ARRAY:", "STORAGE:", "APPLICATION:", "NETWORK:");
        String nonVolatileProps = "  nonVolatileProperties:";
        String idProperty = "    - id";

        final StringBuilder sb = new StringBuilder();
        // For each class add the id as a non volatile property
        for (String className : classNames) {
            sb.append( "\n\n").append(className).append("\n");
            sb.append(nonVolatileProps).append("\n");
            sb.append(idProperty);
        }

        // Make sure the resources directory exists and if it does not create it
        final String identityFileDir = outDir + "/resources";
        final String identityFileName = "identity-metadata.yml";
        final File dst = new File(identityFileDir);
        if (!dst.exists()) {
            if (!dst.mkdirs()) {
                throw new RuntimeException("Error creating directory " + dst);
            }
        }
        // Write the file
        try (OutputStream fos = new FileOutputStream(new File(dst, identityFileName))) {
            // create the identity meta data file and write the data to it
            byte[] bytesArray = sb.toString().getBytes();
            fos.write(bytesArray);
            fos.flush();
        }
    }

}
