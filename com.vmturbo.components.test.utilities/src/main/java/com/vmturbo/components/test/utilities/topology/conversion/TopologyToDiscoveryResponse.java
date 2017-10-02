package com.vmturbo.components.test.utilities.topology.conversion;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * A utility for converting classic/legacy XML topologies into SDK probe protobuf discovery result.
 *
 * A single stitched topology will be converted into a single discovery result.
 *
 * Discovery results exported via this utility may be injected into the system using the
 * {@code DelegatingProbe}.
 *
 * The converter does not attempt to convert all entity-specific data. The focus at the moment
 * is on converting entity information related to the operation of the market into a format
 * that can be ingested by XL.
 *
 * Uses JAXB XMl binding with a {@link javax.xml.transform.stream.StreamSource}
 * for the combination of reasonable performance with readability and ease of use.
 *
 * Makes absolutely NO references to the EMF-bound parts of the classic codebase.
 */
public class TopologyToDiscoveryResponse {

    private static final Logger logger = LogManager.getLogger();

    private final String filename;

    public TopologyToDiscoveryResponse(@Nonnull final String filename) {
        this.filename = filename;
    }

    /**
     * Convert a classic topology into a {@link DiscoveryResponse} suitable for returning
     * as the result of a discovery from a probe (see for example the DelegatingProbe).
     *
     * @return A probe DiscoveryResponse equivalent of the classic topology.
     * @throws IOException If the file cannot be found or read.
     * @throws JAXBException If there is an exception parsing the topology XML.
     */
    public DiscoveryResponse convert() throws IOException, JAXBException {
        final StreamSource streamSource = new StreamSource(new File(filename));
        final JAXBContext jc = JAXBContext.newInstance(XmiElement.class);
        final Unmarshaller unmarshaller = jc.createUnmarshaller();
        final XmiElement xmi = (XmiElement)unmarshaller.unmarshal(streamSource);

        logger.info("Parsed {} markets from file {}.", xmi.getMarkets().size(), filename);
        logger.info("Main market entities: {}", xmi.getMainMarket().getServiceEntities()
            .stream()
            .filter(ServiceEntityElement::isServiceEntity)
            .collect(Collectors.groupingBy(ServiceEntityElement::getEntityType, Collectors.counting())));

        return xmi.getMainMarket().toSdkProto();
    }

    /**
     * Convert a classic topology into a {@link DiscoveryResponse} suitable for returning
     * as the result of a discovery from a probe (see for example the DelegatingProbe).
     * Return the response after serializing it to JSON.
     *
     * @return A probe DiscoveryResponse equivalent of the classic topology serialized into JSON.
     * @throws IOException If the file cannot be found or read.
     * @throws JAXBException If there is an exception parsing the topology XML.
     */
    public String convertToJson() throws IOException, JAXBException {
        return ComponentGsonFactory.createGson()
            .toJson(convert());
    }

    /**
     * Convert a discovery response serialized from a JSON file back into a {@link DiscoveryResponse}.
     *
     * @param filePath The name of the file containing the serialized JSON to be converted.
     * @return A {@link DiscoveryResponse} deserialized from the JSON in the file.
     * @throws IOException If an error occurs when reading the serialized file.
     */
    public static DiscoveryResponse fromJsonFile(@Nonnull final String filePath) throws IOException {
        final InputStreamReader reader =
            new InputStreamReader(FileUtils.openInputStream(new File(filePath)));

        return ComponentGsonFactory.createGson().fromJson(reader, DiscoveryResponse.class);
    }
}
