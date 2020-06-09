package com.vmturbo.licensing.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import com.cisco.magellan.ciscoInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.macrovision.flexlm.FlexlmException;
import com.macrovision.flexlm.lictext.FeatureLine;
import com.macrovision.flexlm.lictext.LicenseCertificate;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.api.dto.license.LicenseApiDTO;

public class LicenseDeserializer {

    private static final Logger logger = LogManager.getLogger(LicenseDeserializer.class);

    private static final Pattern CWOM_VM_COUNT_PATTERN = Pattern.compile("<Count>([\\d]*)</Count>", Pattern.DOTALL);
    private static final XmlMapper XML_MAPPER = createXmlMapper();

    /**
     * Deserialize three flavors of licenses to LicenseApiDTO.
     * - turbo license xml v1
     * - turbo license xml v2
     * - CWOM license flexlm
     */
    public static LicenseApiDTO deserialize(String licenseData, String filename) throws IOException {
        if (isWellFormedXML(licenseData)) {
            return toDTO(deserializeTurboLicenseXmlToLicense(licenseData), filename);
        }
        if (isLicenseGeneratedByFlexlm(licenseData)) {
            return toDTO(deserializeCWOMLicenseToLicenseCertificate(licenseData), filename);
        }
        logger.warn("Invalid License content type");
        return LicenseApiDTO.createLicenseWithError(ErrorReason.INVALID_CONTENT_TYPE);
    }

    /**
     * Deserialize three flavors of licenses to LicenseApiDTO.
     */
    public static LicenseApiDTO deserialize(InputStream licenseData, String filename) throws IOException {
        return deserialize(IOUtils.toString(licenseData, "UTF-8"), filename);
    }

    /**
     * Convert LicenseXmlDTO to LicenseApiDTO
     */
    private static LicenseApiDTO toDTO(LicenseXmlDTO inputV2DTO, final String filename) {
        return new LicenseApiDTO()
                .setLicenseOwner(inputV2DTO.getFirstName() + " " + inputV2DTO.getLastName())
                .setEmail(inputV2DTO.getEmail())
                .setExpirationDate(inputV2DTO.getExpirationDate())
                .setFeatures(inputV2DTO.getFeatures())
                .setCountedEntity(inputV2DTO.getCountedEntity())
                .setNumLicensedEntities(inputV2DTO.getNumEntities())
                .setEdition(inputV2DTO.getEdition())
                .setFilename(filename)
                .setLicenseKey(inputV2DTO.getLockCode());
    }


    /**
     * Convert LicenseCertificate to LicenseApiDTO
     */
    private static LicenseApiDTO toDTO(LicenseCertificate licenseCertificate, final String filename) {
        FeatureLine featureLine = (FeatureLine) licenseCertificate.getFeatures().getFirst();

        Optional<CWOMLicenseEdition> cwomLicenseEdition = CWOMLicenseEdition.valueOfFeatureName(featureLine.getName());

        SortedSet<String> features = cwomLicenseEdition
                .map(CWOMLicenseEdition::getFeatures)
                .orElse(Collections.emptySortedSet());

        String edition = cwomLicenseEdition
                .map(CWOMLicenseEdition::name)
                .orElse(null);

        LicenseApiDTO license = new LicenseApiDTO()
                .setExternalLicenseKey(featureLine.getSignature())
                .setExternalLicense(true)
                .setFeatures(features)
                .setCountedEntity(ILicense.CountedEntity.VM)
                .setNumLicensedEntities(getLicensedVMCount(featureLine))
                .setExpirationDate(getExpirationDate(featureLine))
                .setEmail("support@cisco.com")
                .setLicenseOwner("cisco")
                .setEdition(edition)
                .setFilename(filename);
        license.addErrorReason(validateCWOMLicenseCertificate(licenseCertificate));

        if (license.isValid()) {
            license.setLicenseKey(LicenseUtil.generateLicenseKey(license));
        }

        return license;
    }

    private static String getExpirationDate(final FeatureLine featureLine) {
        Date time = featureLine.getExpirationDateObject().getCalendar().getTime();
        return ISODateTimeFormat.date().print(new DateTime(time));
    }

    private static int getLicensedVMCount(final FeatureLine featureLine) {
        Matcher matcher = CWOM_VM_COUNT_PATTERN.matcher(featureLine.getVendorString());
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : -1;
    }

    /**
     * Deserialize Turbo license XML to LicenseXmlDTO
     */
    private static LicenseXmlDTO deserializeTurboLicenseXmlToLicense(String xml) throws IOException {
        return XML_MAPPER.readValue(xml, LicenseXmlDTO.class);
    }

    /**
     * Deserialize CWOM license to a LicenseCertificate
     */
    private static LicenseCertificate deserializeCWOMLicenseToLicenseCertificate(String licenseData) {
        try {
            return new LicenseCertificate(new StringReader(licenseData), null, new ciscoInfo());
        } catch (FlexlmException | IOException e) {
            logger.warn("Invalid license entered.", e);
            return null;
        }
    }

    /**
     * Validate CWOM license certificates
     */
    @SuppressWarnings("unchecked")
    private static ErrorReason validateCWOMLicenseCertificate(LicenseCertificate licenseCertificate) {
        if (!licenseCertificate.getElementExceptions().isEmpty()) {
            licenseCertificate.getElementExceptions().stream()
                    .filter(elementException -> elementException instanceof FlexlmException)
                    .map(e -> ((FlexlmException) e).getBasicMessage())
                    .forEach(logger::warn);
            return ErrorReason.INVALID_CONTENT_TYPE;
        }
        try {
            LinkedList<FeatureLine> features = licenseCertificate.getFeatures();
            for (FeatureLine featureLine : features) {
                featureLine.authenticate();
            }
            return null;
        } catch (FlexlmException e) {
            logger.warn("Invalid license entered.", e);
            return ErrorReason.INVALID_LICENSE_KEY;
        }
    }

    /**
     * Determines if the License Data Provided by the user of a FlexLM License.
     */
    static boolean isLicenseGeneratedByFlexlm(String licenseData) {
        String trimmedData = StringUtils.trimToEmpty(licenseData);
        return trimmedData.startsWith("FEATURE") || trimmedData.startsWith("INCREMENT");
    }

    /**
     * Determines if the text is well formed XML
     */
    static boolean isWellFormedXML(String text) {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setValidating(false);
            factory.setNamespaceAware(true);
            SimpleErrorHandler errorHandler = new SimpleErrorHandler();
            XMLReader reader = factory.newSAXParser().getXMLReader();
            reader.setErrorHandler(errorHandler);
            reader.parse(new InputSource(IOUtils.toInputStream(text, "UTF-8")));
            return !errorHandler.isHasError();
        } catch (ParserConfigurationException | SAXException | IOException e) {
            return false;
        }
    }

    private static XmlMapper createXmlMapper() {
        XmlMapper xmlMapper = new XmlMapper();
        xmlMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return xmlMapper;
    }

    /**
     * SAX Error handler to capture xml deserialization errors
     */
    private static class SimpleErrorHandler implements ErrorHandler {
        private boolean hasError;

        public void warning(SAXParseException e) throws SAXException {
            hasError = true;
        }

        public void error(SAXParseException e) throws SAXException {
            hasError = true;
        }

        public void fatalError(SAXParseException e) throws SAXException {
            hasError = true;
        }

        public boolean isHasError() {
            return hasError;
        }
    }

    /**
     * DTO used by jackson-xml to deserialize turbo xml v1 and v2 licenses
     * Must have public access to be used by the XmlMapper
     */
    public static class LicenseXmlDTO {

        private String firstName;
        private String lastName;
        private String email;
        private int numSockets;
        private int vmTotal;
        private String expirationDate;
        private String lockCode;
        private String edition;

        private List<FeatureNode> featureNodes = new ArrayList<>();

        @JacksonXmlProperty(localName = "first-name")
        public String getFirstName() {
            return firstName;
        }

        public LicenseXmlDTO setFirstName(final String firstName) {
            this.firstName = firstName;
            return this;
        }

        @JacksonXmlProperty(localName = "last-name")
        public String getLastName() {
            return lastName;
        }

        public LicenseXmlDTO setLastName(final String lastName) {
            this.lastName = lastName;
            return this;
        }

        @JacksonXmlProperty(localName = "email")
        public String getEmail() {
            return email;
        }

        public LicenseXmlDTO setEmail(final String email) {
            this.email = email;
            return this;
        }

        @JacksonXmlProperty(localName = "num-sockets")
        public int getNumSockets() {
            return numSockets;
        }

        @JacksonXmlProperty(localName = "vm-total")
        public int getVmTotal() {
            return vmTotal;
        }

        public LicenseXmlDTO setVmTotal(final int total) {
            this.vmTotal = total;
            return this;
        }

        public int getNumEntities() {
            return Math.max(numSockets, vmTotal);
        }

        @JacksonXmlProperty(localName = "edition")
        public String getEdition() {
            return edition;
        }

        public LicenseXmlDTO setEdition(final String edition) {
            this.edition = edition;
            return this;
        }

        public LicenseXmlDTO setNumSockets(final int numSockets) {
            this.numSockets = numSockets;
            return this;
        }

        @JacksonXmlProperty(localName = "expiration-date")
        public String getExpirationDate() {
            return expirationDate;
        }

        public LicenseXmlDTO setExpirationDate(final String expirationDate) {
            this.expirationDate = expirationDate;
            return this;
        }

        @JacksonXmlProperty(localName = "lock-code")
        public String getLockCode() {
            return lockCode;
        }

        public LicenseXmlDTO setLockCode(final String lockCode) {
            this.lockCode = lockCode;
            return this;
        }

        @JacksonXmlProperty(localName = "feature")
        @JacksonXmlElementWrapper(useWrapping = false)
        public List<FeatureNode> getFeatureNodes() {
            return featureNodes;
        }

        public LicenseXmlDTO setFeatureNodes(List<FeatureNode> featureNodes) {
            this.featureNodes = featureNodes;
            return this;
        }

        public SortedSet<String> getFeatures() {
            return getFeatureNodes().stream()
                    .map(FeatureNode::getFeatureName)
                    .collect(Collectors.toCollection(TreeSet::new));
        }

        public LicenseXmlDTO setFeatures(List<String> features) {
            featureNodes = features.stream()
                    .map(FeatureNode::new)
                    .collect(Collectors.toList());
            return this;
        }

        public CountedEntity getCountedEntity() {
            if (numSockets > 0) {
                return ILicense.CountedEntity.SOCKET;
            }
            if (vmTotal > 0) {
                return ILicense.CountedEntity.VM;
            }
            return null;
        }

        public static class FeatureNode {
            private final String featureName;

            @JsonCreator()
            public FeatureNode(@JacksonXmlProperty(isAttribute = true, localName = "FeatureName") String featureName) {
                this.featureName = featureName;
            }

            public String getFeatureName() {
                return featureName;
            }
        }

    }
}

