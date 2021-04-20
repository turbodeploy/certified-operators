package com.vmturbo.licensing.utils;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.Optional;
import java.util.SortedSet;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.cisco.magellan.ciscoInfo;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
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

import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense.Type;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;

public class LicenseDeserializer {

    private static final Logger logger = LogManager.getLogger(LicenseDeserializer.class);

    private static final Pattern CWOM_VM_COUNT_PATTERN = Pattern.compile("<Count>([\\d]*)</Count>", Pattern.DOTALL);
    private static final XmlMapper XML_MAPPER = createXmlMapper();
    private static final String DISALLOW_DOCTYPE_DECL =
            "http://apache.org/xml/features/disallow-doctype-decl";
    private static final String EXTERNAL_GENERAL_ENTITIES =
            "http://xml.org/sax/features/external-general-entities";

    /**
     * Deserialize 4 flavors of licenses to LicenseDTO.
     * - turbo license xml v1
     * - turbo license xml v2
     * - CWOM license flexlm
     * - external license (e.g. Grafana .jwt license)
     *
     * @param licenseData The license string, which is the raw license file contents.
     * @param filename The name of the license file (for display purposes).
     * @return The {@link LicenseDTO}. An invalid input will generate a license with an
     *         {@link ErrorReason}.
     */
    public static LicenseDTO deserialize(String licenseData, String filename) {
        LicenseDTO.Builder retBldr = LicenseDTO.newBuilder();
        if (filename != null) {
            retBldr.setFilename(filename);
        }
        if (isWellFormedXML(licenseData)) {
            retBldr.setTurbo(deserializeXmlTurboLicense(licenseData));
        } else if (isGrafanaJwt(licenseData)) {
            retBldr.setExternal(deserializeExternalGrafanaLicense(licenseData));
        } else if (isLicenseGeneratedByFlexlm(licenseData)) {
            retBldr.setTurbo(deserializeCWOMLicenseToTurboLicense(licenseData));
        } else {
            logger.warn("Invalid License content type");
            retBldr.setTurbo(TurboLicense.newBuilder()
                .addErrorReason(ErrorReason.INVALID_CONTENT_TYPE.name()));
        }
        return retBldr.build();
    }

    private static String getExpirationDate(final FeatureLine featureLine) {
        Date time = featureLine.getExpirationDateObject().getCalendar().getTime();
        return ISODateTimeFormat.date().print(new DateTime(time));
    }

    private static int getLicensedVMCount(final FeatureLine featureLine) {
        Matcher matcher = CWOM_VM_COUNT_PATTERN.matcher(featureLine.getVendorString());
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : -1;
    }

    @Nonnull
    private static ExternalLicense deserializeExternalGrafanaLicense(String jwt) {
        DecodedJWT decoded = JWT.decode(jwt);

        return ExternalLicense.newBuilder()
            .setExpirationDate(DateTimeUtil.formatDate(decoded.getExpiresAt()))
            .setType(Type.GRAFANA)
            .setPayload(jwt)
            .build();
    }

    /**
     * Deserialize Turbo license XML to LicenseXmlDTO
     *
     * @param xml The XML license string.
     * @return The {@link TurboLicense}.
     */
    @Nonnull
    private static TurboLicense deserializeXmlTurboLicense(String xml) {
        try {
            LicenseXmlDTO xmlDto = XML_MAPPER.readValue(xml, LicenseXmlDTO.class);
            TurboLicense.Builder bldr = TurboLicense.newBuilder().setLicenseOwner(
                    xmlDto.getFirstName() + " " + xmlDto.getLastName());
            setIfNotNull(xmlDto.getEmail(), bldr::setEmail);
            // Using a default value instead of leaving the field unset here will allow us to
            // distinguish between cases where a newly installed license doesn't have the customer
            // ID vs cases where a previously saved DTO that didn't have the ID is migrated.
            // This customer ID is only intended to be sent as-is with telemetry data.
            bldr.setCustomerId(xmlDto.getCustomerId() == null
                ? "missing from license file"
                : xmlDto.getCustomerId());
            setIfNotNull(xmlDto.getExpirationDate(), bldr::setExpirationDate);
            setIfNotNull(xmlDto.getFeatures(), bldr::addAllFeatures);
            setIfNotNull(xmlDto.getCountedEntity(), e -> bldr.setCountedEntity(e.name()));
            setIfNotNull(xmlDto.getNumEntities(), bldr::setNumLicensedEntities);
            setIfNotNull(xmlDto.getEdition(), bldr::setEdition);
            setIfNotNull(xmlDto.getLockCode(), bldr::setLicenseKey);
            return bldr.build();
        } catch (IOException e) {
            return TurboLicense.newBuilder()
                    .addErrorReason(ErrorReason.INVALID_CONTENT_TYPE.name())
                    .build();
        }
    }

    private static <T> void setIfNotNull(T val, Consumer<T> setter) {
        if (val != null) {
            setter.accept(val);
        }
    }

    /**
     * Deserialize CWOM license to a LicenseCertificate
     *
     * @param licenseData The license string.
     * @return The {@link TurboLicense}.
     */
    @Nonnull
    private static TurboLicense deserializeCWOMLicenseToTurboLicense(String licenseData) {
        try {
            LicenseCertificate licenseCertificate = new LicenseCertificate(new StringReader(licenseData), null, new ciscoInfo());
            FeatureLine featureLine = (FeatureLine) licenseCertificate.getFeatures().getFirst();

            Optional<CWOMLicenseEdition> cwomLicenseEdition = CWOMLicenseEdition.valueOfFeatureName(featureLine.getName());

            SortedSet<String> features = cwomLicenseEdition
                    .map(CWOMLicenseEdition::getFeatures)
                    .orElse(Collections.emptySortedSet());

            String edition = cwomLicenseEdition
                    .map(CWOMLicenseEdition::name)
                    .orElse(null);

            CountedEntity countedEntity = CountedEntity.VM;
            TurboLicense.Builder bldr = TurboLicense.newBuilder()
                .addAllFeatures(features)
                .setCountedEntity(countedEntity.name())
                .setEmail("support@cisco.com")
                .setLicenseOwner("cisco");
            setIfNotNull(featureLine.getSignature(), bldr::setExternalLicenseKey);
            setIfNotNull(edition, bldr::setEdition);
            setIfNotNull(getLicensedVMCount(featureLine), bldr::setNumLicensedEntities);
            setIfNotNull(getExpirationDate(featureLine), bldr::setExpirationDate);

            ErrorReason errorReason = validateCWOMLicenseCertificate(licenseCertificate);
            if (errorReason == null) {
                bldr.setLicenseKey(LicenseUtil.generateLicenseKey(bldr.getExpirationDate(),
                        bldr.getEmail(), features, bldr.getNumLicensedEntities(),
                        countedEntity, bldr.getExternalLicenseKey()));
            } else {
                bldr.addErrorReason(errorReason.name());
            }

            return bldr.build();
        } catch (FlexlmException | IOException e) {
            logger.warn("Invalid license entered.", e);
            return TurboLicense.newBuilder()
                .addErrorReason(ErrorReason.INVALID_CONTENT_TYPE.name())
                .build();
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

    static boolean isGrafanaJwt(String text) {
        try {
            DecodedJWT decoded = JWT.decode(text);
            return decoded.getIssuer().contains("grafana");
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Determines if the text is well formed XML
     */
    static boolean isWellFormedXML(String text) {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setValidating(false);
            factory.setNamespaceAware(true);
            factory.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
            factory.setFeature(DISALLOW_DOCTYPE_DECL, true);
            SimpleErrorHandler errorHandler = new SimpleErrorHandler();
            XMLReader reader = factory.newSAXParser().getXMLReader();
            reader.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
            reader.setErrorHandler(errorHandler);
            reader.parse(new InputSource(IOUtils.toInputStream(text, "UTF-8")));
            return !errorHandler.isHasError();

        } catch (SAXParseException e) {
            logger.error("License parse exception.", e);
            if (e.getMessage().contains(DISALLOW_DOCTYPE_DECL)) {
                throw new SecurityException("License parse exception", e);
            }
            return false;
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

}

