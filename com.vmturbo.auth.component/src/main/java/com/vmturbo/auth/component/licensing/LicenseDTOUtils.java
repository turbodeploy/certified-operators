package com.vmturbo.auth.component.licensing;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.LicenseUtil;

/**
 * Utility functions for working with License DTO objects.
 */
public class LicenseDTOUtils {
    private static final Logger logger = LogManager.getLogger();

    private LicenseDTOUtils() {}

    /**
     * Triggers license validation using the common license validation routine in {@link LicenseUtil}
     * that is shared with Ops Manager, and also adds XL-specific license checks as well.
     *
     * @param license the {@link ILicense} object to run validation on.
     * @return a set of {@link ErrorReason} objects representing any validation issues discovered.
     */
    public static Set<ErrorReason> validateXLLicense(ILicense license) {
        // run base validations
        Set<ErrorReason> validationErrors = LicenseUtil.validate(license);

        // Now perform XL-specific validations
        // XL only understands VM entity counts, so reject non-VM licenses.
        if (license.getCountedEntity() != CountedEntity.VM) {
            validationErrors.add(ErrorReason.INCOMPATIBLE);
        }
        // XL will reject any licenses that don't have features.
        if (!license.hasFeatures()) {
            validationErrors.add(ErrorReason.INVALID_FEATURE_SET);
        }
        return validationErrors;
    }

    /**
     * Validates external license.
     *
     * @param license license DTO
     * @return a set of {@link ErrorReason} objects representing any validation issues discovered.
     */
    public static Set<ErrorReason> validateExternalLicense(@Nonnull LicenseDTO license) {
        final Set<ErrorReason> errorReasons = EnumSet.noneOf(ErrorReason.class);
        final ExternalLicense externalLicense = license.getExternal();
        if (externalLicense.hasExpirationDate() && LicenseUtil.isExpired(license)) {
            errorReasons.add(ErrorReason.EXPIRED);
        }
        return errorReasons;
    }

    /**
     * Convert a {@link LicenseDTO} object to a {@link License} object.
     *
     * @param licenseDTO the {@link LicenseDTO} to convert.
     * @return the resulting {@link License} object.
     */
    public static Optional<License> licenseDTOtoLicense(LicenseDTO licenseDTO) {
        License license = new License();
        doIf(licenseDTO.hasUuid(), () -> license.setUuid(licenseDTO.getUuid()));

        if (!licenseDTO.hasTurbo()) {
            return Optional.empty();
        }

        TurboLicense turboLicense = licenseDTO.getTurbo();
        doIf(turboLicense.hasEmail(), () -> license.setEmail(turboLicense.getEmail()));
        doIf(turboLicense.hasEdition(), () -> license.setEdition(turboLicense.getEdition()));
        doIf(turboLicense.hasExpirationDate(), () -> license.setExpirationDate(turboLicense.getExpirationDate()));
        doIf(turboLicense.hasLicenseOwner(), () -> license.setLicenseOwner(turboLicense.getLicenseOwner()));
        doIf(turboLicense.hasLicenseKey(), () -> license.setLicenseKey(turboLicense.getLicenseKey()));
        doIf(turboLicense.hasExternalLicenseKey(), () -> license.setExternalLicenseKey(turboLicense.getExternalLicenseKey()));
        doIf(licenseDTO.hasFilename(), () -> license.setFilename(licenseDTO.getFilename()));
        doIf(turboLicense.hasCountedEntity(),
                () -> CountedEntity.valueOfName(turboLicense.getCountedEntity())
                    .ifPresent(license::setCountedEntity));
        doIf(turboLicense.hasNumLicensedEntities(), () -> license.setNumLicensedEntities(turboLicense.getNumLicensedEntities()));
        doIf(turboLicense.getFeaturesCount() > 0, () -> license.addFeatures(turboLicense.getFeaturesList()));
        // we don't need to transcribe isValid() since it's calculated based on error reasons in the
        // shared ILicense objects.
        doIf(turboLicense.getErrorReasonCount() > 0,
            () -> license.setErrorReasons(turboLicense.getErrorReasonList().stream()
                .map(ErrorReason::valueOf)
                .collect(Collectors.toSet())));

        return Optional.of(license);
    }

    static void doIf(final boolean condition, @Nonnull final Runnable doIt) {
        if (condition) {
            doIt.run();
        }
    }

    /**
     * Convert a License to LicenseDTO object.
     * @param license the License to convert
     *
     * @return A corresponding LicenseDTO object that is equivalent to the License
     */
    public static LicenseDTO iLicenseToLicenseDTO(ILicense license) {
        LicenseDTO.Builder retBldr = LicenseDTO.newBuilder();
        if (null != license.getUuid()) {
            retBldr.setUuid(license.getUuid());
        }
        TurboLicense.Builder builder = retBldr.getTurboBuilder();
        if (null != license.getEmail()) {
            builder.setEmail(license.getEmail());
        }
        if (null != license.getEdition()) {
            builder.setEdition(license.getEdition());
        }
        if (null != license.getExpirationDate()) {
            builder.setExpirationDate(license.getExpirationDate());
        }
        if (null != license.getLicenseOwner()) {
            builder.setLicenseOwner(license.getLicenseOwner());
        }
        if (null != license.getLicenseKey()) {
            builder.setLicenseKey(license.getLicenseKey());
        }
        if (null != license.getExternalLicenseKey()) {
            builder.setExternalLicenseKey(license.getExternalLicenseKey());
        }
        if (null != license.getFilename()) {
            retBldr.setFilename(license.getFilename());
        }
        if (null != license.getCountedEntity()) {
            builder.setCountedEntity(license.getCountedEntity().name());
        }
        builder.setNumLicensedEntities(license.getNumLicensedEntities());
        if (null != license.getFeatures()) {
            builder.addAllFeatures(license.getFeatures());
        }

        builder.setIsValid(license.isValid());
        if (license.getErrorReasons().size() > 0) {
            builder.addAllErrorReason(license.getErrorReasons().stream()
                    .map(ErrorReason::name)
                    .collect(Collectors.toList()));
        }
        return retBldr.build();
    }
}
