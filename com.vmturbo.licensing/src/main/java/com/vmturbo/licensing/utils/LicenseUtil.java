package com.vmturbo.licensing.utils;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSortedSet;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.api.dto.license.LicenseApiDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.TurboEncryptionUtil.HashFunc;

public class LicenseUtil {

    /**
     * TODO: look into storing this secrete elsewhere...
     * not the best practice to keep secrets in source-code :(
     */
    private static final String LICENSE_SALT = "vmt_license_info";

    /**
     * Generate a license key
     */
    public static String generateLicenseKey(ILicense license) {
        String expirationDate = license.getExpirationDate();
        String email = license.getEmail();
        SortedSet<String> features = license.getFeatures();
        Integer numSockets = license.getNumLicensedEntities();
        CountedEntity countedEntity = license.getCountedEntity();
        String externalLicenseKey = license.getExternalLicenseKey();
        return generateLicenseKey(expirationDate, email, features, numSockets, countedEntity, externalLicenseKey);
    }

    /**
     * Generate a license key
     */
    public static String generateLicenseKey(String expirationDate,
                                            String email,
                                            Collection<String> features,
                                            int numSockets,
                                            CountedEntity countedEntity,
                                            String externalLicense) {
        String numSocketsLicensed = numSockets > -1 ? Integer.toString(numSockets) : "";
        String featuresText = canonicalizeFeatures(features);
        String counted = Objects.equals(countedEntity, CountedEntity.VM) ? CountedEntity.VM.name().toLowerCase() : "";
        String licenseData =
                LICENSE_SALT + email + expirationDate + counted + numSocketsLicensed + featuresText + StringUtils.trimToEmpty(externalLicense);
        return StringUtils.trimToEmpty(TurboEncryptionUtil.hash(licenseData, HashFunc.MD5));
    }

    /**
     * Validate a license and return a set of ErrorReason.
     * If no errors are found, then will return an empty set
     *
     * @param license The license to validate.
     */
    public static Set<ErrorReason> validate(ILicense license) {
        if (!license.isValid()) {
            return license.getErrorReasons();
        }
        Set<ErrorReason> errors = new LinkedHashSet<>();
        String email = license.getEmail();
        if (StringUtils.isBlank(email)) {
            errors.add(ErrorReason.INVALID_EMAIL);
        }

        if (license.isExpired()) {
            errors.add(ErrorReason.EXPIRED);
        }

        // Upgrading using an existing externalLicense (e.g. CWOM) may not have an externalLicenseKey and still be valid
        if (license.isExternalLicense() && StringUtils.isBlank(license.getExternalLicenseKey())) {
            return errors;
        }

        String licenseCode = license.getLicenseKey();
        String generatedLicenseCode = generateLicenseKey(license);

        if (StringUtils.isBlank(licenseCode) || !StringUtils.equals(generatedLicenseCode, licenseCode)) {
            errors.add(ErrorReason.INVALID_LICENSE_KEY);
        }
        return errors;
    }

    /**
     * Convert a License to LicenseApiDTO
     */
    public static LicenseApiDTO toDTO(License model) {
        LicenseApiDTO dto = new LicenseApiDTO()
                .setEmail(model.getEmail())
                .setExpirationDate(model.getExpirationDate())
                .setLicenseOwner(model.getLicenseOwner())
                .setLicenseKey(model.getLicenseKey())
                .setNumLicensedEntities(model.getNumLicensedEntities())
                .setNumInUseEntities(model.getNumInUseEntities())
                .setErrorReasons(model.getErrorReasons())
                .setExternalLicenseKey(model.getExternalLicenseKey())
                .setExternalLicense(model.isExternalLicense())
                .setEdition(model.getEdition())
                .setFilename(model.getFilename())
                .setCountedEntity(model.getCountedEntity())
                .setFeatures(model.getFeatures());
        dto.setUuid(model.getUuid());
        return dto;
    }

    /**
     * Convert a {@link LicenseDTO} to {@link License}.
     *
     * @param dto The {@link LicenseDTO} to convert.
     * @return The {@link License}, or an empty {@link Optional} if the {@link LicenseDTO} is an
     *          external license.
     */
    public static Optional<License> toModel(LicenseDTO dto) {
        if (dto.hasTurbo()) {
            TurboLicense turboLicense = dto.getTurbo();
            License license = new License().setUuid(dto.getUuid())
                    .setEmail(turboLicense.getEmail())
                    .setExpirationDate(turboLicense.getExpirationDate())
                    .setLicenseKey(turboLicense.getLicenseKey())
                    .setLicenseOwner(turboLicense.getLicenseOwner())
                    .setEdition(turboLicense.getEdition())
                    .setFilename(dto.getFilename())
                    .setCountedEntity(CountedEntity.valueOf(turboLicense.getCountedEntity()))
                    .setNumLicensedEntities(turboLicense.getNumLicensedEntities())
                    .setErrorReasons(turboLicense.getErrorReasonList().stream()
                        .map(ErrorReason::valueOf)
                        .collect(Collectors.toSet()))
                    .setFeatures(ImmutableSortedSet.<String>naturalOrder()
                        .addAll(turboLicense.getFeaturesList())
                        .build())
                    .setExternalLicenseKey(turboLicense.getExternalLicenseKey())
                    .setExternalLicense(turboLicense.hasExternalLicenseKey());
            license.setErrorReasons(LicenseUtil.validate(license));
            return Optional.of(license);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Extract the expiration date from a {@link LicenseDTO}.
     *
     * @param license The {@link LicenseDTO}.
     * @return The expiration date, or an empty string if none found.
     */
    public static String getExpirationDate(@Nonnull final LicenseDTO license) {
        switch (license.getTypeCase()) {
            case TURBO:
                return license.getTurbo().getExpirationDate();
            case EXTERNAL:
                return license.getExternal().getExpirationDate();
            default:
                return "";
        }
    }

    /**
     * Check if a license protobuf is expired.
     *
     * @param license The {@link LicenseDTO}.
     * @return True if it's expired. False otherwise.
     */
    public static boolean isExpired(LicenseDTO license) {
        return ILicense.isExpired(getExpirationDate(license));
    }

    /**
     * Canonicalize a collection of features into a string for comparison and generating a license key
     * - trim
     * - remove duplicates
     * - sort by natural order
     * - join with no separator
     */
    static String canonicalizeFeatures(final Collection<String> features) {
        if (features == null) {
            return "";
        }
        return features.stream()
                .map(StringUtils::trimToEmpty)
                .filter(StringUtils::isNotBlank)
                .distinct()
                .sorted()
                .collect(Collectors.joining());
    }

    /**
     * Compare features by canonicalizing them
     */
    public static boolean equalFeatures(final Collection<String> features1, final Collection<String> features2) {
        return StringUtils.equals(
                LicenseUtil.canonicalizeFeatures(features1),
                LicenseUtil.canonicalizeFeatures(features2)
        );
    }


}

