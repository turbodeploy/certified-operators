package com.vmturbo.auth.component.licensing;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.LicenseUtil;

/**
 * Utility functions for working with License DTO objects.
 */
public class LicenseDTOUtils {

    /**
     * Triggers license validation using the common license validation routine in {@link LicenseUtil}
     * that is shared with Ops Manager, and also adds XL-specific license checks as well.
     *
     * @param license the {@link ILicense} object to run validation on.
     * @return a set of {@link ErrorReason} objects representing any validation issues discovered.
     */
    static public Set<ErrorReason> validateXLLicense(ILicense license) {
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
     * Convert a {@link LicenseDTO} object to a {@link License} object.
     *
     * @param licenseDTO the {@link LicenseDTO} to convert.
     * @return the resulting {@link License} object.
     */
    static public License licenseDTOtoLicense(LicenseDTO licenseDTO) {
        License license = new License();
        if (licenseDTO.hasUuid()) {
            license.setUuid(licenseDTO.getUuid());
        }
        if (licenseDTO.hasEmail()) {
            license.setEmail(licenseDTO.getEmail());
        }
        if (licenseDTO.hasEdition()) {
            license.setEdition(licenseDTO.getEdition());
        }
        if (licenseDTO.hasExpirationDate()) {
            license.setExpirationDate(licenseDTO.getExpirationDate());
        }
        if (licenseDTO.hasLicenseOwner()) {
            license.setLicenseOwner(licenseDTO.getLicenseOwner());
        }
        if (licenseDTO.hasLicenseKey()) {
            license.setLicenseKey(licenseDTO.getLicenseKey());
        }
        if (licenseDTO.hasExternalLicenseKey()) {
            license.setExternalLicenseKey(licenseDTO.getExternalLicenseKey());
        }
        if (licenseDTO.hasFilename()) {
            license.setFilename(licenseDTO.getFilename());
        }
        if (licenseDTO.hasCountedEntity()) {
            CountedEntity.valueOfName(licenseDTO.getCountedEntity())
                    .ifPresent(license::setCountedEntity);
        }
        if (licenseDTO.hasNumLicensedEntities()) {
            license.setNumLicensedEntities(licenseDTO.getNumLicensedEntities());
        }
        if (licenseDTO.getFeaturesCount() > 0) {
            license.addFeatures(licenseDTO.getFeaturesList());
        }

        // we don't need to transcribe isValid() since it's calculated based on error reasons in the
        // shared ILicense objects.
        if (licenseDTO.getErrorReasonCount() > 0) {
            license.setErrorReasons(licenseDTO.getErrorReasonList().stream()
                    .map(ErrorReason::valueOf)
                    .collect(Collectors.toSet()));
        }

        return license;
    }


    /**
     * Convert a License to LicenseDTO object.
     * @param license the License to convert
     *
     * @return A corresponding LicenseDTO object that is equivalent to the License
     */
    static public LicenseDTO iLicenseToLicenseDTO(ILicense license) {
        LicenseDTO.Builder builder = LicenseDTO.newBuilder();
        if (null != license.getUuid()) {
            builder.setUuid(license.getUuid());
        }
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
            builder.setFilename(license.getFilename());
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
        return builder.build();
    }

    /**
     * Given a collection of {@link LicenseDTO}s, combine them into a single "aggregate" license using
     * the ILicense.combine() method.
     *
     * The aggregate license contains the total licensed workload count for all of the source licenses,
     * as well as the union of all of their features. Inactive or invalid licenses are skipped, and
     * do not contribute to the aggregate information.
     *
     * @param licenseDTOs
     * @return
     */
    static public License combineLicenses(Collection<LicenseDTO> licenseDTOs) {
        // we have licenses -- convert them to model licenses, validate them, and merge them together.
        License aggregateLicense = new License();

        for (LicenseDTO licenseDTO : licenseDTOs) {
            License license = LicenseDTOUtils.licenseDTOtoLicense(licenseDTO);
            license.setErrorReasons(LicenseDTOUtils.validateXLLicense(license));

            aggregateLicense.combine(license); // merge subsequent licenses into the first one
        }

        return aggregateLicense;
    }

    /**
     * Convert a {@link License } to a {@link LicenseSummary} object.
     *
     * @param aggregateLicense An aggregate license created by merging all of the valid licenses.
     * @param isOverLimit true, if the workload count is over the license limit.
     * @return A shiny new LicenseSummary.
     */
    static public LicenseSummary createLicenseSummary(ILicense aggregateLicense, boolean isOverLimit) {
        LicenseSummary.Builder summaryBuilder = LicenseSummary.newBuilder();

        if (StringUtils.isNotBlank(aggregateLicense.getExpirationDate())) {
            summaryBuilder.setExpirationDate(aggregateLicense.getExpirationDate());
            summaryBuilder.setIsExpired(aggregateLicense.isExpired());
        }

        if (aggregateLicense.getCountedEntity() != null) {
            summaryBuilder.setCountedEntity(aggregateLicense.getCountedEntity().name());
            summaryBuilder.setNumLicensedEntities(aggregateLicense.getNumLicensedEntities());
            summaryBuilder.setNumInUseEntities(aggregateLicense.getNumInUseEntities());

            summaryBuilder.setIsOverEntityLimit(isOverLimit);
        }

        // the aggregate feature set available across all of the stored licenses
        summaryBuilder.addAllFeature(aggregateLicense.getFeatures());

        summaryBuilder.addAllErrorReason(aggregateLicense.getErrorReasons().stream()
            .map(ErrorReason::name)
            .collect(Collectors.toList()));

        summaryBuilder.setIsValid(aggregateLicense.isValid());

        // set generation date to now.
        summaryBuilder.setGenerationDate(DateTimeUtil.getNow());

        return summaryBuilder.build();
    }
}
