package com.vmturbo.api.component.external.api.mapper;

import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.api.dto.license.LicenseApiDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;

/**
 * Conversions between {@link LicenseApiDTO}, {@link LicenseDTO} and {@link LicenseSummary} objects.
 */
public class LicenseMapper {

    /**
     * Convert a {@link LicenseDTO} to {@link LicenseApiDTO} object.
     *
     * @param licenseDTO the {@link LicenseDTO} to convert
     * @return A corresponding {@link LicenseApiDTO} object that is equivalent to the LicenseDTO
     */
    static public LicenseApiDTO licenseDTOtoLicenseApiDTO(LicenseDTO licenseDTO) {
        LicenseApiDTO license = new LicenseApiDTO();
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
     * Convert a {@link LicenseApiDTO} to {@link LicenseDTO} object.
     *
     * @param license the {@link LicenseApiDTO} to convert
     * @return A corresponding {@link LicenseDTO} object that is equivalent to the input license.
     */
    static public LicenseDTO licenseApiDTOtoLicenseDTO(LicenseApiDTO license) {
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
        return builder.build();
    }

    /**
     * Convert a {@link LicenseSummary} to a {@link LicenseApiDTO}.
     *
     * The license summary doesn't contain all of the license properties, so the resulting license
     * will only provide summary information, such as the available feature set, workload count, and
     * validation-related information.
     *
     * @param licenseSummary The {@link LicenseSummary} to convert.
     * @return The {@link LicenseApiDTO} created from the License Summary.
     */
    static public LicenseApiDTO licenseSummaryToLicenseApiDTO(LicenseSummary licenseSummary) {
        LicenseApiDTO retVal = new LicenseApiDTO();

        if (licenseSummary.hasExpirationDate()) {
            retVal.setExpirationDate(licenseSummary.getExpirationDate());
        }
        // --- NOTE: not handling isExpired or isValid ---
        // there is no explicit setter for "isExpired" in LicenseApiDTO -- this is a calculated
        // field in Ops Manager based on the expiration date field. If expiration date is blank,
        // the license is expired. If expiration date is "Permanent License" the license will not
        // be expired. Otherwise, it will perform a date comparison vs now and return the value
        // based on that.
        //
        // ditto for isValid. This is calculated based on the presence of validation errors.

        if (licenseSummary.hasCountedEntity()) {
            retVal.setCountedEntity(CountedEntity.valueOf(licenseSummary.getCountedEntity()));
        }
        if (licenseSummary.hasNumLicensedEntities()) {
            retVal.setNumLicensedEntities(licenseSummary.getNumLicensedEntities());
        }
        if (licenseSummary.hasNumInUseEntities()) {
            retVal.setNumInUseEntities(licenseSummary.getNumInUseEntities());
        }
        // isOverEntityLimit has no equivalent, so this won't be added to the return LicenseApiDTO

        if (licenseSummary.getFeatureCount() > 0) {
            retVal.addFeatures(licenseSummary.getFeatureList());
        }

        if (licenseSummary.getErrorReasonCount() > 0) {
            Set<ErrorReason> reasons = licenseSummary.getErrorReasonList().stream()
                    .map(ErrorReason::valueOf)
                    .collect(Collectors.toSet());
            retVal.setErrorReasons(reasons);
        }

        return retVal;
    }
}
