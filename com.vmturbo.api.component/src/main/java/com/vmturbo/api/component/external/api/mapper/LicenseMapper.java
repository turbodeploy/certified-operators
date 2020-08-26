package com.vmturbo.api.component.external.api.mapper;

import java.util.Set;
import java.util.stream.Collectors;

import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.dto.license.ILicense.ErrorReason;
import com.vmturbo.api.dto.license.LicenseApiDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense.Type;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.TurboLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;

/**
 * Conversions between {@link LicenseApiDTO}, {@link LicenseDTO} and {@link LicenseSummary} objects.
 */
public class LicenseMapper {
    private static final String GRAFANA_LICENSE_DISPLAY_NAME = "Reporting License";
    private static final String UNKNOWN_LICENSE_DISPLAY_NAME = "Unknown External License";

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
        if (licenseDTO.hasFilename()) {
            license.setFilename(licenseDTO.getFilename());
        }

        if (licenseDTO.hasTurbo()) {
            TurboLicense turboLicense = licenseDTO.getTurbo();
            if (turboLicense.hasEmail()) {
                license.setEmail(turboLicense.getEmail());
            }
            if (turboLicense.hasEdition()) {
                license.setEdition(turboLicense.getEdition());
            }
            if (turboLicense.hasExpirationDate()) {
                license.setExpirationDate(turboLicense.getExpirationDate());
            }
            if (turboLicense.hasLicenseOwner()) {
                license.setLicenseOwner(turboLicense.getLicenseOwner());
            }
            if (turboLicense.hasLicenseKey()) {
                license.setLicenseKey(turboLicense.getLicenseKey());
            }
            if (turboLicense.hasExternalLicenseKey()) {
                license.setExternalLicenseKey(turboLicense.getExternalLicenseKey());
            }
            if (turboLicense.hasCountedEntity()) {
                CountedEntity.valueOfName(turboLicense.getCountedEntity()).ifPresent(license::setCountedEntity);
            }
            if (turboLicense.hasNumLicensedEntities()) {
                license.setNumLicensedEntities(turboLicense.getNumLicensedEntities());
            }
            if (turboLicense.getFeaturesCount() > 0) {
                license.addFeatures(turboLicense.getFeaturesList());
            }

            // we don't need to transcribe isValid() since it's calculated based on error reasons in the
            // shared ILicense objects.
            if (turboLicense.getErrorReasonCount() > 0) {
                license.setErrorReasons(turboLicense.getErrorReasonList()
                        .stream()
                        .map(ErrorReason::valueOf)
                        .collect(Collectors.toSet()));
            }
        } else if (licenseDTO.hasExternal()) {
            ExternalLicense externalLicense = licenseDTO.getExternal();
            if (externalLicense.hasExpirationDate()) {
                license.setExpirationDate(externalLicense.getExpirationDate());
            }

            if (externalLicense.getType() == Type.GRAFANA) {
                license.setEdition(GRAFANA_LICENSE_DISPLAY_NAME);
            } else {
                license.setEdition(UNKNOWN_LICENSE_DISPLAY_NAME);
            }
        }

        return license;
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
