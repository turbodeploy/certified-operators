package com.vmturbo.api.component.external.api.service;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;

import com.vmturbo.api.dto.license.LicenseApiDTO;
import com.vmturbo.api.dto.license.LicenseApiInputDTO;
import com.vmturbo.api.serviceinterfaces.ILicenseService;

public class LicenseService implements ILicenseService {
    final LocalDate EXPIRATION_DATE = LocalDate.of(2018, 1, 1);

    @Override
    public LicenseApiDTO getLicense() {
        LicenseApiDTO license = new LicenseApiDTO();

        final Date expirationDate = Date.from(EXPIRATION_DATE
            .atStartOfDay(ZoneId.systemDefault())
            .toInstant());

        license.setLicenseOwner("Turbonomic XL");
        license.setEmail("");
        license.setExpirationDate(new SimpleDateFormat("MMM dd yyyy").format(expirationDate));

        license.setFeatures(Arrays.asList(
            "historical_data",
            "multiple_vc",
            "scoped_user_view",
            "customized_views",
            "group_editor",
            "vmturbo_api",
            "automated_actions",
            "active_directory",
            "custom_reports",
            "planner",
            "optimizer",
            "full_policy",
            "deploy",
            "aggregation",
            "action_script",
            "fabric",
            "cloud_targets",
            "cluster_flattening",
            "cluster_flattening",
            "network_control",
            "cluster_flattening",
            "storage",
            "vdi_control",
            "public_cloud",
            "container_control",
            "applications",
            "loadbalancer",
            "app_control"
        ));

        license.setNumSocketsLicensed(Integer.MAX_VALUE);
        license.setNumSocketsInUse(0);
        license.setIsValid(true);
        license.setExpired(LocalDate.now().isAfter(EXPIRATION_DATE));

        return license;
    }

    @Override
    public LicenseApiDTO populateLicense(final LicenseApiInputDTO licenseApiInputDTO) {
        return getLicense();
    }

    @Override
    public LicenseApiDTO validateLicense() {
        return getLicense();
    }
}
