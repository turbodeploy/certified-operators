package com.vmturbo.auth.component.licensing;

import java.security.KeyPair;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Date;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.impl.crypto.EllipticCurveProvider;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO.ExternalLicense.Type;
import com.vmturbo.licensing.License;
import com.vmturbo.licensing.utils.LicenseUtil;

/**
 * Utility methods for testing turbo licenses
 */
public class LicenseTestUtils {

    /**
     * Utility function for creating a license.
     * @return
     */
    protected static ILicense createLicense(Date expirationDate, String email, Collection<String> features, int workloadCount) {
        License newLicense = new License();
        newLicense.setEdition("Test");
        newLicense.setEmail(email);
        newLicense.setExpirationDate(DateTimeUtil.formatDate(expirationDate));
        newLicense.addFeatures(features);
        newLicense.setCountedEntity(CountedEntity.VM);
        newLicense.setNumLicensedEntities(workloadCount);
        newLicense.setLicenseKey(LicenseUtil.generateLicenseKey(newLicense));
        return newLicense;
    }

    protected static ILicense createInvalidLicense(Date expirationDate, String email, Collection<String> features, int workloadCount) {
        License newLicense = new License();
        newLicense.setEdition("Test");
        newLicense.setEmail(email);
        newLicense.setExpirationDate(DateTimeUtil.formatDate(expirationDate));
        newLicense.addFeatures(features);
        newLicense.setCountedEntity(CountedEntity.VM);
        newLicense.setNumLicensedEntities(workloadCount);
        newLicense.setLicenseKey("BWAHAHAHAHA");
        return newLicense;
    }

    protected static LicenseDTO createExternalLicense(@Nonnull Type type, @Nonnull String payload,
            @Nullable LocalDateTime expirationDate) {
        final ExternalLicense.Builder externalLicenseBuilder = ExternalLicense.newBuilder();
        if (expirationDate != null) {
            externalLicenseBuilder.setExpirationDate(expirationDate.toString());
        }
        return LicenseDTO.newBuilder()
                .setExternal(externalLicenseBuilder.setType(type).setPayload(payload).build())
                .build();
    }

    protected static String createGrafanaJwtToken(@Nullable Integer grafanaReportEditorsCount) {
        final KeyPair keyPair = EllipticCurveProvider.generateKeyPair(SignatureAlgorithm.ES256);
        final JwtBuilder jwtBuilder = Jwts.builder()
                .setSubject("subject")
                .signWith(SignatureAlgorithm.ES256, keyPair.getPrivate());
        if (grafanaReportEditorsCount != null) {
            jwtBuilder.claim(LicenseSummaryCreator.GRAFANA_ADMINS_CLAIM_NAME,
                    grafanaReportEditorsCount);
        }
        return jwtBuilder.compact();
    }

}
