package com.vmturbo.api.component.security;

import com.vmturbo.auth.api.Base64CodecUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Map;
import java.util.Optional;

/**
 * A class to map between Cisco Intersight and XL:
 * <ul>
 * <li> barracuda_account -> user name
 * <li> barracuda_roles -> user role -> external user group
 * e.g. Service Administrator -> Administrator -> CWOM_Administrator (group)
 * </ul>
 */
public class IntersightHeaderMapper implements HeaderMapper {

    private static final String RSA_PUBLIC_KEY_PREFIX = "-----BEGIN RSA PUBLIC KEY----- ";
    private static final String RSA_PUBLIC_KEY_SUFFIX = " -----END RSA PUBLIC KEY-----";

    private static final String ECDSASHA256_PUBLIC_KEY_PREFIX = "-----BEGIN PUBLIC KEY-----";
    private static final String ECDSASHA256_PUBLIC_KEY_SUFFIX = "-----END PUBLIC KEY-----";

    private final String userName;
    private final String role;
    private final Map<String, String> intersightToCwomRoleMap;
    private final String jwtTokenTag;
    private final String jwtTokenPublicKeyTag;

    /**
     * Constructor for the mapper.
     *
     * @param intersightToCwomGroupMap map Cisco intersight role to CWOM group.
     * @param xBarracudaAccount        Cisco intersight user account header.
     * @param xBarracudaRoles          Cisco intersight user role header.
     * @param jwtTokenPublicKeyTag     Cisco intersight public key property name (tag)
     * @param jwtTokenTag              Cisco inetsight JWT token property name (tag)
     */
    public IntersightHeaderMapper(@Nonnull Map<String, String> intersightToCwomGroupMap,
                                  @Nonnull String xBarracudaAccount, @Nonnull String xBarracudaRoles,
                                  @Nonnull String jwtTokenPublicKeyTag, @Nonnull String jwtTokenTag) {
        this.userName = xBarracudaAccount;
        this.role = xBarracudaRoles;
        this.intersightToCwomRoleMap = intersightToCwomGroupMap;
        this.jwtTokenPublicKeyTag = jwtTokenPublicKeyTag;
        this.jwtTokenTag = jwtTokenTag;
    }

    /**
     * Get user name.
     *
     * @return user name.
     */
    @Override
    public String getUserName() {
        return userName;
    }

    /**
     * Get user role.
     *
     * @return user role.
     */
    @Override
    public String getRole() {
        return role;
    }

    /**
     * Get internal Auth role by intersight role.
     *
     * @param intersigtRole intersight role.
     * @return CWOM role if exists.
     */
    @Override
    public String getAuthGroup(@Nullable String intersigtRole) {
        return intersightToCwomRoleMap.get(intersigtRole);
    }

    @Override
    public Optional<PublicKey> buildPublicKey(Optional<String> jwtTokenPublicKey) {
        try {
            byte[] bytes = Base64CodecUtils.decode(getRsaPublicKey(jwtTokenPublicKey));
            X509EncodedKeySpec ks = new X509EncodedKeySpec(bytes);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            return Optional.of(kf.generatePublic(ks));
        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<PublicKey> buildPublicKeyLatest(Optional<String> jwtTokenPublicKey) {
        try {
            byte[] bytes = Base64CodecUtils.decode(getECDSASHA256PublicKey(jwtTokenPublicKey));
            X509EncodedKeySpec ks = new X509EncodedKeySpec(bytes);
            KeyFactory kf = KeyFactory.getInstance("EC");
            return Optional.of(kf.generatePublic(ks));
        } catch (InvalidKeySpecException | NoSuchAlgorithmException e) {
            return Optional.empty();
        }
    }

    @Override
    public String getJwtTokenTag() {
        return jwtTokenTag;
    }

    @Override
    public String getJwtTokenPublicKeyTag() {
        return jwtTokenPublicKeyTag;
    }

    private String getRsaPublicKey(Optional<String> rsaPublicKeyString) {
        return rsaPublicKeyString.map(s -> s.replaceAll(RSA_PUBLIC_KEY_PREFIX, ""))
                .map(s -> s.replaceAll(RSA_PUBLIC_KEY_SUFFIX, ""))
                .orElse("");
    }

    private String getECDSASHA256PublicKey(Optional<String> rsaPublicKeyString) {
        return rsaPublicKeyString.map(s -> s.replaceAll(ECDSASHA256_PUBLIC_KEY_PREFIX, ""))
                .map(s -> s.replaceAll(ECDSASHA256_PUBLIC_KEY_SUFFIX, ""))
                .map(s -> s.replaceAll("\\s+", ""))
                .orElse("");
    }
}
