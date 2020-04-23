package com.vmturbo.api.component.external.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.security.saml2.credentials.Saml2X509Credential.Saml2X509CredentialType.DECRYPTION;
import static org.springframework.security.saml2.credentials.Saml2X509Credential.Saml2X509CredentialType.ENCRYPTION;
import static org.springframework.security.saml2.credentials.Saml2X509Credential.Saml2X509CredentialType.SIGNING;
import static org.springframework.security.saml2.credentials.Saml2X509Credential.Saml2X509CredentialType.VERIFICATION;

import java.io.ByteArrayInputStream;
import java.security.KeyException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import org.opensaml.security.crypto.KeySupport;
import org.springframework.security.saml2.Saml2Exception;
import org.springframework.security.saml2.credentials.Saml2X509Credential;

/**
 * SAML2 credentials Utility.
 */
final class Saml2X509CredentialsUtil {

    private Saml2X509CredentialsUtil() {

    }

    /**
     * Get Relying party default credentials.
     *
     * @return relying party default credentials.
     */
    public static List<Saml2X509Credential> getRelyingPartyCredentials() {
        return Arrays.asList(
                new Saml2X509Credential(spPrivateKey(), spCertificate(), SIGNING, DECRYPTION));
    }

    /**
     * Build X509 SAML 2 credentials.
     *
     * @param x509Certificate certificate strings
     * @return X509 SAML 2 credential
     */
    public static Saml2X509Credential buildSaml2X509Credential(@Nonnull String x509Certificate) {
        return new Saml2X509Credential(certificate(x509Certificate), ENCRYPTION, VERIFICATION);
    }

    private static X509Certificate certificate(String cert) {
        ByteArrayInputStream certBytes = new ByteArrayInputStream(cert.getBytes());
        try {
            return (X509Certificate)CertificateFactory.getInstance("X.509")
                    .generateCertificate(certBytes);
        } catch (CertificateException e) {
            throw new Saml2Exception(e);
        }
    }

    private static PrivateKey privateKey(String key) {
        try {
            return KeySupport.decodePrivateKey(key.getBytes(UTF_8), new char[0]);
        } catch (KeyException e) {
            throw new Saml2Exception(e);
        }
    }

    // default certificate
    private static X509Certificate spCertificate() {

        return certificate("-----BEGIN CERTIFICATE-----\n" +
                "MIICgTCCAeoCCQCuVzyqFgMSyDANBgkqhkiG9w0BAQsFADCBhDELMAkGA1UEBhMC\n" +
                "VVMxEzARBgNVBAgMCldhc2hpbmd0b24xEjAQBgNVBAcMCVZhbmNvdXZlcjEdMBsG\n" +
                "A1UECgwUU3ByaW5nIFNlY3VyaXR5IFNBTUwxCzAJBgNVBAsMAnNwMSAwHgYDVQQD\n" +
                "DBdzcC5zcHJpbmcuc2VjdXJpdHkuc2FtbDAeFw0xODA1MTQxNDMwNDRaFw0yODA1\n" +
                "MTExNDMwNDRaMIGEMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3RvbjES\n" +
                "MBAGA1UEBwwJVmFuY291dmVyMR0wGwYDVQQKDBRTcHJpbmcgU2VjdXJpdHkgU0FN\n" +
                "TDELMAkGA1UECwwCc3AxIDAeBgNVBAMMF3NwLnNwcmluZy5zZWN1cml0eS5zYW1s\n" +
                "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDRu7/EI0BlNzMEBFVAcbx+lLos\n" +
                "vzIWU+01dGTY8gBdhMQNYKZ92lMceo2CuVJ66cUURPym3i7nGGzoSnAxAre+0YIM\n" +
                "+U0razrWtAUE735bkcqELZkOTZLelaoOztmWqRbe5OuEmpewH7cx+kNgcVjdctOG\n" +
                "y3Q6x+I4qakY/9qhBQIDAQABMA0GCSqGSIb3DQEBCwUAA4GBAAeViTvHOyQopWEi\n" +
                "XOfI2Z9eukwrSknDwq/zscR0YxwwqDBMt/QdAODfSwAfnciiYLkmEjlozWRtOeN+\n" +
                "qK7UFgP1bRl5qksrYX5S0z2iGJh0GvonLUt3e20Ssfl5tTEDDnAEUMLfBkyaxEHD\n" +
                "RZ/nbTJ7VTeZOSyRoVn5XHhpuJ0B\n" + "-----END CERTIFICATE-----");
    }

    // default private key
    private static PrivateKey spPrivateKey() {
        return privateKey("-----BEGIN PRIVATE KEY-----\n" +
                "MIICeAIBADANBgkqhkiG9w0BAQEFAASCAmIwggJeAgEAAoGBANG7v8QjQGU3MwQE\n" +
                "VUBxvH6Uuiy/MhZT7TV0ZNjyAF2ExA1gpn3aUxx6jYK5UnrpxRRE/KbeLucYbOhK\n" +
                "cDECt77Rggz5TStrOta0BQTvfluRyoQtmQ5Nkt6Vqg7O2ZapFt7k64Sal7AftzH6\n" +
                "Q2BxWN1y04bLdDrH4jipqRj/2qEFAgMBAAECgYEAj4ExY1jjdN3iEDuOwXuRB+Nn\n" +
                "x7pC4TgntE2huzdKvLJdGvIouTArce8A6JM5NlTBvm69mMepvAHgcsiMH1zGr5J5\n" +
                "wJz23mGOyhM1veON41/DJTVG+cxq4soUZhdYy3bpOuXGMAaJ8QLMbQQoivllNihd\n" +
                "vwH0rNSK8LTYWWPZYIECQQDxct+TFX1VsQ1eo41K0T4fu2rWUaxlvjUGhK6HxTmY\n" +
                "8OMJptunGRJL1CUjIb45Uz7SP8TPz5FwhXWsLfS182kRAkEA3l+Qd9C9gdpUh1uX\n" +
                "oPSNIxn5hFUrSTW1EwP9QH9vhwb5Vr8Jrd5ei678WYDLjUcx648RjkjhU9jSMzIx\n" +
                "EGvYtQJBAMm/i9NR7IVyyNIgZUpz5q4LI21rl1r4gUQuD8vA36zM81i4ROeuCly0\n" +
                "KkfdxR4PUfnKcQCX11YnHjk9uTFj75ECQEFY/gBnxDjzqyF35hAzrYIiMPQVfznt\n" +
                "YX/sDTE2AdVBVGaMj1Cb51bPHnNC6Q5kXKQnj/YrLqRQND09Q7ParX0CQQC5NxZr\n" +
                "9jKqhHj8yQD6PlXTsY4Occ7DH6/IoDenfdEVD5qlet0zmd50HatN2Jiqm5ubN7CM\n" +
                "INrtuLp4YHbgk1mi\n" + "-----END PRIVATE KEY-----");
    }
}
