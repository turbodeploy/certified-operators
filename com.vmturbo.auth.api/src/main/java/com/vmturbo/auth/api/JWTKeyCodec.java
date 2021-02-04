package com.vmturbo.auth.api;

import java.math.BigInteger;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Security;
import java.security.spec.InvalidKeySpecException;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;

import io.jsonwebtoken.impl.Base64Codec;

import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPrivateKey;
import org.bouncycastle.jcajce.provider.asymmetric.ec.BCECPublicKey;
import org.bouncycastle.jce.ECNamedCurveTable;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.spec.ECNamedCurveSpec;
import org.bouncycastle.jce.spec.ECParameterSpec;
import org.bouncycastle.jce.spec.ECPrivateKeySpec;
import org.bouncycastle.jce.spec.ECPublicKeySpec;

/**
 * The JWTKeyCodec implements encoding/decoding the EC key pair used for JWT signatures.
 */
public class JWTKeyCodec {

    // Add the Legion of Bouncy Castle Provider.
    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    /**
     * This class must not be instantiated.
     */
    private JWTKeyCodec() {
    }

    /**
     * Encodes the public key.
     *
     * @param publicKey the public key to encode
     * @return The encoded public key
     */
    public static @Nonnull String encodePublicKey(final PublicKey publicKey) {
        // Encode the public key.
        StringBuilder sb = new StringBuilder();
        Base64Codec codec = new Base64Codec();

        BCECPublicKey pubKey = (BCECPublicKey)publicKey;
        sb.append(codec.encode(pubKey.getQ().getEncoded(true)));

        // EC Parameters Spec.
        ECNamedCurveSpec ecParamSpec = (ECNamedCurveSpec)pubKey.getParams();
        String curveName = ecParamSpec.getName();
        sb.append('|');
        sb.append(curveName);

        return sb.toString();
    }

    /**
     * Decodes the public key.
     *
     * @param publicKeyString The String representation of the encoded public key.
     * @return The decoded public key.
     */
    public static @Nonnull PublicKey decodePublicKey(final @Nonnull String publicKeyString) {
        try {
            Base64Codec codec = new Base64Codec();
            String[] parts = publicKeyString.split("\\|");
            if (parts.length != 2 || Strings.isNullOrEmpty(parts[0]) || Strings.isNullOrEmpty(parts[1])) {
                throw new IllegalArgumentException("Corrupted encoded key");
            }
            byte[] q = codec.decode(parts[0]);
            ECParameterSpec ecParamSpec = ECNamedCurveTable.getParameterSpec(parts[1]);
            ECPublicKeySpec keySpec =
                    new ECPublicKeySpec(ecParamSpec.getCurve().decodePoint(q), ecParamSpec);
            return KeyFactory.getInstance("ECDSA", "BC").generatePublic(keySpec);
        } catch (NoSuchProviderException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new SecurityException(e);
        }
    }

    /**
     * Encodes the private key.
     *
     * @param privateKey the private key to encode
     * @return The encoded private key
     */
    public static @Nonnull String encodePrivateKey(final PrivateKey privateKey) {
        // Encode the private key.
        StringBuilder sb = new StringBuilder();
        Base64Codec codec = new Base64Codec();

        BCECPrivateKey privKey = (BCECPrivateKey)privateKey;
        sb.append(codec.encode(privKey.getD().toByteArray()));

        // EC Parameters Spec.
        ECNamedCurveSpec ecParamSpec = (ECNamedCurveSpec)privKey.getParams();
        String curveName = ecParamSpec.getName();
        sb.append('|');
        sb.append(curveName);

        return sb.toString();
    }

    /**
     * Decodes the private key.
     *
     * @param privateKeyString The String representation of the encoded private key.
     * @return The decoded private key.
     */
    public static @Nonnull PrivateKey decodePrivateKey(final @Nonnull String privateKeyString) {
        try {
            Base64Codec codec = new Base64Codec();
            String[] parts = privateKeyString.split("\\|");
            if (parts.length != 2 || Strings.isNullOrEmpty(parts[0]) || Strings.isNullOrEmpty(parts[1])) {
                throw new IllegalArgumentException("Corrupted encoded key");
            }
            BigInteger d = new BigInteger(codec.decode(parts[0]));
            ECParameterSpec ecParamSpec = ECNamedCurveTable.getParameterSpec(parts[1]);
            ECPrivateKeySpec keySpec = new ECPrivateKeySpec(d, ecParamSpec);
            return KeyFactory.getInstance("ECDSA", "BC").generatePrivate(keySpec);
        } catch (NoSuchProviderException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new SecurityException(e);
        }
    }

}
