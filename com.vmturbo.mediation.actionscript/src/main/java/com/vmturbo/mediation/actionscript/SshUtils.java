package com.vmturbo.mediation.actionscript;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Collection;

import org.apache.sshd.common.config.keys.FilePasswordProvider;
import org.apache.sshd.common.config.keys.loader.openssh.OpenSSHRSAPrivateKeyDecoder;
import org.apache.sshd.common.config.keys.loader.pem.RSAPEMResourceKeyPairParser;
import org.apache.sshd.common.keyprovider.KeyPairProvider;
import org.apache.sshd.common.util.security.SecurityUtils;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;

/**
 * Utilities for creating SSH and SFTP connections.
 **/
public class SshUtils {

    private static final OpenSSHRSAPrivateKeyDecoder privateKeyDecoder =
            OpenSSHRSAPrivateKeyDecoder.INSTANCE;
    private static final Decoder decoder = Base64.getDecoder();


    public static KeyPair extractKeyPair(String privateKeyString)
            throws GeneralSecurityException, IOException {

        InputStream privateKeyStream = new ByteArrayInputStream(privateKeyString.getBytes());

        return SecurityUtils.loadKeyPairIdentity(null, privateKeyStream, null);
    }

}
