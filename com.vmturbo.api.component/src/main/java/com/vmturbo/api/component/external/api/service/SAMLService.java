package com.vmturbo.api.component.external.api.service;

import com.vmturbo.api.dto.user.SAMLConfigurationApiDTO;
import com.vmturbo.api.serviceinterfaces.ISAMLService;
import com.vmturbo.auth.api.Base64CodecUtils;
import com.vmturbo.kvstore.ISAMLConfigurationStore;

/**
 * {@inheritDoc}
 */
public class SAMLService implements ISAMLService {

    private static final String SAML_KEYSTORE = "samlKeystore";
    private static final String SAML_IDP_METADATA = "samlIdpMetadata";
    private static final String SAML_EXTERNAL_IP = "samlExternalIP";
    private static final String SAML_ENTITY_ID = "samlEntityId";
    private static final String SAML_ENABLED = "samlEnabled";
    public static final String SAML_KEYSTORE_PASSWORD = "samlKeystorePassword";
    public static final String SAML_PRIVATE_KEY_ALIAS = "samlPrivateKeyAlias";
    private final ISAMLConfigurationStore samlConfigurationStore;

    public SAMLService(final ISAMLConfigurationStore samlConfigurationStore) {
        this.samlConfigurationStore = samlConfigurationStore;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SAMLConfigurationApiDTO createSamlConfiguration(final SAMLConfigurationApiDTO samlConfigurationApiDTO) {
        samlConfigurationStore.put(SAML_ENABLED, samlConfigurationApiDTO.isEnabled() ? "true" : "false");
        samlConfigurationStore.put(SAML_ENTITY_ID, samlConfigurationApiDTO.getEntityId());
        samlConfigurationStore.put(SAML_EXTERNAL_IP, samlConfigurationApiDTO.getExternalIP());
        if (samlConfigurationApiDTO.getPassword() != null) {
            samlConfigurationStore.put(SAML_KEYSTORE_PASSWORD, samlConfigurationApiDTO.getPassword());
        }
        if (samlConfigurationApiDTO.getAlias() != null) {
            samlConfigurationStore.put(SAML_PRIVATE_KEY_ALIAS, samlConfigurationApiDTO.getAlias());
        }
        return samlConfigurationApiDTO;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateKeystore(final byte[] keyStore) {
        samlConfigurationStore.put(SAML_KEYSTORE, Base64CodecUtils.encode(keyStore));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateIdpMetadata(final byte[] idpMetada) {
        samlConfigurationStore.put(SAML_IDP_METADATA, Base64CodecUtils.encode(idpMetada));
    }

}
