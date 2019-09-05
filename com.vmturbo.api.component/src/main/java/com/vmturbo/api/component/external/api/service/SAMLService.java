package com.vmturbo.api.component.external.api.service;

import javax.annotation.Nonnull;

import org.springframework.web.client.RestClientException;

import com.vmturbo.api.dto.user.SAMLConfigurationApiDTO;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.serviceinterfaces.ISAMLService;
import com.vmturbo.auth.api.Base64CodecUtils;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;

/**
 * Implementation for the external API service related to SAML configuration.
 *
 * <p>TODO: Update ISAMLService methods to throw checked exception, and then change
 * RuntimeException thrown here to that exception.
 * */
public class SAMLService implements ISAMLService {

    private static final String SAML_KEYSTORE = "samlKeystore";
    private static final String SAML_IDP_METADATA = "samlIdpMetadata";
    private static final String SAML_EXTERNAL_IP = "samlExternalIP";
    private static final String SAML_ENTITY_ID = "samlEntityId";
    private static final String SAML_ENABLED = "samlEnabled";
    private static final String SAML_KEYSTORE_PASSWORD = "samlKeystorePassword";
    private static final String SAML_PRIVATE_KEY_ALIAS = "samlPrivateKeyAlias";

    private final String apiComponentType;
    private final ClusterMgrRestClient clusterMgrRestClient;

    /**
     * Create an instance of the SAMLService.
     *
     * @param apiComponentType the component-type for the API component, where SAML is implemented
     * @param clusterMgrRestClient call this client to set current property values for the
     */
    public SAMLService(final String apiComponentType,
                       final ClusterMgrRestClient clusterMgrRestClient) {
        this.apiComponentType = apiComponentType;
        this.clusterMgrRestClient = clusterMgrRestClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public SAMLConfigurationApiDTO createSamlConfiguration(
        @Nonnull final SAMLConfigurationApiDTO samlConfigurationApiDTO) {
        try {
            setApiComponentProperty(SAML_ENABLED,  Boolean.toString(samlConfigurationApiDTO.isEnabled()));
            setApiComponentProperty(SAML_ENTITY_ID, samlConfigurationApiDTO.getEntityId());
            setApiComponentProperty(SAML_EXTERNAL_IP, samlConfigurationApiDTO.getExternalIP());
            if (samlConfigurationApiDTO.getPassword() != null) {
                setApiComponentProperty(SAML_KEYSTORE_PASSWORD, samlConfigurationApiDTO.getPassword());
            }
            if (samlConfigurationApiDTO.getAlias() != null) {
                setApiComponentProperty(SAML_PRIVATE_KEY_ALIAS, samlConfigurationApiDTO.getAlias());
            }
            return samlConfigurationApiDTO;
        } catch (OperationFailedException e) {
            throw new RuntimeException("Error creating SAML configuration", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateKeystore(final byte[] keyStore) {
        try {
            setApiComponentProperty(SAML_KEYSTORE, Base64CodecUtils.encode(keyStore));
        } catch (OperationFailedException e) {
            throw new RuntimeException("Error updating keystore", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateIdpMetadata(final byte[] idpMetada) {
        try {
            setApiComponentProperty(SAML_IDP_METADATA, Base64CodecUtils.encode(idpMetada));
        } catch (OperationFailedException e) {
            throw new RuntimeException("Error updating IdpMetadata", e);
        }
    }

    private void setApiComponentProperty(final String propertyName, final String propertyValue)
        throws OperationFailedException {
        try {
            clusterMgrRestClient.setComponentLocalProperty(apiComponentType, propertyName,
                propertyValue);
        } catch (RestClientException e) {
            // don't include the property value in the error since it may contain sensitive info
            throw new OperationFailedException("Error setting API Component property "
                + propertyName, e);
        }
    }
}
