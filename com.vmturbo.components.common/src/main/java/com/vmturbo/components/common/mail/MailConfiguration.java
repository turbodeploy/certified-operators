package com.vmturbo.components.common.mail;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;

/**
 * MailConfiguration encapsulates the settings required for sending emails.
 * It retrieves settings through the settings RPC service, and validates the settings.
 * It also provides getter methods to get the setting values from the object.
 */
public class MailConfiguration {

    private final SettingServiceGrpc.SettingServiceBlockingStub settingService;

    // Setting "Spec" names.  These are the keys for storing settings in the database.
    public static final String SETTING_SMTP_SERVER = "smtpServer";
    public static final String SETTING_SMTP_PORT = "smtpPort";
    public static final String SETTING_FROM_ADDRESS = "fromAddress";
    public static final String SETTING_USERNAME = "smtpUsername";
    public static final String SETTING_PASSWORD = "smtpPassword";
    public static final String SETTING_ENCRYPTION = "smtpEncryption";

    public enum EncryptionType { NONE, SSL, TLS }

    private String smtpServer = null;
    private Integer smtpPort = null;
    private String fromAddress = null;
    private String username = null;
    private String password = null;
    private EncryptionType encryption = EncryptionType.NONE;

    /**
     * Constructor.
     *
     * @param settingService RPC client for the setting service
     * @throws MailConfigException if missing or invalid settings exist
     */
    public MailConfiguration(SettingServiceGrpc.SettingServiceBlockingStub settingService)
            throws MailConfigException {
        this.settingService = settingService;

        getMailConfig();
    }

    /**
     * Retrieve settings using the setting service.
     *
     * @throws MailConfigException if missing or invalid settings exist
     */
    private void getMailConfig() throws MailConfigException {
        List<String> specNames = Arrays.asList(
                SETTING_SMTP_SERVER,
                SETTING_SMTP_PORT,
                SETTING_FROM_ADDRESS,
                SETTING_USERNAME,
                SETTING_PASSWORD,
                SETTING_ENCRYPTION
        );
        Iterator<Setting> settingsIter = settingService.getMultipleGlobalSettings(
                GetMultipleGlobalSettingsRequest.newBuilder()
                        .addAllSettingSpecName(specNames)
                        .build());

        while (settingsIter.hasNext()) {
            Setting setting = settingsIter.next();
            switch (setting.getSettingSpecName()) {
                case SETTING_SMTP_SERVER:
                    smtpServer = setting.getStringSettingValue().getValue();
                    break;
                case SETTING_SMTP_PORT:
                    smtpPort = new Float(setting.getNumericSettingValue().getValue()).intValue();
                    break;
                case SETTING_FROM_ADDRESS:
                    fromAddress = setting.getStringSettingValue().getValue();
                    break;
                case SETTING_USERNAME:
                    username = setting.getStringSettingValue().getValue();
                    break;
                case SETTING_PASSWORD:
                    password = setting.getStringSettingValue().getValue();
                    break;
                case SETTING_ENCRYPTION:
                    encryption = EncryptionType.valueOf(setting.getEnumSettingValue().getValue());
                    break;
            }
        }

        validateEmailConfig();
    }

    /**
     * Validate the settings.
     *
     * @throws MailConfigException if missing or invalid settings exist
     */
    private void validateEmailConfig() throws MailConfigException {
        if (smtpServer == null) {
            throw new MailConfigException("SMTP Server is missing.");
        } else if (smtpPort == null) {
            throw new MailConfigException("SMTP port is missing.");
        } else if (fromAddress == null) {
            throw new MailConfigException("From address is missing.");
        } else if (username != null && username.isEmpty() == false && password == null) {
            throw new MailConfigException("Password is missing.");
        } else if (encryption == null) {
            throw new MailConfigException("Encryption type is missing.");
        } else if (Arrays.asList(EncryptionType.values()).contains(encryption) == false) {
            throw new MailConfigException("Invalid encryption type.");
        }
    }

    /**
     * Gets the SMTP Server.
     *
     * @return SMPT Server
     */
    public String getSmtpServer() {
        return smtpServer;
    }

    /**
     * Gets the SMTP Port.
     *
     * @return SMTP Port
     */
    public Integer getSmtpPort() {
        return smtpPort;
    }

    /**
     * Gets the 'From' address.
     *
     * @return 'From' address
     */
    public String getFromAddress() {
        return fromAddress;
    }

    /**
     * Gets the username to be used for authentication.
     *
     * @return Username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Gets the password to be used for authentication.
     * TODO:  Decrypt password if the password is encrypted when stored.
     *
     * @return
     */
    public String getPassword() {
        return password;
    }

    /**
     * Get the encryption type.
     *
     * @return Encryption type
     */
    public EncryptionType getEncryption() {
        return encryption;
    }
}
