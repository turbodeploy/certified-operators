package com.vmturbo.components.common.mail;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Test sending emails with the MailManager class with different SMTP settings.
 *
 * Note that the test cases will send out emails. The Test annotations are commented out to avoid
 * sending emails on every test run.
 *
 * Uncomment the Test annotations to run the tests manually.
 */
public class MailManagerTest {

    private SettingServiceMole settingServiceMole = spy(new SettingServiceMole());

    private final List<String> specNames = Arrays.asList(
            MailConfiguration.SETTING_SMTP_SERVER,
            MailConfiguration.SETTING_SMTP_PORT,
            MailConfiguration.SETTING_FROM_ADDRESS,
            MailConfiguration.SETTING_USERNAME,
            MailConfiguration.SETTING_PASSWORD,
            MailConfiguration.SETTING_ENCRYPTION
    );

    @Rule
    public GrpcTestServer settingsServer = GrpcTestServer.newServer(settingServiceMole);
    private SettingServiceGrpc.SettingServiceBlockingStub settingsService;

    @Before
    public void setup() throws IOException {
        settingsService =
            SettingServiceGrpc.newBlockingStub(settingsServer.getChannel());
    }

    /**
     * Test skipping sending email.
     *
     * @throws Exception
     */
    @Test(expected = MailEmptyConfigException.class)
    public void testSendMailEmptyConfig() throws Exception {
        MailManager mailManager = new MailManager(settingsService);

        when(settingServiceMole.getMultipleGlobalSettings(eq(GetMultipleGlobalSettingsRequest.newBuilder()
            .addAllSettingSpecName(specNames)
            .build())))
            .thenReturn(getEmptySettingList());

        mailManager.sendMail(Arrays.asList(""), "", "");
    }

    /**
     * Test sending an email without authentication using a relay SMTP server.
     *
     * @throws Exception
     */
    //@Test
    public void testSendMailNoAuth() throws Exception {
        MailManager mailManager = new MailManager(settingsService);

        when(settingServiceMole.getMultipleGlobalSettings(eq(GetMultipleGlobalSettingsRequest.newBuilder()
                .addAllSettingSpecName(specNames)
                .build())))
                .thenReturn(getSettingList());

        mailManager.sendMail(Arrays.asList("test@turbonomic.com"), "Test subject No Auth", "Test body");
    }

    /**
     * Test sending an email with an attachment with the SSL encryption setting.
     *
     * @throws Exception
     */
    //@Test
    public void testSendMailSSL() throws Exception {
        MailManager mailManager = new MailManager(settingsService);

        when(settingServiceMole.getMultipleGlobalSettings(eq(GetMultipleGlobalSettingsRequest.newBuilder()
                .addAllSettingSpecName(specNames)
                .build())))
                .thenReturn(getSettingListGoogleSSL());

        mailManager.sendMail(Arrays.asList("testeamil@turbonomic.com"),
                "Test SSL",
                "Test body",
                Arrays.asList(getClass().getClassLoader()
                        .getResource("emailAttachment.txt").getFile()));
    }

    /**
     * Test sending an email with an attachment with the TLS encryption setting.
     *
     * @throws Exception
     */
    //@Test
    public void testSendMailTLS() throws Exception {
        MailManager mailManager = new MailManager(settingsService);
        when(settingServiceMole.getMultipleGlobalSettings(eq(GetMultipleGlobalSettingsRequest.newBuilder()
                .addAllSettingSpecName(specNames)
                .build())))
                .thenReturn(getSettingListGoogleTLS());

        mailManager.sendMail(Arrays.asList("testemail@turbonomic.com"),
                "Test TLS",
                "Test body",
                Arrays.asList(getClass().getClassLoader()
                        .getResource("emailAttachment.txt").getFile()));
    }

    /**
     * Return empty smtpServer and fromAddress settings.
     *
     * @return settings for skipping sending email.
     */
    private List<Setting> getEmptySettingList() {
        List<Setting> settings = new ArrayList<>();
        settings.add(Setting.newBuilder()
            .setSettingSpecName(MailConfiguration.SETTING_SMTP_SERVER)
            .setStringSettingValue(StringSettingValue.newBuilder()
                .setValue(""))
            .build());
        settings.add(Setting.newBuilder()
            .setSettingSpecName(MailConfiguration.SETTING_SMTP_PORT)
            .setNumericSettingValue(NumericSettingValue.newBuilder()
                .setValue(25))
            .build());
        settings.add(Setting.newBuilder()
            .setSettingSpecName(MailConfiguration.SETTING_FROM_ADDRESS)
            .setStringSettingValue(StringSettingValue.newBuilder()
                .setValue(""))
            .build());
        return settings;
    }

    /**
     * Return settings for our internal SMTP server. Authentication is not used.
     *
     * @return settings for sending email without authentication
     */
    private List<Setting> getSettingList() {
        List<Setting> settings = new ArrayList<>();
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_SMTP_SERVER)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("qasmtp01.corp.vmturbo.com"))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_SMTP_PORT)
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                        .setValue(25))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_FROM_ADDRESS)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("test@turbonomic.com"))
                .build());
        return settings;
    }

    /**
     * Return settings for sending email via the Gmail SMTP server. Use SSL encryption.
     *
     * @return settings for sending email via the Gmail SMTP server using SSL encryption
     */
    private List<Setting> getSettingListGoogleSSL() {
        List<Setting> settings = new ArrayList<>();
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_SMTP_SERVER)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("smtp.gmail.com"))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_SMTP_PORT)
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                        .setValue(465))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_FROM_ADDRESS)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("myTurboTest@gmail.com"))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_USERNAME)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("myTurboTest@gmail.com"))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_PASSWORD)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("turbonomicTest"))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_ENCRYPTION)
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue("SSL"))
                .build());
        return settings;
    }

    /**
     * Return settings for sending email via the Gmail SMTP server. Use TLS encryption.
     *
     * @return settings for sending email via the Gmail SMTP server with TLS encryption.
     */
    private List<Setting> getSettingListGoogleTLS() {
        List<Setting> settings = new ArrayList<>();
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_SMTP_SERVER)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("smtp.gmail.com"))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_SMTP_PORT)
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                        .setValue(587))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_FROM_ADDRESS)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("myTurboTest@gmail.com"))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_USERNAME)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("myTurboTest@gmail.com"))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_PASSWORD)
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("turbonomicTest"))
                .build());
        settings.add(Setting.newBuilder()
                .setSettingSpecName(MailConfiguration.SETTING_ENCRYPTION)
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue("TLS"))
                .build());
        return settings;
    }
}
