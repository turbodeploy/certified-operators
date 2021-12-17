-- In the OM-60946 we changed the code so empty settings are not saved in DB rather than keeping
-- them as as empty string. However, we didn't delete the empty settings
DELETE FROM setting_policy_setting WHERE setting_value = '';