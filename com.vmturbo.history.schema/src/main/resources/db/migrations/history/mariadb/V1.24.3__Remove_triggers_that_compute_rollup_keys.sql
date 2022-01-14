/**
 * Formerly, rollup key columns (hour_key, day_key and month_key) in entity stats _latest tables
 * were computed by insert triggers. We now compute them in Java code in history component, which
 * results in a significant performance improvement.
 *
 * So here we remove those triggers.
 */
DROP TRIGGER IF EXISTS set_app_primary_keys;
DROP TRIGGER IF EXISTS set_bu_primary_keys;
DROP TRIGGER IF EXISTS set_ch_primary_keys;
DROP TRIGGER IF EXISTS set_cnt_primary_keys;
DROP TRIGGER IF EXISTS set_cpod_primary_keys;
DROP TRIGGER IF EXISTS set_da_primary_keys;
DROP TRIGGER IF EXISTS set_dpod_primary_keys;
DROP TRIGGER IF EXISTS set_ds_primary_keys;
DROP TRIGGER IF EXISTS set_iom_primary_keys;
DROP TRIGGER IF EXISTS set_lp_primary_keys;
DROP TRIGGER IF EXISTS set_pm_primary_keys;
DROP TRIGGER IF EXISTS set_sc_primary_keys;
DROP TRIGGER IF EXISTS set_sw_primary_keys;
DROP TRIGGER IF EXISTS set_vdc_primary_keys;
DROP TRIGGER IF EXISTS set_view_pod_primary_keys;
DROP TRIGGER IF EXISTS set_vm_primary_keys;
DROP TRIGGER IF EXISTS set_vpod_primary_keys;
DROP TRIGGER IF EXISTS set_bu_primary_keys;
DROP TRIGGER IF EXISTS set_view_pod_primary_keys;
