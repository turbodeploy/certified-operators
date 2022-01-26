-- Changes default setting value from PRESERVE TO PRESERVE_SOCKETS
UPDATE setting_policy_setting
SET setting_value = 'PRESERVE_SOCKETS'
WHERE setting_name = 'vcpuScaling_coresPerSocket_SocketMode'
AND setting_value = 'PRESERVE';

-- Changes default setting value from PRESERVE TO PRESERVE_CORES_PER_SOCKET
UPDATE setting_policy_setting
SET setting_value = 'PRESERVE_CORES_PER_SOCKET'
WHERE setting_name = 'vcpuScaling_sockets_coresPerSocketMode'
AND setting_value = 'PRESERVE';