-- Changes default setting value from PRESERVE TO PRESERVE_SOCKETS
UPDATE setting_policy_setting sps SET sps.setting_value = 'PRESERVE_SOCKETS'
WHERE sps.setting_name = 'vcpuScaling_coresPerSocket_SocketMode'
AND sps.setting_value = 'PRESERVE';

-- Changes default setting value from PRESERVE TO PRESERVE_CORES_PER_SOCKET
UPDATE setting_policy_setting sps SET sps.setting_value = 'PRESERVE_CORES_PER_SOCKET'
WHERE sps.setting_name = 'vcpuScaling_sockets_coresPerSocketMode'
AND sps.setting_value = 'PRESERVE';