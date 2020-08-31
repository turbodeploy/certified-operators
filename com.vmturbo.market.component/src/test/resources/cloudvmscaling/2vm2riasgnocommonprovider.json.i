{
    "context": {
        "csp": "AWS",
        "osType": "LINUX",
        "regionId": "31",
        "billingFamilyId": "1",
        "tenancy": "DEFAULT"
    },
    "virtualMachines": [
        {
            "oid": "2000001",
            "name": "VM1",
            "groupName": "101",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": 3.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001"
            ],
            "currentRIOID": "1000001"
        },
        {
            "oid": "2000002",
            "name": "VM2",
            "groupName": "101",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": 1.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100002",
            "providersOid": [
                "100002"
            ],
            "currentRIOID": "1000002"
        }
    ],
    "reservedInstances": [
        {
            "oid": "1000001",
            "riKeyOid": "3000001",
            "name": "RI1",
            "businessAccountId": "10",
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": true,
            "shared": true,
            "platformFlexible": false,
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "-1",
            "riCoveragePerGroup": {},
            "templateOid": "100001"
        },
        {
            "oid": "1000002",
            "riKeyOid": "3000002",
            "name": "RI2",
            "businessAccountId": "10",
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": true,
            "shared": true,
            "platformFlexible": false,
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "-1",
            "riCoveragePerGroup": {},
            "templateOid": "100002"
        }
    ],
    "templates": [
        {
            "oid": "100001",
            "name": "template1",
            "family": "family1",
            "coupons": 4,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 100.0,
                        "license": 20.0
                    }
                },
                "11": {
                    "LINUX": {
                        "compute": 100.0,
                        "license": 20.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 0.0,
                        "license": 0.0
                    }
                },
                "11": {
                    "LINUX": {
                        "compute": 0.0,
                        "license": 0.0
                    }
                }
            }
        },
        {
            "oid": "100002",
            "name": "template2",
            "family": "family2",
            "coupons": 4,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 100.0,
                        "license": 20.0
                    }
                },
                "11": {
                    "LINUX": {
                        "compute": 100.0,
                        "license": 20.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 0.0,
                        "license": 0.0
                    }
                },
                "11": {
                    "LINUX": {
                        "compute": 0.0,
                        "license": 0.0
                    }
                }
            }
        }
    ]
}
