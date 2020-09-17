{
    "context": {
        "billingFamilyId": "1",
        "csp": "AWS",
        "osType": "LINUX",
        "regionId": "31",
        "tenancy": "DEFAULT"
    },
    "reservedInstances": [
        {
            "applicableBusinessAccounts": [],
            "businessAccountId": "10",
            "count": 1.0,
            "couponToBestVM": {},
            "discountableVMs": [],
            "isf": false,
            "name": "RI1",
            "oid": "1000001",
            "platformFlexible": false,
            "riCoveragePerGroup": {},
            "riKeyOid": "3000001",
            "shared": true,
            "skippedVMsWIthIndex": [],
            "templateOid": "100002",
            "zoneId": "21"
        }
    ],
    "templates": [
        {
            "coupons": 3,
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
            },
            "family": "family1",
            "name": "template1",
            "oid": "100001",
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
            }
        },
        {
            "coupons": 3,
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 0.0,
                        "license": 0.0
                    }
                }
            },
            "family": "family1",
            "name": "template2",
            "oid": "100002",
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 120.0,
                        "license": 20.0
                    }
                }
            }
        }
    ],
    "virtualMachines": [
        {
            "businessAccountId": "10",
            "currentRICoverage": 0.0,
            "currentRIOID": "0",
            "currentTemplateOid": "100002",
            "groupName": "",
            "groupProviders": [],
            "groupSize": 1,
            "minCostProviderPerFamily": {},
            "name": "VM1",
            "oid": "2000001",
            "osType": "LINUX",
            "providers": [],
            "providersOid": [
                "100001",
                "100002"
            ],
            "zoneId": "21"
        },
        {
            "businessAccountId": "10",
            "currentRICoverage": 0.0,
            "currentRIOID": "0",
            "currentTemplateOid": "100001",
            "groupName": "",
            "groupProviders": [],
            "groupSize": 1,
            "minCostProviderPerFamily": {},
            "name": "VM2",
            "oid": "2000002",
            "osType": "LINUX",
            "providers": [],
            "providersOid": [
                "100001",
                "100002"
            ],
            "zoneId": "21"
        }
    ]
}

