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
            "groupName": "",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": 0.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100003",
            "providersOid": [
                "100001",
                "100002"
            ],
            "currentRIOID": "0"
        }
    ],
    "reservedInstances": [
        {
            "oid": "1000002",
            "riKeyOid": "3000002",
            "name": "RI2",
            "businessAccountId": "10",
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": false,
            "shared": true,
            "platformFlexible": false,
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "21",
            "riCoveragePerGroup": {},
            "templateOid": "100002"
        },
        {
            "oid": "1000001",
            "riKeyOid": "3000001",
            "name": "RI1",
            "businessAccountId": "10",
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": false,
            "shared": true,
            "platformFlexible": false,
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "21",
            "riCoveragePerGroup": {},
            "templateOid": "100001"
        }
    ],
    "templates": [
        {
            "oid": "100001",
            "name": "t3.small",
            "family": "t3",
            "coupons": 4,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 4.0,
                        "license": 0.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 0.0,
                        "license": 0.0
                    }
                }
            }
        },
        {
            "oid": "100002",
            "name": "t3a.small",
            "family": "t3a",
            "coupons": 4,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 4.1,
                        "license": 0.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 0.0,
                        "license": 0.0
                    }
                }
            }
        },
        {
            "oid": "100003",
            "name": "t2.small",
            "family": "t2",
            "coupons": 4,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 4.1,
                        "license": 0.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 0.0,
                        "license": 0.0
                    }
                }
            }
        }
    ]
}
