{
    "context": {
        "csp": "AZURE",
        "osType": "UNKNOWN_OS",
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
            "osType": "WINDOWS",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": 0.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001",
                "100002"
            ],
            "currentRIOID": "0"
        }
    ],
    "reservedInstances": [
        {
            "oid": "1000001",
            "riKeyOid": "3000001",
            "name": "RI1",
            "businessAccountId": "10",
            "applicableBusinessAccounts": [
                "10"
            ],
            "count": 1.0,
            "isf": false,
            "shared": false,
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
            "applicableBusinessAccounts": [
                "10"
            ],
            "count": 1.0,
            "isf": false,
            "shared": false,
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
            "name": "Standard_A1",
            "family": "",
            "coupons": 1,
            "onDemandCosts": {
                "10": {
                    "WINDOWS": {
                        "compute": 1.0,
                        "license": 0.0
                    },
                    "RHEL": {
                        "compute": 1.0,
                        "license": 0.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "WINDOWS": {
                        "compute": 0.0,
                        "license": 0.5
                    },
                    "RHEL": {
                        "compute": 0.0,
                        "license": 0.2
                    }
                }
            }
        },
        {
            "oid": "100002",
            "name": "Standard_A1_v2",
            "family": "",
            "coupons": 1,
            "onDemandCosts": {
                "10": {
                    "RHEL": {
                        "compute": 1.0,
                        "license": 0.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "RHEL": {
                        "compute": 0.0,
                        "license": 0.2
                    }
                }
            }
        }
    ]
}
