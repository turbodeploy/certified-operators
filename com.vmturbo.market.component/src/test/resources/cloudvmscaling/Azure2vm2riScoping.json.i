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
            "businessAccountId": "11",
            "osType": "WINDOWS",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": {"coupons": 0.0},
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001"
            ],
            "currentRIOID": "0",
            "regionId":"31",
            "operatingSystemLicenseModel":"LICENSE_INCLUDED",
            "accountPricingDataOid":"11"

        },
        {
            "oid": "2000002",
            "name": "VM2",
            "groupName": "",
            "businessAccountId": "12",
            "osType": "WINDOWS",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": {"coupons": 0.0},
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001"
            ],
            "currentRIOID": "0",
            "regionId":"31",
            "operatingSystemLicenseModel":"LICENSE_INCLUDED",
            "accountPricingDataOid":"12"
        }
    ],
    "reservedInstances": [
        {
            "oid": "1000001",
            "riKeyOid": "3000002",
            "name": "RI2",
            "businessAccountId": "12",
            "applicableBusinessAccounts": [
                "12"
            ],
            "count": 1.0,
            "isf": false,
            "shared": false,
            "platformFlexible": false,
            "commitmentAmount": {
                "coupons": 1
            },
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "-1",
            "riCoveragePerGroup": {},
            "templateOid": "100001"
        },
        {
            "oid": "1000002",
            "riKeyOid": "3000003",
            "name": "RI2",
            "businessAccountId": "12",
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": false,
            "shared": true,
            "platformFlexible": false,
            "commitmentAmount": {
                "coupons": 1
            },
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "-1",
            "riCoveragePerGroup": {},
            "templateOid": "100001"
        },
        {
            "oid": "1000003",
            "riKeyOid": "3000001",
            "name": "RI2",
            "businessAccountId": "11",
            "applicableBusinessAccounts": [
                "11"
            ],
            "count": 1.0,
            "isf": false,
            "shared": false,
            "platformFlexible": false,
            "commitmentAmount": {
                "coupons": 1
            },
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "-1",
            "riCoveragePerGroup": {},
            "templateOid": "100001"
        },
        {
            "oid": "1000004",
            "riKeyOid": "3000003",
            "name": "RI2",
            "businessAccountId": "11",
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": false,
            "shared": true,
            "platformFlexible": false,
            "commitmentAmount": {
                "coupons": 1
            },
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "-1",
            "riCoveragePerGroup": {},
            "templateOid": "100001"
        }
    ],
    "templates": [
        {
            "oid": "100001",
            "name": "Standard_D1",
            "family": "D-series",
            "commitmentAmount": {
                "coupons": 1
            },
            "onDemandCosts": {
                "11": {
                    "WINDOWS": {
                        "compute": 1.0,
                        "license": 0.0
                    }
                },
                "12": {
                    "WINDOWS": {
                        "compute": 1.0,
                        "license": 0.0
                    }
                },
                "13": {
                    "WINDOWS": {
                        "compute": 1.0,
                        "license": 0.0
                    }
                }
            },
            "discountedCosts": {
                "11": {
                    "WINDOWS": {
                        "compute": 0.0,
                        "license": 0.5
                    }
                },
                "12": {
                    "WINDOWS": {
                        "compute": 0.0,
                        "license": 0.5
                    }
                },
                "13": {
                    "WINDOWS": {
                        "compute": 0.0,
                        "license": 0.5
                    }
                }
            }
        }
    ]
}
