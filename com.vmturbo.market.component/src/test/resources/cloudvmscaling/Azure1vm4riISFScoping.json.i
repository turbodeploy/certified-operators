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
            "currentRICoverage": {"coupons": 0.0},
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100002",
            "providersOid": [
                "100002",
                "100003"
            ],
            "currentRIOID": "0",
            "regionId":"31",
            "operatingSystemLicenseModel":"LICENSE_INCLUDED",
            "accountPricingDataOid":"10"

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
            "commitmentAmount": {
                "coupons": 8
            },
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "-1",
            "riCoveragePerGroup": {},
            "templateOid": "100003"
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
            "isf": true,
            "shared": false,
            "platformFlexible": false,
            "commitmentAmount": {
                "coupons": 8
            },
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "-1",
            "riCoveragePerGroup": {},
            "templateOid": "100003"
        },
        {
            "oid": "1000003",
            "riKeyOid": "3000002",
            "name": "RI2",
            "businessAccountId": "10",
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": true,
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
                "10": {
                    "WINDOWS": {
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
                    }
                }
            }
        },
        {
            "oid": "100002",
            "name": "Standard_D2",
            "family": "D-series",
            "commitmentAmount": {
                "coupons": 2
            },
            "onDemandCosts": {
                "10": {
                    "WINDOWS": {
                        "compute": 2.0,
                        "license": 0.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "WINDOWS": {
                        "compute": 0.0,
                        "license": 1.0
                    }
                }
            }
        },
        {
            "oid": "100003",
            "name": "Standard_D4",
            "family": "D-series",
            "commitmentAmount": {
                "coupons": 8
            },
            "onDemandCosts": {
                "10": {
                    "WINDOWS": {
                        "compute": 8.0,
                        "license": 0.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "WINDOWS": {
                        "compute": 0.0,
                        "license": 2.0
                    }
                }
            }
        }
    ]
}
