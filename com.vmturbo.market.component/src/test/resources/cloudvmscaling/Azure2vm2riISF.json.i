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
            "businessAccountId": "12",
            "osType": "WINDOWS",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": 0.0,
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
            "accountPricingDataOid":"10"

        },
        {
            "oid": "2000002",
            "name": "VM2",
            "groupName": "",
            "businessAccountId": "12",
            "osType": "WINDOWS",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": 0.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100002",
            "providersOid": [
                "100002"
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
            "name": "RI2",
            "businessAccountId": "12",
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": false,
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
            "businessAccountId": "12",
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": true,
            "shared": true,
            "platformFlexible": false,
            "couponToBestVM": {},
            "discountableVMs": [],
            "skippedVMsWIthIndex": [],
            "zoneId": "-1"
            "riCoveragePerGroup": {},
            "templateOid": "100001"
        }
    ],
    "templates": [
        {
            "oid": "100001",
            "name": "Standard_D1",
            "family": "D-series",
            "coupons": 1,
            "onDemandCosts": {
                "12": {
                    "WINDOWS": {
                        "compute": 1.0,
                        "license": 0.0
                    }
                }
            },
            "discountedCosts": {
                "12": {
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
            "coupons": 2,
            "onDemandCosts": {
                "12": {
                    "WINDOWS": {
                        "compute": 2.0,
                        "license": 0.0
                    }
                }
            },
            "discountedCosts": {
                "12": {
                    "WINDOWS": {
                        "compute": 0.0,
                        "license": 1.0
                    }
                }
            }
        }
    ]
}
