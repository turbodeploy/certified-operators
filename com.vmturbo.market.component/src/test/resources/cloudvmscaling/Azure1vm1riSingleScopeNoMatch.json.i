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
                "100001"
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
            "businessAccountId": "11",
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
        }
    ],
    "templates": [
        {
            "oid": "100001",
            "name": "Standard_A1",
            "family": "",
            "commitmentAmount": {
                "coupons": 1
            },
            "onDemandCosts": {
                "10": {
                    "WINDOWS": {
                        "compute": 1.0,
                        "license": 0.0
                    }
                },
                "11": {
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
                },
                "11": {
                    "WINDOWS": {
                        "compute": 0.0,
                        "license": 0.5
                    }
                }
            }
        }
    ]
}
