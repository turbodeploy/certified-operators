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
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001"
            ],
            "currentRIOID": "0"
        }
    ],
    "reservedInstances": [
        {
            "oid": "1000002",
            "riKeyOid": "3000000",
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
            "oid": "1000005",
            "riKeyOid": "3000002",
            "name": "RI5",
            "businessAccountId": "12",
            "applicableBusinessAccounts": [
                "12"
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
            "oid": "1000008",
            "riKeyOid": "3000002",
            "name": "RI8",
            "businessAccountId": "12",
            "applicableBusinessAccounts": [
                "12"
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
            "oid": "1000011",
            "riKeyOid": "3000005",
            "name": "RI8",
            "businessAccountId": "12",
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": true,
            "shared": false,
            "platformFlexible": false,
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
        }
    ]
}
