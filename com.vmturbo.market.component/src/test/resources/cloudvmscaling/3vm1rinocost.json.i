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
            "currentRICoverage": 3.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001",
                "100002",
                "100003"
            ],
            "currentRIOID": "1000001"
        },
        {
            "oid": "2000002",
            "name": "VM2",
            "groupName": "",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": 1.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001",
                "100002",
                "100003"
            ],
            "currentRIOID": "1000001"
        },
        {
            "oid": "2000003",
            "name": "VM3",
            "groupName": "",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": 0.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001",
                "100002",
                "100003"
            ],
            "currentRIOID": "0"
        },
        {
            "oid": "2000004",
            "name": "VM4",
            "groupName": "",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": 0.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100002",
                "100003"
            ],
            "currentRIOID": "0"
        },
        {
            "oid": "2000005",
            "name": "VM5",
            "groupName": "",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": 0.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100004",
            "providersOid": [
                "100001",
                "100005"
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
            "applicableBusinessAccounts": [],
            "count": 1.0,
            "isf": true,
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
            "name": "template1",
            "family": "family1",
            "coupons": 4.0,
            "onDemandCosts": {},
            "discountedCosts": {}
        },
        {
            "oid": "100002",
            "name": "template2",
            "family": "family1",
            "coupons": 8,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 10.0,
                        "license": 20.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 1.0,
                        "license": 2.0
                    }
                }
            }
        },
        {
            "oid": "100003",
            "name": "template3",
            "family": "family1",
            "coupons": 16,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 20.0,
                        "license": 40.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 2.0,
                        "license": 4.0
                    }
                }
            }
        },
        {
            "oid": "100004",
            "name": "template4",
            "family": "family1",
            "coupons": 16.0,
            "onDemandCosts": {},
            "discountedCosts": {}
        },
        {
            "oid": "100005",
            "name": "template5",
            "family": "family1",
            "coupons": 16.0,
            "onDemandCosts": {},
            "discountedCosts": {}
        }
    ]
}
