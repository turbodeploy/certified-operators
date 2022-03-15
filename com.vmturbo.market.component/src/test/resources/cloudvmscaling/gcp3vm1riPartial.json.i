{
    "context": {
        "csp": "GCP",
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
            "currentRICoverage": {
                "commoditiesBought": {
                    "commodity": [
                        {
                            "commodityType": "CPU",
                            "capacity": 0.0
                        }
                    ]
                }
            },
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001",
                "100003"
            ],
            "currentRIOID": "0",
            "regionId": "31",
            "operatingSystemLicenseModel": "LICENSE_INCLUDED",
            "accountPricingDataOid": "10"
        },
        {
            "oid": "2000002",
            "name": "VM2",
            "groupName": "",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": {
                "commoditiesBought": {
                    "commodity": [
                        {
                            "commodityType": "CPU",
                            "capacity": 0.0
                        }
                    ]
                }
            },
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100002",
            "providersOid": [
                "100002",
                "100003"
            ],
            "currentRIOID": "0",
            "regionId": "31",
            "operatingSystemLicenseModel": "LICENSE_INCLUDED",
            "accountPricingDataOid": "10"
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
            "commitmentAmount": {
                "commoditiesBought": {
                    "commodity": [
                        {
                            "commodityType": "CPU",
                            "capacity": 2.0
                        },
                        {
                            "commodityType": "MEM",
                            "capacity": 1.0
                        }
                    ]
                }
            },
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
            "commitmentAmount": {
                "commoditiesBought": {
                    "commodity": [
                        {
                            "commodityType": "CPU",
                            "capacity": 1.0
                        },
                        {
                            "commodityType": "MEM",
                            "capacity": 2.0
                        }
                    ]
                }
            }
        },
        {
            "oid": "100002",
            "name": "template2",
            "family": "family1",
            "commitmentAmount": {
                "commoditiesBought": {
                    "commodity": [
                        {
                            "commodityType": "CPU",
                            "capacity": 2.0
                        },
                        {
                            "commodityType": "MEM",
                            "capacity": 1.0
                        }
                    ]
                }
            }
        },
        {
            "oid": "100003",
            "name": "template3",
            "family": "family2",
            "commitmentAmount": {
                "commoditiesBought": {
                    "commodity": [
                        {
                            "commodityType": "CPU",
                            "capacity": 2.0
                        },
                        {
                            "commodityType": "MEM",
                            "capacity": 1.0
                        }
                    ]
                }
            }
        }
    ]
}