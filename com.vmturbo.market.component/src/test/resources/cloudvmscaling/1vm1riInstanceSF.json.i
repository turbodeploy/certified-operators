{
    "context": {
        "csp": "AWS",
        "osType": "LINUX",
        "regionId": "21",
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
            "currentRICoverage": {"coupons": 0.0},
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100003",
            "providersOid": [
                "100001",
                "100002",
                "100003"
            ],
            "currentRIOID": "0",
            "regionId":"21",
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
            "applicableBusinessAccounts": [],
            "count": 4.0,
            "isf": true,
            "shared": true,
            "platformFlexible": false,
            "commitmentAmount": {
                "coupons": 4.0
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
            "name": "t3.nano",
            "family": "t3",
            "commitmentAmount": {
                "coupons": 1
            },
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 1.0,
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
            "name": "t3.micro",
            "family": "t3",
            "commitmentAmount": {
                "coupons": 2
            },
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 2.0,
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
            "name": "t3.small",
            "family": "t3",
            "commitmentAmount": {
                "coupons": 4
            },
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
            "oid": "100004",
            "name": "t3a.small",
            "family": "t3a",
            "commitmentAmount": {
                "coupons": 4
            },
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
