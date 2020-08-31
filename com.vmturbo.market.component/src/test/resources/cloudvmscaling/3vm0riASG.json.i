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
            "groupName": "101",
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
                "100003",
                "100004",
                "100006"
            ],
            "currentRIOID": "0"
        },
        {
            "oid": "2000010",
            "name": "VM2B",
            "groupName": "101",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "22",
            "providers": [],
            "currentRICoverage": 0.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001",
                "100003",
                "100004",
                "100005",
                "100006"
            ],
            "currentRIOID": "0"
        },
        {
            "oid": "2000011",
            "name": "VM2A",
            "groupName": "101",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "22",
            "providers": [],
            "currentRICoverage": 0.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001",
                "100003",
                "100004",
                "100005",
                "100006"
            ],
            "currentRIOID": "0"
        }
    ],
    "reservedInstances": [],
    "templates": [
        {
            "oid": "100001",
            "name": "t3.nano",
            "family": "t3",
            "coupons": 1,
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
            "coupons": 2,
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
            "coupons": 4,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 3.0,
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
            "name": "t3a.nano",
            "family": "t3a",
            "coupons": 1,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 1.1,
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
            "oid": "100005",
            "name": "t3a.micro",
            "family": "t3a",
            "coupons": 2,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 2.1,
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
            "oid": "100006",
            "name": "t3a.small",
            "family": "t3a",
            "coupons": 4,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 3.1,
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
