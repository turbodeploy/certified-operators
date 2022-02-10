{
    "context": {
        "csp": "AWS",
        "osType": "LINUX",
        "regionId": "31",
        "billingFamilyId": "1",
        "tenancy": "DEFAULT"
    },
    "smaConfig": {
              "reduceDependency": "true"
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
            "currentRICoverage": {"coupons": 15.0},
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100002"
            ],
            "currentRIOID": "1000001",
            "regionId":"31",
            "operatingSystemLicenseModel":"LICENSE_INCLUDED",
            "accountPricingDataOid":"10"
        },
        {
            "oid": "2000002",
            "name": "VM2",
            "groupName": "",
            "businessAccountId": "10",
            "osType": "LINUX",
            "zoneId": "21",
            "providers": [],
            "currentRICoverage": {"coupons": 1.0},
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100001",
            "providersOid": [
                "100001",
                "100002",
                "100003"
            ],
             "currentRIOID": "1000001",
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
            "applicableBusinessAccounts": [],
            "count": 1,
            "isf": true,
            "shared": true,
            "platformFlexible": false,
            "commitmentAmount": {
                "coupons": 16
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
            "name": "t2.large",
            "family": "t2",
            "commitmentAmount": {
                "coupons": 16
            },
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 100.0,
                        "license": 20.0
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
            "name": "t2.xlarge",
            "family": "t2",
            "commitmentAmount": {
                "coupons": 32
            },
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 200.0,
                        "license": 40.0
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
                     "name": "t3.large",
                     "family": "t3",
                     "commitmentAmount": {
                "coupons": 16
            },
                     "onDemandCosts": {
                         "10": {
                             "LINUX": {
                                 "compute": 50.0,
                                 "license": 10.0
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
