{
    "context": {
        "csp": "AWS",
        "osType": "LINUX",
        "regionId": "30",
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
            "currentRICoverage": 0.0,
            "groupProviders": [],
            "minCostProviderPerFamily": {},
            "groupSize": 1,
            "currentTemplateOid": "100003",
            "providersOid": [
                "100001",
                "100002",
                "100003"
            ],
            "currentRIOID": "0"
        },
        {
          "oid": "2000002",
          "name": "VM2",
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
                  "oid": "2000003",
                  "name": "VM3",
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
                "zoneId": "21",
                "templateOid": "100003"
            }
        ],
    "templates": [
        {
            "oid": "100001",
            "name": "template1",
            "family": "family1",
            "coupons": 3,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 200.0,
                        "license": 20.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 9.0,
                        "license": 2.0
                    }
                }
            },
            "scalingPenalty": 0.5
        },
        {
            "oid": "100002",
            "name": "template2",
            "family": "family1",
            "coupons": 3,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 200.0,
                        "license": 20.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 10.0,
                        "license": 2.0
                    }
                }
            }
        },
        {
            "oid": "100003",
            "name": "template2",
            "family": "family1",
            "coupons": 3,
            "onDemandCosts": {
                "10": {
                    "LINUX": {
                        "compute": 300.0,
                        "license": 20.0
                    }
                }
            },
            "discountedCosts": {
                "10": {
                    "LINUX": {
                        "compute": 10.0,
                        "license": 2.0
                    }
                }
            }
        }
    ]
}