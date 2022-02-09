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
      "currentRICoverage": {"coupons": 0.0},
      "groupProviders": [],
      "minCostProviderPerFamily": {},
      "groupSize": 1,
      "currentTemplateOid": "100002",
      "providersOid": [
        "100001",
        "100002"
      ],
      "currentRIOID": "0",
      "regionId":"30",
      "operatingSystemLicenseModel":"LICENSE_INCLUDED",
      "accountPricingDataOid":"10"
    }
  ],
  "reservedInstances": [],
  "templates": [
    {
      "oid": "100001",
      "name": "template1",
      "family": "family1",
      "commitmentAmount": {
        "coupons": 3
      },
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
      },
      "scalingPenalty": 0.5
    },
    {
      "oid": "100002",
      "name": "template2",
      "family": "family1",
      "commitmentAmount": {
        "coupons": 3
      },
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
      },
      "scalingPenalty": 0.5
    },
    {
      "oid": "100003",
      "name": "template2",
      "family": "family1",
      "commitmentAmount": {
        "coupons": 3
      },
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
