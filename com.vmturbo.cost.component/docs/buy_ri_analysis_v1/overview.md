# Buy RI Analysis

## Overview
The buy RI analysis works by collecting allocation and scaling demand of workloads (currently only virtual machines) over time. The tracked historical demand is used as a predictor of future behavior, allowing the analysis to recommend RI purchases in cases where it projects purchasing an RI will be cheaper than paying for the projected demand at an on-demand rate. The buy RI analysis has 3 essential steps:
1. Recording of demand over time, in order to base projected demand on historical usage.
2. At the point analysis is invoked, the current RI inventory is applied to recorded demand, in order to calculate **uncovered demand**.
3. Uncovered demand and pricing data (both on-demand and RI prices) are used to calculate the break even point at which the RI cost and on-demand cost of X demand would be equivalent. If the calculated uncovered demand is greater than the break even demand of X, a recommendation is made.

## Demand Tracking
The analysis tracks two types of demand, in order to make recommendations under two distinct assumptions:
1. **Allocation** - In order to make recommendations, in which it is assumed no scaling actions will be executed i.e. the recommendations will be made based on the **allocated** instance type of each workload.
2. **Projected** - In the codebase, this is generally referred to as **consumption** demand. Projected demand is tracked, in order to make recommendations based on demand after scaling actions have been executed. This is answering the question "Which RIs should I purchase, based on right sizing my workloads?".

### Weighted Demand
Demand is collected and tracked one per hour over a week, such that there are 168 datapoints collected (24 hours x 7 days). Each datapoint is qualified by the following criteria:
* Account
* Location (availability zone or region)
* Instance type
* Operating system
* Tenancy
* Day
* Hour

Allocation and projected demand are stored separately. Upon collecting the same datapoint twice (i.e. a match for all qualifying criteria listed above), the demand is averaged using a configurable weighting. The default is to take 60% of the current demand value with 40% of the stored demand value (which will represent a weighting of all previous stored demand). For example, in collecting a datapoint for t3.nano allocated demand in us-east-1 for Account A on Monday at 12 pm, the current demand value will be averaged with the previously recorded value for the same instance type/region/account from last Monday at 12 pm.

The demand is stored in the instance_type_hourly_by_week table within the cost database.

### Collecting Allocation Demand
Allocated demand is collected once per hour, storing total allocated demand (RI covered and on-demand) based on the source topology broadcast by the topology-processor.

### Collection Projected (Consumption) Demand
The market broadcasts a projected topology, representing the topology graph after executing all market generated actions. This projected topology will include updates to workloads, based on moving towards available RIs (where available) or to the ideal (natural) instance type of the workload. Because the projected instance type of a workload is biased towards available RIs, the demand tracker ignores any demand due to RI coverage i.e. it only records **uncovered demand**. This diverges from allocation demand, in which all demand is recorded.

If instead all projected demand were to be recorded, once the biasing RI were to expire, the analysis would simply recommend a replacement. For example, if a workload could naturally fit on a t3.nano (smaller) instance type, but the market is recommending to scale the workload to an m4.large (larger) to utilize an available RI, the projected topology will indicate the workload is on an m4.large instance type with 100% RI coverage (or some value > 0%). In recording demand for buy RI analysis, there are two options. Either 1) m4.large demand can be recorded and RI inventory can be subtracted from recorded demand at the point the analysis runs or 2) the workload can be ignored; no m4.large demand will be recorded. 

If the demand recording and uncovered demand calculation are designed around #1, while the m4 RI exists, the analysis will not make any recommendation for this demand. Once the m4 RI expires, the analysis will immediately recommend purchasing another m4 RI (as long as the workload is running long enough to justify the purchase). This 