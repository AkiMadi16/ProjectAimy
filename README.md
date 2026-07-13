# Project Aimy

**Power BI | Data Modelling | Microsoft Fabric | Operational Decision Support**

## Overview

**Project Aimy** is a business intelligence project that demostrates how data analytics can support operational decision-making within the childcare industry. This project transforms raw data in to real impact, empowering center managers to monitor performance, optimise enrollment, improve capacity utilisation and to make informed business decisions.

The solution combinees **Power BI dashboards**, **data modelling**, and **Microsoft Fabric data engineering** principles to create an operational reporting platform focused on childcare centre performance.

## Business Problem

**Childcare centers** needs to ensure that they provide a quality care and a sustainable business through measurable outcomes. Tracking the right metrics such as tracking enrollments and conversion rates at each step of the process and identify where families are dropping off and fix those pain points. It allows business to understand opportunities to increase enrollments by attracting more families while providing a quality service.

## Center Management Dashboard

This Dashboard provides a view of the operational metrics of a childcare cantre through business and enrollment metrics.


## Business-side metrics

- Occupancy Rate℅ : 

Shows the percentage of licensed capacity that is currently enrolled (e.g 85% full), directly impacting revenue potential. 

***Business Value*** - Support stratergic enrollment planning 
***Goal*** - increase occupancy rate **100%**.

-  Enrollment Rate / New Enrollments: 

Tracks the number of new children enrolled over a period, indicates marketing and growth effectiveness.

## Technologies

- Microsoft Power BI
- SSMS
- Microsoft Fabric (Lakehouse/ Notebook)
- Power Query
- DAX

## Skills Demonstrated

1. Identify Key tables

- First Step - Data Profiling
     - Data Quality Fix 
     -- ❌ Problems: Same AccountId -> multiple DOB
      Duplicate attendees

2. Define Business Goal

3. Understand Relationships

4. Sample Queries

```
SELECT distinct(BusinessUnitId) AS totalbusinessunits
FROM Enrollment 
```

| TotalAttendee                                                    | TotalBusinessUnits | TotalOrgs                | TotalAccounts      |
| ----------------------------------------------------------- | --------------- | ---------------------- | ------------------| |  |474     | 8| 17 | 371 |
