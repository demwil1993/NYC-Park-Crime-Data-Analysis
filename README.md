# Enhancing Public Safety in NYC Parks Through Data-Driven Insights

## Context
New York City is renowned for its numerous public parks, which provide recreational space for residents and tourists alike. However, ensuring these parks remain safe for visitors is a critical concern for city officials, park management, and law enforcement agencies. Despite various measures to enhance safety, incidents of crimes such as murder, rape, robbery, and other felonies still occur, creating a need for a comprehensive understanding of crime patterns and trends within these public spaces.

## Problem
Currently, the NYC Parks Department and local law enforcement agencies lack a unified and detailed dataset that integrates crime data from all city parks over multiple years. This hampers their ability to:
1. Identify crime hotspots within parks.
2. Understand seasonal and yearly crime trends.
3. Allocate resources effectively to prevent crime.
4. Develop targeted intervention strategies to enhance public safety.

## Objective
To develop a robust ETL (Extract, Transform, Load) process that consolidates NYC park crime data from 2019 to 2023 into a structured format suitable for analysis. This will enable stakeholders to gain actionable insights and make informed decisions to improve public safety in NYC parks.

## Key Goals
1. **Data Integration:** Collect and integrate crime data from various sources and formats into a single, comprehensive dataset.
2. **Data Transformation:** Clean and transform the data to ensure consistency, accuracy, and readiness for analysis.
3. **Data Analysis:** Utilize the consolidated data to identify patterns, trends, and anomalies in park crime incidents.
4. **Resource Allocation:** Provide insights that help in the optimal allocation of law enforcement resources to reduce crime in parks.
5. **Policy Development:** Assist in the development of evidence-based policies and safety measures to enhance the safety and security of park visitors.

## Stakeholders
- **NYC Parks Department:** To use the data for managing park safety and improving visitor experience.
- **NYC Police Department (NYPD):** To allocate resources and plan interventions effectively.
- **City Government:** To develop policies and allocate budgets for public safety.
- **Community Organizations:** To engage in community policing and safety awareness programs.
- **Public:** To have a safer environment in NYC parks.

## ETL Process Overview
### Data Integration
Collect data from various sources and formats, including Excel files, CSV files, and databases, spanning from 2019 to 2023. 

### Data Transformation
Clean and transform the data to ensure consistency, accuracy, and readiness for analysis. This includes handling missing values, normalizing data formats, and ensuring data integrity.

### Database Design
The cleaned and transformed data is then loaded into a PostgreSQL database. The following ERD (Entity-Relationship Diagram) illustrates the structure of the database:

![ERD](https://github.com/demwil1993/NYC-Park-Crime-Data-Analysis/assets/79153503/963ea0ac-c6b0-4016-8fce-1a03ef4101db)

### Data Analysis
Utilize the consolidated data to identify patterns, trends, and anomalies in park crime incidents. 

## Analysis and Reporting
A PowerBI dashboard is created from the data stored in the PostgreSQL database to provide stakeholders with actionable insights. The dashboard includes various visualizations and reports to aid in decision-making.

![PowerBI Dashboard](https://github.com/demwil1993/NYC-Park-Crime-Data-Analysis/assets/79153503/a77681ba-d042-498b-827c-0509db916c84)

## Expected Outcomes
1. **Improved Safety:** Reduction in crime rates within NYC parks through data-driven interventions.
2. **Enhanced Resource Utilization:** More efficient deployment of law enforcement and park safety resources.
3. **Informed Policy Making:** Development of targeted safety policies and measures based on detailed crime data analysis.
4. **Public Awareness:** Increased public awareness and engagement in park safety initiatives.

By implementing this ETL process, NYC can leverage historical crime data to enhance the safety and security of its parks, ensuring that these public spaces remain welcoming and safe for all visitors.

