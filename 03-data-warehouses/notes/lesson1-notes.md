# Introduction to Data Warehouses

## Operational vs. Analytical Processes

Data Warehouse – a system (including processes, technologies & data representations) that enables us to support analytical processes
OLTP – Online Transactional Processing
OLAP – Online Analytical Processing

## Data Warehouse: Technical Perspective

Definitions:
• A data warehouse is a copy of transaction data specifically structured for query and analysis.
• A data warehouse is a subject-oriented, integrated, nonvolatile, and time-variant collection of data in support of management's decisions.
• A data warehouse is a system that retrieves and consolidates data periodically from the source systems into a dimensional or normalized data store. It usually keeps years of history and is queried for business intelligence or other analytical activities. It is typically updated in batches, not every time a transaction happens in the source system.
ETL – Extract, Transform, Load
Dimensional model is designed to:
• make it easy for business users to work with the data
• Improve analytical queries performance
The technologies used for storing dimensional models are different than traditional technologies
Goals:
• Simple to understand
• Performant
• Quality Assured
• Handles new questions well
• Secure

## Dimensional Modeling

Goals:
• Easy to understand
• Fast analytical query performance
3NF – lots of expensive joins; hard to explain to business users
Star Schema – joins with dimensions only; Good for OLAP not OLTP

### Facts and Dimensions

Fact Tables
• Record business events, like an order, a phone call, a book review
• Fact tables' columns record events in quantifiable metrics like quantity of an item, duration of a call, a book rating
Dimension Tables
• Record the context of the business events, e.g. who, what, where, why etc.
• Dimension tables columns contain attributes like the store at which an item is purchased, or the customer who made the call, etc.

### Fact or Dimension Dilemma

For facts, if you're unsure if a column is a fact or dimension, the simplest rule is that a fact is usually: Numeric and Additive

Example Facts:
• A comment on an article represents an event but we can not easily make a statistic out of its content per se (not a good fact)
• Invoice number is numeric but adding it does not make sense (Not a good fact)
• Total amount of an invoice could be added to compute total sales (a good fact)

Example Dimensions:
• Date and time are always a dimension
• Physical locations and their attributes are good candidates for dimensions
• Human Roles like customers and staff are always good candidates for dimensions
• Goods sold always good candidates for dimensions

Naïve ETL: From 3NF to ETL
• Query the 3NF DB (Extract)
• Then: (transform)
○ Join tables together
○ Change Types
○ Add New Columns
• Loading (Load)
○ Insert into fact & dimension tables

## Kimball's Bus Architecture

### Back Room:

ETL System:

- Transform from source to target
- Conform dimensions
- No user query support

-> Feeds Into ->

### Front Room

Presentation Area:

- Dimensional
- Atomic & Summary Data
- Organized by Business Procss
- Uses Conformed Dimensions
  Design Goals:
- Ease of use
- Query performance

<- Queried By <-

Application:

- Ad hoc queries
- Standard reports
- Analytic apps

### ETL:

- Extracting
  - Get the data from its source
  - Possibly deleting old state
- Transforming
  - Integrates many sources together
  - Possibly cleansing: inconsistencies, duplication, missing values, etc.
  - Possibly producing diagnostic metadata
- Loading
  - Structuring and loading the data into the dimensional data model

## DWH Architecture: Independent Data Marts

- Departments have independent ETL processes & dimensional models
- These separate & smaller dimensional models are called "Data Marts"
- Different fact tables for the same events, no conformed dimensions
- Uncoordinated efforts can lead to inconsistent views
- Despite awareness of the emergence of this architecture from departmental autonomy, it is generally discouraged

## DWH Architecture: CIF

- 2 ETL Processes
  - Source systems - 3NF DB
  - 3 NF DB - Departmental Data Marts
- The 3NF DB acts as an enterprise wide data source
  - Single integrated source of truth for data-marts
  - Could be accessed by end-users if needed
- Data marts dimensionally modeled & unlike Kimball's dimensional models, they are mostly aggregated

## OLAP Cubes

- Aggregation of a fact metric on a number of dimensions
- Easy to communicate to business users
- Common OLAP operations: rollup, drill-down, slice, dice

## OLAP Cubes: Roll-Up and Drill Down

- Roll-up: Sum up the sales of each city by country: e.g. US, France (less columns in branch dimension)
- Drill-Down: Decompose the sales of each city into smaller districts (more columns in branch dimension)
- The OLAP cubes should store the finest grain of data (atomic data), in case we need to drill-down to the lowest level, e.g. Country -> City -> District -> Street, etc...

## OLAP Cubes: Slice and Dice

- Slice: Reducing N dimensions to N-1 dimensions by restricting one dimension to a single value
  - E.g. month='MAR'
- Dice: Same dimensions by computing a sub-cube by restricting, some of the values of the dimensions
  - E.g. month in ['FEB', 'MAR'] and movie in ['Avatar', 'Batman'] branch='NY'

## OLAP Cubes: Query Optimization

- Business users will typically want to slice, dice, rollup and drill-down all the time
- Each such combination will potentially go through all the facts table (suboptimal)
- The GROUP BY CUBE (x, x, x) will make ONE pass through the facts table and will aggregate all possible combinations of groupings, of length 0, 1, 2, 3, etc.
- Saving/Materializing the output of the CUBE operation and using it is usually enough to answer all forthcoming aggregations from business users without having to process the whole facts table again
