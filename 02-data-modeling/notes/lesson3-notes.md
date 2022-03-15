# NoSQL Data Models

## Non-Relational Databases

When Not to Use SQL:

- Need High Availability in the Data
- Have Large Amounts of Data
- Need Linear Scalability
- Low Latency
- Fast Reads and Writes

Apache Cassandra:

- Open Source NoSQL DB
- Masterless Architecture
- High Availability
- Linearly Scalable

## Distributed Databases

Eventual Consistency - Over time each copy of the data will be the same, but if there are new changes, the data may be different in different locations.

## Cap Theorem

A theorem in computer science that states it is impossible for a distributed data store to simultaneously provide more than two out of the following three guarantees of consistency, availability, and partition tolerance.

Consistency - Every read from the database gets the latest piece of data or an error
Availability - Every request is received and a response is given - without a guarantee that the data is the latest update
Partition Tolerance - The system continues to work regardless of losing network connectivity between nodes

## Denormalization in Apache Cassandra

Denormalization of tables in Apache Cassandra is absolutely critical. The biggest take away when doing data modeling in Apache Cassandra is to think about your queries first. There are no JOINS in Apache Cassandra.

Data Modeling in Apache Cassandra:

- Denormalization is not just okay - it's a must
- Denormalization must be done for fast reads
- Apache Cassandra has been optimized for fast writes
- ALWAYS think Queries first
- One table per query is a great strategy
- Apache cassandra does not allow for JOINs between tables

## Cassandra Query Language: CQL

Cassandra query language is the way to interact with the database and is very similar to SQL. JOINs, GROUP BY, or subqueries are not in CQL and are not supported by CQL.

## Primary Key

- Must be unique
- Made up of just the PARTITION KEY or may include additional CLUSTERING COLUMNS
- A simple PRIMARY KEY is just one column that is also the PARTITION KEY. A Composite PRIMARY KEY is made up of more than one column and will assist in creating a unique value and in your retrieval queries
- The PARTITION KEY will determine the distribution of data across the system

## Clustering Columns

The PRIMARY KEY is made up of either just the PARTITION KEY or with the addition of CLUSTERING COLUMNS. The CLUSTERING COLUMN will determine the sort order within a Partition.

- Sorts the data in ascending order
- More than one clustering column can be added
- Clustering columns sort in order of how they were added to the primary key

As many clustering columns can be added as necessary.

## WHERE Clause

- Data Modeling in Apache Cassandra is query focused, and that focus needs to be on the WHERE clause
- Failure to include a WHERE clause will result in an error

## Lesson Wrap Up

- Basics of Distributed Database Design
- Must know your queries and model the tables to your queries
- Importance of Denormalization
- Apache Cassandra as a popular NoSQL database
- Primary Key, Partition Key (distribute), and Clustering (ordering) Column
- The WHERE clause

## Course Wrap Up

- Relational vs. Non-Relational Databases
- Fundamentals of Relational Database Modeling
- Normalization
- Denormalization
- Fundamentals of NoSQL data modeling
- Basics of Distributed Database design
