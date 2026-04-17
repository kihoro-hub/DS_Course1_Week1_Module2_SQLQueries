# SQL Filter, Order & Group Lab

A Jupyter Notebook lab assessment focused on writing advanced SQL queries to analyze data at a granular level using Python and SQLite.

## Overview

This lab covers four key SQL operations — filtering, ordering, limiting, and grouping — applied across three different databases. Each section builds on the previous, progressing from basic to more complex queries.

## Databases Used

| Database | Description |
|---|---|
| `planets.db` | Data about planets in our solar system |
| `dogs.db` | Data about famous fictional dog characters |
| `babe_ruth.db` | Babe Ruth's career baseball statistics |

## Learning Objectives

- Retrieve subsets of records using `WHERE` clauses
- Filter results using conditional operators (`AND`, `BETWEEN`, `LIKE`, `LENGTH`)
- Apply aggregate functions (`COUNT`, `SUM`, `AVG`)
- Order query results using `ORDER BY` (ascending and descending)
- Limit the number of records returned with `LIMIT`
- Use `GROUP BY` with aggregate functions
- Filter grouped results using `HAVING`

## Lab Structure

### Part 1 — Basic Filtering (`planets.db`)
- Step 1: Filter planets with 0 moons
- Step 2: Filter planets by name length (exactly 7 letters)

### Part 2 — Advanced Filtering (`planets.db`)
- Step 3: Filter by mass (≤ 1.00)
- Step 4: Filter by multiple conditions (moons and mass)
- Step 5: Filter by color string match

### Part 3 — Ordering and Limiting (`dogs.db`)
- Step 6: Filter hungry dogs, sorted youngest to oldest
- Step 7: Filter hungry dogs by age range, sorted alphabetically
- Step 8: Return the 4 oldest dogs, sorted by breed

### Part 4 — Aggregation (`babe_ruth.db`)
- Step 9: Count total years played
- Step 10: Sum total career home runs

### Part 5 — Grouping and Aggregation (`babe_ruth.db`)
- Step 11: Group by team, count years played per team
- Step 12: Group by team, filter teams with average at bats > 200

## Prerequisites

- Python 3.x
- Jupyter Notebook or JupyterLab
- The following Python libraries:
  - `pandas`
  - `sqlite3` (built-in)

## Installation

1. Clone or download this repository.
2. Install dependencies:
   ```bash
   pip install pandas jupyter
   ```
3. Ensure the three database files (`planets.db`, `dogs.db`, `babe_ruth.db`) are in the same directory as the notebook.

## Usage

1. Launch Jupyter Notebook:
   ```bash
   jupyter notebook
   ```
2. Open `SQLFilterOrderGroupLab.ipynb`.
3. Run the setup cells first (marked `# Run this cell without changes`).
4. Complete each step by replacing `None` with your SQL query.

## Notes

- Each database uses a separate connection (`conn1`, `conn2`, `conn3`) to avoid conflicts.
- Remember to run the final cell to close all database connections when done.
- Steps are graded via CodeGrade — make sure your variable names match exactly as given.
