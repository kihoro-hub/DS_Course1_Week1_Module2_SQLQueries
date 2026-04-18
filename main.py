import pandas as pd
import sqlite3

# =============================================================================
# Introduction
# =============================================================================
# This lab explores advanced SQL queries for analyzing data on a granular level.
# Databases used:
#   - planets.db: Data on planets in our solar system
#   - dogs.db: Data on famous fictional dog characters
#   - babe_ruth.db: Babe Ruth's baseball career statistics

# =============================================================================
# Setup Connections
# =============================================================================

conn1 = sqlite3.connect('planets.db')
conn2 = sqlite3.connect('dogs.db')
conn3 = sqlite3.connect('babe_ruth.db')

# Preview all tables
print("=== planets table ===")
print(pd.read_sql("SELECT * FROM planets;", conn1))

print("\n=== dogs table ===")
print(pd.read_sql("SELECT * FROM dogs;", conn2))

print("\n=== babe_ruth_stats table ===")
print(pd.read_sql("SELECT * FROM babe_ruth_stats;", conn3))

# =============================================================================
# Part I: Basic Filtering
# =============================================================================

# Step 1: Return all columns for planets that have 0 moons.
df_no_moons = pd.read_sql("""
SELECT *
FROM planets
WHERE num_of_moons = 0;
""", conn1)
print("\n=== Step 1: Planets with 0 moons ===")
print(df_no_moons)

# Step 2: Return the name and mass of each planet with exactly 7 letters in its name.
df_name_seven = pd.read_sql("""
SELECT name, mass
FROM planets
WHERE length(name) = 7;
""", conn1)
print("\n=== Step 2: Planets with 7-letter names ===")
print(df_name_seven)

# =============================================================================
# Part 2: Advanced Filtering
# =============================================================================

# Step 3: Return the name and mass for each planet with mass <= 1.00.
df_mass = pd.read_sql("""
SELECT name, mass
FROM planets
WHERE mass <= 1.00;
""", conn1)
print("\n=== Step 3: Planets with mass <= 1.00 ===")
print(df_mass)

# Step 4: Return all columns for planets with at least one moon and mass < 1.00.
df_mass_moon = pd.read_sql("""
SELECT *
FROM planets
WHERE num_of_moons >= 1
AND mass < 1.00;
""", conn1)
print("\n=== Step 4: Planets with >= 1 moon and mass < 1.00 ===")
print(df_mass_moon)

# Step 5: Return the name and color of planets whose color contains "blue".
df_blue = pd.read_sql("""
SELECT name, color
FROM planets
WHERE color = 'blue';
""", conn1)
print("\n=== Step 5: Planets with color 'blue' ===")
print(df_blue)

# =============================================================================
# Part 3: Ordering and Limiting
# =============================================================================

# Step 6: Return name, age, and breed of hungry dogs, sorted youngest to oldest.
df_hungry = pd.read_sql("""
SELECT name, age, breed, hungry
FROM dogs
WHERE hungry = 1
ORDER BY age ASC;
""", conn2)
print("\n=== Step 6: Hungry dogs sorted by age (youngest to oldest) ===")
print(df_hungry)

# Step 7: Return name, age, and hungry columns for hungry dogs aged 2–7, sorted alphabetically.
df_hungry_ages = pd.read_sql("""
SELECT name, age, hungry
FROM dogs
WHERE hungry = 1
AND age BETWEEN 2 AND 7
AND name IS NOT NULL
ORDER BY name ASC;
""", conn2)
print("\n=== Step 7: Hungry dogs aged 2-7, sorted alphabetically ===")
print(df_hungry_ages)

# Step 8: Return name, age, and breed for the 4 oldest dogs, sorted alphabetically by breed.
df_4_oldest = pd.read_sql("""
SELECT name, age, breed
FROM dogs
ORDER BY age DESC, breed ASC
LIMIT 4;
""", conn2)
print("\n=== Step 8: 4 oldest dogs sorted by breed ===")
print(df_4_oldest)

# =============================================================================
# Part 4: Aggregation
# =============================================================================

# Step 9: Return the total number of years Babe Ruth played professional baseball.
df_ruth_years = pd.read_sql("""
SELECT COUNT(year) AS num_years
FROM babe_ruth_stats
""", conn3)
print("\n=== Step 9: Total years Babe Ruth played ===")
print(df_ruth_years)

# Step 10: Return the total number of homeruns hit by Babe Ruth during his career.
df_hr_total = pd.read_sql("""
SELECT HR AS num_hr
FROM babe_ruth_stats
""", conn3).sum()
print("\n=== Step 10: Total career homeruns ===")
print(df_hr_total)

# =============================================================================
# Part 5: Grouping and Aggregation
# =============================================================================

# Step 11: For each team, return the team name and number of years Babe Ruth played on it.
df_teams_years = pd.read_sql("""
SELECT team, COUNT(year) AS number_years
FROM babe_ruth_stats
GROUP BY team
""", conn3)
print("\n=== Step 11: Years played per team ===")
print(df_teams_years)

# Step 12: For each team where Babe Ruth averaged over 200 at bats,
#          return the team name and average at bats.
df_at_bats = pd.read_sql("""
SELECT team, AVG(at_bats) AS average_at_bats
FROM babe_ruth_stats
GROUP BY team
HAVING AVG(at_bats) > 200
""", conn3)
print("\n=== Step 12: Teams with average at bats > 200 ===")
print(df_at_bats)

# =============================================================================
# Close Connections
# =============================================================================

conn1.close()
conn2.close()
conn3.close()
