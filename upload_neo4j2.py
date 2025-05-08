from neo4j import GraphDatabase
import os

# Neo4j connection details (Update with your credentials)
uri = "bolt://localhost:7687"
username = "neo4j"
password = "neo4jneo5j"  
driver = GraphDatabase.driver(uri, auth=(username, password))

def run_query(driver, query, parameters=None):
    """Execute a single Cypher query using the Neo4j driver."""
    with driver.session() as session:
        # Use write_transaction for queries that modify the graph
        session.execute_write(lambda tx: tx.run(query, parameters))
        print(f"Executed query: {query[:100]}...") # Print start of query for logging

# --- Define Absolute File Paths ---
# Updated based on your provided path: E:\DSM\FinalProj\processed_filtered\vegas_businesses_filtered.csv
# Neo4j's LOAD CSV requires file URLs with forward slashes.

base_file_path = "E:/DSM/FinalProj/processed_filtered/" # Note: forward slashes

users_csv_path = f'file:///{base_file_path}vegas_users_filtered.csv'
businesses_csv_path = f'file:///{base_file_path}vegas_businesses_filtered.csv' # Using your provided filename
reviews_csv_path = f'file:///{base_file_path}vegas_reviews_filtered_cleaned_v4.csv'
friendships_csv_path = f'file:///{base_file_path}vegas_friendships.csv'

print(f"Using the following CSV paths:")
print(f"Users CSV: {users_csv_path}")
print(f"Businesses CSV: {businesses_csv_path}")
print(f"Reviews CSV: {reviews_csv_path}")
print(f"Friendships CSV: {friendships_csv_path}")
print(f"Ensure these files exist at the specified locations and Neo4j has read access.\n")


# --- Create Indexes for better performance ---
# These are idempotent, Neo4j won't create them if they already exist.
create_user_id_index_query = "CREATE INDEX user_user_id IF NOT EXISTS FOR (u:User) ON (u.user_id)"
create_business_id_index_query = "CREATE INDEX business_business_id IF NOT EXISTS FOR (b:Business) ON (b.business_id)"
# New index for Category nodes
create_category_name_index_query = "CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name)"
# New index for Review nodes
create_review_id_index_query = "CREATE INDEX review_review_id IF NOT EXISTS FOR (r:Review) ON (r.review_id)"


print("Creating indexes (if they don't exist)...")
run_query(driver, create_user_id_index_query)
run_query(driver, create_business_id_index_query)
run_query(driver, create_category_name_index_query) # Create the new category index
run_query(driver, create_review_id_index_query) # Create the new review index
print("Indexes checked/created.")

# --- Load Users from vegas_users_filtered.csv ---
# CSV Headers: user_id,name,yelping_since,review_count_total,useful,funny,cool,elite_years
load_users_query = f"""
LOAD CSV WITH HEADERS FROM '{users_csv_path}' AS row
MERGE (u:User {{user_id: row.user_id}})
ON CREATE SET
  u.name = row.name,
  u.yelping_since = row.yelping_since,
  u.review_count_total = toInteger(row.review_count_total),
  u.useful = toInteger(row.useful),
  u.funny = toInteger(row.funny),
  u.cool = toInteger(row.cool),
  u.elite_years = CASE WHEN row.elite_years IS NOT NULL AND trim(row.elite_years) <> '' THEN split(row.elite_years, ', ') ELSE [] END
ON MATCH SET // Keep ON MATCH in case the script is run multiple times
  u.name = row.name,
  u.yelping_since = row.yelping_since,
  u.review_count_total = toInteger(row.review_count_total),
  u.useful = toInteger(row.useful),
  u.funny = toInteger(row.funny),
  u.cool = toInteger(row.cool),
  u.elite_years = CASE WHEN row.elite_years IS NOT NULL AND trim(row.elite_years) <> '' THEN split(row.elite_years, ', ') ELSE [] END
"""
print("Loading users...")
run_query(driver, load_users_query)

# --- Load Businesses from vegas_businesses_filtered.csv ---
# CSV Headers: business_id,name,city,state,latitude,longitude,stars,review_count,categories
# Removed categories property from Business node
load_businesses_query = f"""
LOAD CSV WITH HEADERS FROM '{businesses_csv_path}' AS row
MERGE (b:Business {{business_id: row.business_id}})
ON CREATE SET
  b.name = row.name,
  b.city = row.city,
  b.state = row.state,
  b.latitude = toFloat(row.latitude),
  b.longitude = toFloat(row.longitude),
  b.stars = toFloat(row.stars),
  b.review_count = toInteger(row.review_count)
ON MATCH SET // Keep ON MATCH in case the script is run multiple times
  b.name = row.name,
  b.city = row.city,
  b.state = row.state,
  b.latitude = toFloat(row.latitude),
  b.longitude = toFloat(row.longitude),
  b.stars = toFloat(row.stars),
  b.review_count = toInteger(row.review_count)
"""
print("Loading businesses (without categories property)...")
run_query(driver, load_businesses_query)

# --- Create Category nodes and HAS_CATEGORY relationships ---
# This query iterates through the categories list and creates Category nodes and relationships
create_categories_query = f"""
LOAD CSV WITH HEADERS FROM '{businesses_csv_path}' AS row
MATCH (b:Business {{business_id: row.business_id}})
WHERE row.categories IS NOT NULL AND trim(row.categories) <> ''
UNWIND split(row.categories, ', ') AS category_name
MERGE (c:Category {{name: trim(category_name)}})
CREATE (b)-[:HAS_CATEGORY]->(c)
"""
print("Creating Category nodes and HAS_CATEGORY relationships...")
run_query(driver, create_categories_query)


# --- Create Review nodes and WROTE/ABOUT relationships ---
# CSV Headers: "review_id","user_id","business_id","stars","date","useful","funny","cool","text"
# This replaces the previous CREATE (u)-[:REVIEWED {...}]->(b) query
create_reviews_relationships_query = f"""
LOAD CSV WITH HEADERS FROM '{reviews_csv_path}' AS row
MATCH (u:User {{user_id: row.user_id}})
MATCH (b:Business {{business_id: row.business_id}})
MERGE (r:Review {{review_id: row.review_id}})
ON CREATE SET
  r.stars = toFloat(row.stars),
  r.date = row.date,
  r.useful = toInteger(row.useful),
  r.funny = toInteger(row.funny),
  r.cool = toInteger(row.cool),
  r.text = row.text
ON MATCH SET // Keep ON MATCH in case the script is run multiple times
  r.stars = toFloat(row.stars),
  r.date = row.date,
  r.useful = toInteger(row.useful),
  r.funny = toInteger(row.funny),
  r.cool = toInteger(row.cool),
  r.text = row.text
CREATE (u)-[:WROTE]->(r)
CREATE (r)-[:ABOUT]->(b)
"""
print("Creating Review nodes and WROTE/ABOUT relationships...")
run_query(driver, create_reviews_relationships_query)

# --- Create FRIENDS_WITH relationships from vegas_friendships.csv ---
# CSV Headers: user_id,friend_id
create_friendships_query = f"""
LOAD CSV WITH HEADERS FROM '{friendships_csv_path}' AS row
MATCH (u1:User {{user_id: row.user_id}})
MATCH (u2:User {{user_id: row.friend_id}})
CREATE (u1)-[:FRIENDS_WITH]->(u2)
"""
print("Creating FRIENDS_WITH relationships...")
run_query(driver, create_friendships_query)

# Close the driver connection
print("Closing Neo4j connection...")
driver.close()
print("Data upload script execution completed.")
