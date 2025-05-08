from neo4j import GraphDatabase

# Neo4j connection details
uri = "bolt://localhost:7687"
username = "neo4j"
password = "neo4jneo3j"  # Replace with your actual Neo4j password
driver = GraphDatabase.driver(uri, auth=(username, password))

def run_query(driver, query):
    """Execute a single Cypher query using the Neo4j driver."""
    with driver.session() as session:
        session.run(query)


# Load businesses from CSV (absolute file path)
load_businesses_query = """
LOAD CSV WITH HEADERS FROM 'file:///E:/DSM/FinalProj/DSMTOP100/DSMTOP100/TOP_100_business_state.csv' AS row
CREATE (:Business {
  business_id: row.business_id,
  name: row.name,
  address: row.address,
  city: row.city,
  state: row.state,
  latitude: toFloat(row.latitude),
  longitude: toFloat(row.longitude),
  stars: toFloat(row.stars),
  review_count: toInteger(row.review_count),
  is_open: toInteger(row.is_open) = 1,
  attributes: row.attributes,
  categories: row.categories,
  hours: row.hours
})
"""
print("Loading businesses...")
run_query(driver, load_businesses_query)

# Update businesses with check-in data (absolute file path)
update_checkins_query = """
LOAD CSV WITH HEADERS FROM 'file:///E:/DSM/FinalProj/DSMTOP100/DSMTOP100/TOP_100_checkins_state.csv' AS row
MATCH (b:Business {business_id: row.business_id})
SET b.checkins = split(row.checkin_dates, ', ')
"""
print("Updating check-ins...")
run_query(driver, update_checkins_query)

# Load users from CSV (absolute file path)
load_users_query = """
LOAD CSV WITH HEADERS FROM 'file:///E:/DSM/FinalProj/DSMTOP100/DSMTOP100/TOP_100_users_state.csv' AS row
CREATE (:User {
  user_id: row.user_id,
  name: row.name,
  review_count: toInteger(row.review_count),
  yelping_since: row.yelping_since,
  useful: toInteger(row.useful),
  funny: toInteger(row.funny),
  cool: toInteger(row.cool),
  elite: row.elite,
  friends: split(row.friends, ', '),
  fans: toInteger(row.fans),
  average_stars: toFloat(row.average_stars),
  compliment_hot: toInteger(row.compliment_hot),
  compliment_more: toInteger(row.compliment_more),
  compliment_profile: toInteger(row.compliment_profile),
  compliment_cute: toInteger(row.compliment_cute),
  compliment_list: toInteger(row.compliment_list),
  compliment_note: toInteger(row.compliment_note),
  compliment_plain: toInteger(row.compliment_plain),
  compliment_cool: toInteger(row.compliment_cool),
  compliment_funny: toInteger(row.compliment_funny),
  compliment_writer: toInteger(row.compliment_writer),
  compliment_photos: toInteger(row.compliment_photos)
})
"""
print("Loading users...")
run_query(driver, load_users_query)

# Create REVIEWED relationships from reviews CSV (absolute file path)
create_reviews_query = """
LOAD CSV WITH HEADERS FROM 'file:///E:/DSM/FinalProj/DSMTOP100/DSMTOP100/TOP_100_review_state.csv' AS row
MATCH (u:User {user_id: row.user_id})
MATCH (b:Business {business_id: row.business_id})
CREATE (u)-[:REVIEWED {
  review_id: row.review_id,
  stars: toFloat(row.stars),
  useful: toInteger(row.useful),
  funny: toInteger(row.funny),
  cool: toInteger(row.cool),
  text: row.text,
  date: row.date
}]->(b)
"""
print("Creating REVIEWED relationships...")
run_query(driver, create_reviews_query)

# Create TIPPED relationships from tips CSV (absolute file path)
create_tips_query = """
LOAD CSV WITH HEADERS FROM 'file:///E:/DSM/FinalProj/DSMTOP100/DSMTOP100/TOP_100_tips_state.csv' AS row
MATCH (u:User {user_id: row.user_id})
MATCH (b:Business {business_id: row.business_id})
CREATE (u)-[:TIPPED {
  text: row.text,
  date: row.date,
  compliment_count: toInteger(row.compliment_count)
}]->(b)
"""
print("Creating TIPPED relationships...")
run_query(driver, create_tips_query)

# Create FRIENDS_WITH relationships based on friends list
create_friendships_query = """
MATCH (u:User)
UNWIND u.friends AS friend_id
MATCH (f:User {user_id: friend_id})
CREATE (u)-[:FRIENDS_WITH]->(f)
"""
print("Creating FRIENDS_WITH relationships...")
run_query(driver, create_friendships_query)

# Close the driver connection
print("Closing connection...")
driver.close()
print("Data upload completed.")
