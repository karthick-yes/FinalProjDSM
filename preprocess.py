import polars as pl
from pathlib import Path
from tqdm import tqdm
import time

RAW_DIR = Path("raw")
OUT_DIR = Path("processed")
OUT_DIR.mkdir(exist_ok=True)

# Function to count lines in a file for progress tracking
def count_lines(file_path):
    print(f"Counting lines in {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)

# Function to show manual progress in tqdm
def process_with_progress(message, total, callback_fn):
    with tqdm(total=total, desc=message) as pbar:
        result = callback_fn()
        pbar.update(total)  # Mark as complete
        return result

# # 1) Business table
# print("\nProcessing businesses data...")
# business_file = RAW_DIR / "yelp_academic_dataset_business.json"
# total_businesses = count_lines(business_file)

# def process_businesses():
#     return (
#         pl.scan_ndjson(business_file, infer_schema_length=10000)
#           .select([
#               "business_id", "name", "city", "state", "latitude", "longitude",
#               "stars", "review_count",
#               # categories may be inferred as string; ensure it's Utf8
#               pl.col("categories").cast(pl.Utf8).alias("categories")
#           ])
#           .collect()
#     )

# businesses_df = process_with_progress("Businesses", total_businesses, process_businesses)
# businesses_df.write_csv(OUT_DIR / "businesses.csv")
# print(f"✓ Businesses data saved to {OUT_DIR / 'businesses.csv'}")

# # 2) Users table
# print("\nProcessing users data...")
# users_file = RAW_DIR / "yelp_academic_dataset_user.json"
# total_users = count_lines(users_file)

# def process_users():
#     return (
#         pl.scan_ndjson(users_file, infer_schema_length=10000)
#           .select([
#               "user_id", "name", "yelping_since", pl.col("review_count").alias("review_count_total"),
#               "useful", "funny", "cool",
#               # elite years might also be string or list; cast to Utf8
#               pl.col("elite").cast(pl.Utf8).alias("elite_years"),
#               "friends"
#           ])
#     ).collect()

# # Process users
# users_df = process_with_progress("Users", total_users, process_users)

# # Save users without friends column
# users_df.drop("friends").write_csv(OUT_DIR / "users.csv")
# print(f"✓ Users data saved to {OUT_DIR / 'users.csv'}")

# # 2b) Friendship edges
# print("\nProcessing friendships...")

# def process_friendships():
#     return (
#         users_df
#         .select([
#             "user_id", 
#             # Split the friends string by comma and handle empty strings
#             pl.when(pl.col("friends") == "")
#               .then(pl.lit([]))
#               .otherwise(pl.col("friends").str.split(","))
#               .alias("friends_list")
#         ])
#         .explode("friends_list")
#         .filter(pl.col("friends_list") != "None")  # Filter out "None" values if they exist
#         .select(["user_id", pl.col("friends_list").alias("friend_id")])
#     )

# friendships_df = process_with_progress("Friendships", len(users_df), process_friendships)
# friendships_df.write_csv(OUT_DIR / "friendships.csv")
# print(f"✓ Friendships data saved to {OUT_DIR / 'friendships.csv'}")

# 3) Reviews table
print("\nProcessing reviews data...")
reviews_file = RAW_DIR / "yelp_academic_dataset_review.json"
total_reviews = count_lines(reviews_file)

def process_reviews():
    return (
        pl.scan_ndjson(reviews_file, infer_schema_length=10000)
          .select([
              "review_id", "user_id", "business_id",
              "stars", "date", "useful", "funny", "cool",
              pl.col("text").str.replace_all(r"\n", " ")
          ])
          .collect()
    )

reviews_df = process_with_progress("Reviews", total_reviews, process_reviews)
reviews_df.write_csv(OUT_DIR / "reviews.csv")
print(f"✓ Reviews data saved to {OUT_DIR / 'reviews.csv'}")

print("\nAll data processing complete! Files saved to the 'processed' directory.")