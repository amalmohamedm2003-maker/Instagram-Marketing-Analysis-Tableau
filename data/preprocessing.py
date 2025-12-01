
import os, json, pandas as pd

# 1. Load base paths
BASE = "instagram_account_data_export"

posts_json     = os.path.join(BASE, "posts", "posts.json")
media_json     = os.path.join(BASE, "media", "media.json")
likes_json     = os.path.join(BASE, "likes.json")
comments_json  = os.path.join(BASE, "comments.json")
profile_json   = os.path.join(BASE, "profile_information.json")

# 2. Read JSON files
posts     = pd.read_json(posts_json)
media     = pd.read_json(media_json)
likes     = pd.read_json(likes_json)
comments  = pd.read_json(comments_json)

# 3. Convert likes → like_count per post
likes_count = likes.groupby("media_id").size().rename("like_count")

# 4. Convert comments → comment_count per post
comment_count = comments.groupby("media_id").size().rename("comment_count")

# 5. Merge all information into single file
df = posts.merge(media[["media_id","media_type"]], on="media_id", how="left")
df = df.merge(likes_count,   on="media_id", how="left")
df = df.merge(comment_count, on="media_id", how="left")

# 6. Final cleaning
df["timestamp"] = pd.to_datetime(df["timestamp"])   # standard datetime
df.fillna(0, inplace=True)                          # replace blanks with zero

# 7. Export to CSV (Final dataset used for Tableau)
output_name = "posts_merged_tableau_instagram.csv"
df.to_csv(output_name, index=False)

print("\n CSV successfully created!")
print(f"Saved as → {output_name}")
print(f"Total Posts Processed: {len(df)}")
