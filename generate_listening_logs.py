import csv
import random
from datetime import datetime, timedelta

users = [f"user_{i}" for i in range(1, 101)]
songs = [f"song_{i}" for i in range(1, 51)]

with open("listening_logs.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["user_id", "song_id", "timestamp", "duration_sec"])
    
    for _ in range(5000):
        user = random.choice(users)
        song = random.choice(songs)
        timestamp = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        duration = random.randint(30, 300)
        writer.writerow([user, song, timestamp.strftime('%Y-%m-%d %H:%M:%S'), duration])
    