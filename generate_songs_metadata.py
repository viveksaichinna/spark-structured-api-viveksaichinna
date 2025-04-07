import csv
import random

songs = [f"song_{i}" for i in range(1, 51)]
genres = ["Pop", "Rock", "Jazz", "Classical", "Hip-Hop"]
moods = ["Happy", "Sad", "Energetic", "Chill"]

with open("songs_metadata.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["song_id", "title", "artist", "genre", "mood"])
    
    for song in songs:
        title = f"Title_{random.randint(1, 100)}"
        artist = f"Artist_{random.randint(1, 50)}"
        genre = random.choice(genres)
        mood = random.choice(moods)
        writer.writerow([song, title, artist, genre, mood])