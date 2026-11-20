import csv
import random
import time
from datetime import datetime
import os

LOG_DIR = os.path.abspath("hdfs_root/raw_logs")
os.makedirs(LOG_DIR, exist_ok=True)

USER_IDS = [f"U{i:04d}" for i in range(1, 101)]
MOVIE_IDS = [f"M{i:03d}" for i in range(101, 115)]
DEVICES = ["Smart TV", "Mobile App", "Web Browser", "Tablet"]
EVENT_TYPES = ["play_start", "buffer_wait", "pause", "play_end"]

REVIEWS = [
    "The plot twist in season 2 was completely unexpected, amazing acting!",
    "Buffering issue every 5 minutes, terrible streaming quality.",
    "Boring storyline, slow pacing, could not finish the movie.",
    "Visuals were insane! Absolute masterpiece of cinematography.",
    "Ending was wild, total cliffhanger for the next season.",
    "Audio quality was quiet, bad sound mixing and dialogue tones."
]

def generate_watch_logs(record_count=1000):
    filename = os.path.join(LOG_DIR, f"watch_logs_{int(time.time())}.csv")
    
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "timestamp", "user_id", "movie_id", "device_type", 
            "event_type", "watch_duration_sec", "rating", "review_text"
        ])
        
        for _ in range(record_count):
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                random.choice(USER_IDS),
                random.choice(MOVIE_IDS),
                random.choice(DEVICES),
                random.choice(EVENT_TYPES),
                random.randint(10, 7200),
                random.randint(1, 5),
                random.choice(REVIEWS)
            ])
            
    print(f"✅ Generated {record_count} watch log records -> {filename}")
    return filename

if __name__ == "__main__":
    generate_watch_logs(5000)
