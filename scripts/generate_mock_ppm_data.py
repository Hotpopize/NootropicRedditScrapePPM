import json
import os
from datetime import datetime
import uuid

def generate_mock_data():
    mock_posts = [
        {
            "id": str(uuid.uuid4()),
            "title": "Finally giving up coffee, it's just not worth it anymore.",
            "text": "I've been drinking 3 cups of coffee a day for years, but recently the jitters and anxiety are through the roof. I literally can't focus because my heart is racing. I decided to try Lion's Mane instead to see if it gives me a cleaner energy boost without the crash. So far, it feels much safer and I'm sleeping better.",
            "author": "mock_user_1",
            "url": "https://reddit.com/r/Nootropics/mock1",
            "score": 45,
            "created_utc": datetime.now().timestamp(),
            "subreddit": "Nootropics",
            "type": "post"
        },
        {
            "id": str(uuid.uuid4()),
            "title": "Are these natural stacks actually doing anything?",
            "text": "I switched from Modafinil because I was worried about what it was doing to my heart long-term. I bought this expensive organic 'brain blend' that everyone here recommended, but honestly, I don't feel a thing. It's so expensive and I don't know who to trust with all these conflicting reviews. Might just go back to my prescription.",
            "author": "mock_user_2",
            "url": "https://reddit.com/r/StackAdvice/mock2",
            "score": 12,
            "created_utc": datetime.now().timestamp(),
            "subreddit": "StackAdvice",
            "type": "post"
        },
        {
            "id": str(uuid.uuid4()),
            "title": "My experience replacing Adderall with a natural stack",
            "text": "I hated the way Adderall made me feel like an emotionless zombie, and my tolerance kept going up so it wasn't even working anymore. A friend suggested I try Rhodiola and Bacopa. It's been a month. The effects are subtle, but there's no horrible crash at 3 PM, and I feel like my memory is genuinely improving long-term. Plus, it's way easier to buy online without dealing with doctors.",
            "author": "mock_user_3",
            "url": "https://reddit.com/r/Nootropics/mock3",
            "score": 150,
            "created_utc": datetime.now().timestamp(),
            "subreddit": "Nootropics",
            "type": "post"
        },
        {
            "id": str(uuid.uuid4()),
            "title": "Anyone else feel like this is cheating?",
            "text": "I started taking these cognitive enhancers for my university exams because everyone else was doing it, but I feel weird about it ethically. Like, am I really earning my grades? Also, the research on some of these 'safe' nootropics is totally lacking. Hard to separate the snake oil from real science.",
            "author": "mock_user_4",
            "url": "https://reddit.com/r/Nootropics/mock4",
            "score": 88,
            "created_utc": datetime.now().timestamp(),
            "subreddit": "Nootropics",
            "type": "post"
        },
        {
            "id": str(uuid.uuid4()),
            "title": "L-Theanine + Caffeine is the only thing that works for me",
            "text": "Coffee alone gives me palpitations. Adding L-Theanine completely smooths it out. It's cheap, I understand exactly how it works, and I don't have to change my morning routine too much. I tried pure nootropics but the learning curve and trying to figure out the right doses was just too overwhelming.",
            "author": "mock_user_5",
            "url": "https://reddit.com/r/StackAdvice/mock5",
            "score": 210,
            "created_utc": datetime.now().timestamp(),
            "subreddit": "StackAdvice",
            "type": "post"
        }
    ]

    os.makedirs('data', exist_ok=True)
    file_path = 'data/mock_reddit_posts.json'
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(mock_posts, f, indent=4)
        
    print(f"✅ Generated {len(mock_posts)} mock posts and saved to {file_path}")
    print("Use these in the Dashboard (or load them via a script) to test your automated coding pipeline.")

if __name__ == "__main__":
    generate_mock_data()
