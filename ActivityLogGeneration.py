import csv
import random
import string

NUM_ACTIONS = 10_000_000
NUM_USERS = 200_000
NUM_PAGES = 200_000
OUTPUT_FILE = "ActivityLog.csv"

VIEW_PROBABILITY = 0.75  # % of actions that are just views

NON_VIEW_ACTIONS = [
    "left a thoughtful note on the page",
    "poked the page owner playfully",
    "shared the page with their network",
    "commented with a detailed response",
    "reacted enthusiastically to the page",
    "sent a private message via the page",
]

def random_action_text(base_text):
    """Ensure length between 20 and 50 characters"""
    if len(base_text) >= 20:
        return base_text[:50]
    extra = ''.join(random.choices(string.ascii_lowercase + ' ', k=50))
    return (base_text + " " + extra)[:random.randint(20, 50)]

with open(OUTPUT_FILE, "w", newline="", buffering=1024*1024) as f:
    writer = csv.writer(f)

    action_id = 1

    while action_id <= NUM_ACTIONS:
        by_who = random.randint(1, NUM_USERS)
        what_page = random.randint(1, NUM_PAGES)
        action_time = random.randint(1, 1_000_000)

        writer.writerow([
            action_id,
            by_who,
            what_page,
            random_action_text("viewed the page"),
            action_time
        ])
        action_id += 1

        if action_id > NUM_ACTIONS:
            break

        if random.random() > VIEW_PROBABILITY:
            follow_up = random.choice(NON_VIEW_ACTIONS)
            writer.writerow([
                action_id,
                by_who,
                what_page,
                random_action_text(follow_up),
                action_time + random.randint(0, 5)
            ])
            action_id += 1