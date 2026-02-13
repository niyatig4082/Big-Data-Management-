import csv
import random

ids = [i for i in range(1, 200001)]
relations = [
  "College friend from my freshman year dorm",
  "Close family member I keep in touch with",
  "High school friend from band class",
  "Coworker from my previous internship",
  "Favorite author whose work I admire",
  "Celebrity whose interviews I enjoy",
  "Professor who shares useful insights",
  "Photographer whose style inspires me",
  "Old neighbor I grew up next to",
  "Friend I met through a study group",
  "Musician whose songs I love",
  "Relative who posts family updates",
  "Artist with consistently great content",
  "Online friend from a shared hobby",
  "Startup founder with interesting ideas",
  "Travel blogger with great tips",
  "Former teammate from a club sport",
  "Researcher in a field I’m curious about",
  "Comedian whose humor matches mine",
  "Mentor who gives solid career advice",
  "Game developer working on cool projects",
  "Friend of a friend I met at events",
  "Alumni from my college program",
  "Journalist covering topics I follow",
  "Local creator supporting the community",
  "Volunteer I met through community work",
  "Speaker whose talks are very insightful",
  "Developer sharing helpful technical posts"
]

with open("follows.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    for i in range(1, 20_000_001):
        if i % 100000 == 0:
            print(i)

        id1, id2 = random.sample(ids, 2)

        writer.writerow([
            i,
            id1,
            id2,
            random.randint(1, 1_000_000),
            random.choice(relations)
        ])