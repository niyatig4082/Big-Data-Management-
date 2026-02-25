import random

data = {}

with open("P2_Dataset.csv", "w") as f:
    for i in range(10_000):
        w = random.randint(0, 10_000)
        x = random.randint(20_000, 1_000_000)
        y = random.randint(-5_000, 5000)
        z = random.randint(250_000, 750_000)
        f.write(f"{w},{x},{y},{z}\n")
    