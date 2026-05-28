import numpy as np
import sqlite3
import pandas as pd
from db import DB

STATES = [
    "focus",
    "shallow_work",
    "communication",
    "entertainment",
    "context_switching",
    "idle"
]

class BeliefStateEngine:
    def __init__(self):
        self.n = len(STATES)
        self.transition = np.ones((self.n, self.n))
        self.belief = np.ones(self.n) / self.n

    def load(self):
        conn = sqlite3.connect(DB)
        df = pd.read_sql_query("SELECT * FROM sessions ORDER BY start_time", conn)
        conn.close()
        return df

    def likelihood(self, obs, state):
        cat = obs["category"]
        dur = obs["duration"]
        idle = obs["idle"]

        if state == "focus":
            return 1.5 if cat == "development" else 0.3
        if state == "communication":
            return 2.0 if cat == "communication" else 0.2
        if state == "entertainment":
            return 2.0 if cat == "entertainment" else 0.1
        if state == "idle":
            return 2.0 if idle else 0.1

        return 0.5

    def update(self, obs):
        new_b = np.zeros(self.n)

        for i in range(self.n):
            emission = self.likelihood(obs, STATES[i])
            trans = sum(self.transition[j][i] * self.belief[j] for j in range(self.n))
            new_b[i] = emission * trans

        new_b /= (np.sum(new_b) + 1e-9)
        self.belief = new_b

        idx = int(np.argmax(self.belief))
        return {
            "state": STATES[idx],
            "confidence": float(self.belief[idx]),
            "belief": dict(zip(STATES, self.belief))
        }

    def learn_transitions(self):
        df = self.load()
        prev = None

        for _, r in df.iterrows():
            cur = r["category"]
            if prev in STATES and cur in STATES:
                i = STATES.index(prev)
                j = STATES.index(cur)
                self.transition[i][j] += 1
            prev = cur

        self.transition /= self.transition.sum(axis=1, keepdims=True)

    def run_trace(self):
        df = self.load()
        trace = []

        for _, r in df.iterrows():
            obs = {
                "duration": r["duration"],
                "category": r["category"],
                "idle": r["idle"]
            }
            trace.append(self.update(obs))

        return trace
