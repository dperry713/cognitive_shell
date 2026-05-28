import sqlite3
from belief_engine import BeliefStateEngine
from suggestions import SuggestionEngine
from db import DB

def load_sessions():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT duration FROM sessions")
    rows = cur.fetchall()
    conn.close()
    return [{"duration": r[0]} for r in rows]

def main():
    engine = BeliefStateEngine()
    engine.learn_transitions()

    trace = engine.run_trace()
    sessions = load_sessions()

    sugg = SuggestionEngine()
    results = sugg.analyze(trace, sessions)

    print("\nSUGGESTIONS:")
    for r in results:
        print("-", r)

if __name__ == "__main__":
    main()
