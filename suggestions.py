import numpy as np

class SuggestionEngine:

    def entropy(self, belief):
        p = np.array(list(belief.values())) + 1e-9
        return -np.sum(p * np.log(p))

    def analyze(self, trace, sessions):
        ent = [self.entropy(t["belief"]) for t in trace]
        avg = np.mean(ent)

        short = sum(1 for s in sessions if s["duration"] < 300)

        suggestions = []

        if avg > 1.2:
            suggestions.append("High instability detected")

        if short / len(sessions) > 0.5:
            suggestions.append("Too many short sessions")

        if not suggestions:
            suggestions.append("Behavior stable")

        return suggestions
