from typing import Dict, List, Tuple

# State indices
HEALTHY = 0
SLOW = 1
DEAD = 2

# Observation indices
LOW_LATENCY = 0
HIGH_LATENCY = 1
TIMEOUT = 2
MATCH = 3
CONFLICT = 4

class BeliefEngine:
    def __init__(self, peers: List[str]) -> None:
        self.peers = peers
        # Initial belief distribution for each peer
        # [P(HEALTHY), P(SLOW), P(DEAD)]
        self.beliefs: Dict[str, List[float]] = {
            peer: [0.8, 0.15, 0.05] for peer in peers
        }

        # Transition matrix T[s][s']
        self.T = [
            [0.95, 0.03, 0.02],  # From HEALTHY
            [0.15, 0.75, 0.10],  # From SLOW
            [0.05, 0.05, 0.90]   # From DEAD
        ]

        # Observation matrix Z[s'][o]
        self.Z = [
            [0.80, 0.10, 0.01, 0.08, 0.01],  # HEALTHY
            [0.05, 0.70, 0.15, 0.05, 0.05],  # SLOW
            [0.001, 0.001, 0.996, 0.001, 0.001]  # DEAD
        ]

    def update_belief(self, peer_id: str, observation: int) -> List[float]:
        """
        Executes a weighted Bayesian update on the belief state of a peer given an observation.
        """
        peer_id = str(peer_id)
        if peer_id not in self.beliefs:
            return [0.8, 0.15, 0.05]

        prior = self.beliefs[peer_id]

        # 1. Prediction step: bar_b(s') = sum_s T[s][s'] * b(s)
        bar_b = [0.0, 0.0, 0.0]
        for s_prime in range(3):
            bar_b[s_prime] = sum(self.T[s][s_prime] * prior[s] for s in range(3))

        # 2. Update step: b_new(s') = Z[s'][o] * bar_b(s') / Normalization
        raw_b = [0.0, 0.0, 0.0]
        for s_prime in range(3):
            raw_b[s_prime] = self.Z[s_prime][observation] * bar_b[s_prime]

        norm = sum(raw_b)
        if norm > 0:
            self.beliefs[peer_id] = [r / norm for r in raw_b]
        else:
            # Fallback to prior in case of numerical anomaly
            self.beliefs[peer_id] = prior

        return self.beliefs[peer_id]

    def suspicion_score(self, peer_id: str) -> float:
        """
        Computes suspicion score: P(DEAD) + 0.5 * P(SLOW)
        """
        peer_id = str(peer_id)
        b = self.beliefs.get(peer_id, [0.8, 0.15, 0.05])
        return b[DEAD] + 0.5 * b[SLOW]

    def get_election_timeout_bias(self, current_leader_id: str) -> float:
        """
        If the current leader has high suspicion score, we bias the election timeout
        to start candidate reelection faster (shrinking the timeout).
        """
        current_leader_id = str(current_leader_id)
        if not current_leader_id or current_leader_id not in self.beliefs:
            return 1.0
        
        susp = self.suspicion_score(current_leader_id)
        if susp > 0.6:
            # High suspicion of leader failure: start election faster (up to 5x faster)
            return max(0.2, 1.0 - susp)
        return 1.0

    def get_retry_interval_bias(self, peer_id: str) -> float:
        """
        If a peer is suspected to be slow or dead, back off retries to avoid overloading.
        """
        peer_id = str(peer_id)
        if peer_id not in self.beliefs:
            return 1.0
        
        b = self.beliefs[peer_id]
        # P(SLOW) adds up to 5x retry interval, P(DEAD) adds up to 20x retry interval
        return 1.0 + 5.0 * b[SLOW] + 20.0 * b[DEAD]
