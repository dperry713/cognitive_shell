import time
import random
from typing import Any, Dict, List, Optional, Set
from core.log.model import RaftLog, LogEntry
from cognitive.belief_engine.engine import BeliefEngine, TIMEOUT, HIGH_LATENCY

class RaftNode:
    def __init__(self, node_id: str, peers: List[str], config: Dict[str, Any]) -> None:
        self.node_id = str(node_id)
        self.peers = [str(p) for p in peers]
        self.config = config
        
        # Timing parameters
        self.election_timeout_min: float = config.get("raft", {}).get("election_timeout_min", 0.150)
        self.election_timeout_max: float = config.get("raft", {}).get("election_timeout_max", 0.300)
        self.heartbeat_interval: float = config.get("raft", {}).get("heartbeat_interval", 0.050)

        # Persistent state on all nodes
        self.current_term: int = 0
        self.voted_for: Optional[str] = None
        
        # Volatile state on all nodes
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.state: str = "follower"  # follower, candidate, leader
        self.current_leader: Optional[str] = None
        
        # Timers and election state
        self.last_heartbeat_time: float = time.time()
        self.election_timeout: float = random.uniform(self.election_timeout_min, self.election_timeout_max)
        self.votes: Set[str] = set()
        
        # Volatile state on leaders
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        
        # Local log
        self.log: RaftLog = RaftLog()
        
        # Dirty flag to notify storage layer
        self.storage_dirty: bool = False

        # Belief engine (POMDP-lite)
        self.belief_engine: BeliefEngine = BeliefEngine(self.peers)
        self.last_leader_belief_update: float = 0.0

    def load_persistent_state(self, state_data: Dict[str, Any], log_data: Dict[str, Any]) -> None:
        self.current_term = state_data.get("current_term", 0)
        self.voted_for = state_data.get("voted_for", None)
        self.commit_index = state_data.get("commit_index", 0)
        self.last_applied = state_data.get("last_applied", 0)
        
        self.log.load_from_dict(log_data)
        
        self.commit_index = max(self.commit_index, self.log.last_snapshot_index)
        self.last_applied = max(self.last_applied, self.log.last_snapshot_index)

    def get_persistent_state_dict(self) -> Dict[str, Any]:
        return {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied
        }

    def get_log_dict(self) -> Dict[str, Any]:
        return self.log.to_dict()

    def reset_election_timer(self) -> None:
        self.last_heartbeat_time = time.time()
        bias = 1.0
        if self.current_leader:
            bias = self.belief_engine.get_election_timeout_bias(self.current_leader)
        # Apply POMDP leader status belief: if we suspect the leader has crashed/slowed,
        # we shrink our election timeout so that we trigger a reelection much faster!
        timeout_min = self.election_timeout_min * bias
        timeout_max = self.election_timeout_max * bias
        self.election_timeout = random.uniform(timeout_min, timeout_max)

    def tick(self) -> Optional[Dict[str, Any]]:
        """
        Increments election timer. Returns election RequestVote payload if candidate triggers.
        """
        if self.state == "leader":
            return None

        # Check for missing heartbeats from the active leader to update belief state
        if self.current_leader:
            elapsed = time.time() - self.last_heartbeat_time
            now = time.time()
            if elapsed >= self.heartbeat_interval * 1.5 and (now - self.last_leader_belief_update >= self.heartbeat_interval):
                self.last_leader_belief_update = now
                obs = TIMEOUT if elapsed >= self.heartbeat_interval * 3.0 else HIGH_LATENCY
                if self.current_leader in self.belief_engine.beliefs:
                    self.belief_engine.update_belief(self.current_leader, obs)

        # Check timeout with belief engine bias update
        if time.time() - self.last_heartbeat_time >= self.election_timeout:
            return self.start_election()
        return None

    def start_election(self) -> Dict[str, Any]:
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes = {self.node_id}
        self.reset_election_timer()
        self.storage_dirty = True
        
        return {
            "type": "RequestVote",
            "term": self.current_term,
            "candidate_id": self.node_id,
            "last_log_index": self.log.last_log_index(),
            "last_log_term": self.log.last_log_term()
        }

    def handle_request_vote(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        term = msg.get("term", 0)
        candidate_id = msg.get("candidate_id")
        last_log_index = msg.get("last_log_index", 0)
        last_log_term = msg.get("last_log_term", 0)

        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        if term > self.current_term:
            self.current_term = term
            self.state = "follower"
            self.voted_for = None
            self.storage_dirty = True

        my_last_term = self.log.last_log_term()
        my_last_index = self.log.last_log_index()

        log_ok = False
        if last_log_term > my_last_term:
            log_ok = True
        elif last_log_term == my_last_term:
            if last_log_index >= my_last_index:
                log_ok = True

        granted = False
        candidate_trust = 1.0
        if candidate_id in self.belief_engine.beliefs:
            candidate_trust = self.belief_engine.beliefs[candidate_id][0] # P(HEALTHY)

        if (self.voted_for is None or self.voted_for == candidate_id) and log_ok and (candidate_trust >= 0.25):
            self.voted_for = candidate_id
            self.reset_election_timer()
            self.storage_dirty = True
            granted = True

        return {"term": self.current_term, "vote_granted": granted}

    def handle_append_entries(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        term = msg.get("term", 0)
        leader_id = msg.get("leader_id")
        prev_log_index = msg.get("prev_log_index", 0)
        prev_log_term = msg.get("prev_log_term", 0)
        entries_dict = msg.get("entries", [])
        leader_commit = msg.get("leader_commit", 0)

        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        if term > self.current_term or (term == self.current_term and self.state == "candidate"):
            self.current_term = term
            self.state = "follower"
            self.voted_for = None
            self.storage_dirty = True

        self.current_leader = leader_id
        self.reset_election_timer()

        # Check prev log alignment
        if prev_log_index == self.log.last_snapshot_index:
            if prev_log_term != self.log.last_snapshot_term:
                return {"term": self.current_term, "success": False}
        elif prev_log_index > 0:
            entry = self.log.get_entry(prev_log_index)
            if entry is None or entry.term != prev_log_term:
                return {"term": self.current_term, "success": False}

        # Conflict verification & log insertions
        for e_dict in entries_dict:
            idx = e_dict["index"]
            existing = self.log.get_entry(idx)
            if existing:
                if existing.term != e_dict["term"]:
                    self.log.truncate_from(idx)
                    self.log.append(e_dict["term"], e_dict["command"])
                    self.storage_dirty = True
            else:
                if idx == self.log.last_log_index() + 1:
                    self.log.append(e_dict["term"], e_dict["command"])
                    self.storage_dirty = True

        # Commit index updates
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.log.last_log_index())
            self.storage_dirty = True

        return {"term": self.current_term, "success": True, "match_index": self.log.last_log_index()}

    def handle_install_snapshot(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        term = msg.get("term", 0)
        leader_id = msg.get("leader_id")
        last_included_index = msg.get("last_included_index", 0)
        last_included_term = msg.get("last_included_term", 0)

        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        if term > self.current_term or (term == self.current_term and self.state == "candidate"):
            self.current_term = term
            self.state = "follower"
            self.voted_for = None
            self.storage_dirty = True

        self.current_leader = leader_id
        self.reset_election_timer()

        # Update log structure
        self.log.compact(last_included_index, last_included_term)
        self.commit_index = max(self.commit_index, last_included_index)
        self.last_applied = max(self.last_applied, last_included_index)
        self.storage_dirty = True

        return {"term": self.current_term, "success": True}

    def handle_rpc_response(self, peer_id: str, request_msg: Dict[str, Any], response_msg: Optional[Dict[str, Any]]) -> None:
        peer_id = str(peer_id)
        if not response_msg:
            return
        
        resp_term = response_msg.get("term", 0)
        if resp_term > self.current_term:
            self.current_term = resp_term
            self.state = "follower"
            self.voted_for = None
            self.storage_dirty = True
            return

        req_type = request_msg.get("type")

        if self.state == "candidate" and req_type == "RequestVote":
            if response_msg.get("vote_granted"):
                self.votes.add(peer_id)
                needed = (len(self.peers) + 1) // 2 + 1
                if len(self.votes) >= needed:
                    self.state = "leader"
                    self.current_leader = self.node_id
                    print(f"\n>>>> [RaftNode {self.node_id}] Elected LEADER for term {self.current_term}! <<<<\n")
                    
                    last_idx = self.log.last_log_index()
                    for p in self.peers:
                        self.next_index[p] = last_idx + 1
                        self.match_index[p] = 0

        elif self.state == "leader" and req_type == "AppendEntries":
            if response_msg.get("success"):
                match_idx = response_msg.get("match_index", 0)
                self.next_index[peer_id] = match_idx + 1
                self.match_index[peer_id] = match_idx
                self.update_leader_commit_index()
            else:
                self.next_index[peer_id] = max(1, self.next_index[peer_id] - 1)

        elif self.state == "leader" and req_type == "InstallSnapshot":
            if response_msg.get("success"):
                self.next_index[peer_id] = self.log.last_snapshot_index + 1
                self.match_index[peer_id] = self.log.last_snapshot_index

    def update_leader_commit_index(self) -> None:
        matches = [self.log.last_log_index()]
        for p in self.peers:
            matches.append(self.match_index.get(p, 0))
        matches.sort()
        majority_idx = matches[len(matches) // 2]
        
        if majority_idx > self.commit_index:
            if majority_idx == self.log.last_snapshot_index:
                self.commit_index = majority_idx
                self.storage_dirty = True
            else:
                entry = self.log.get_entry(majority_idx)
                if entry and entry.term == self.current_term:
                    self.commit_index = majority_idx
                    self.storage_dirty = True

    def propose(self, command: Any) -> bool:
        if self.state != "leader":
            return False
        self.log.append(self.current_term, command)
        self.storage_dirty = True
        return True

    def get_peer_replication_payload(self, peer_id: str) -> Dict[str, Any]:
        peer_id = str(peer_id)
        next_idx = self.next_index.get(peer_id, 1)
        prev_idx = next_idx - 1

        if prev_idx < self.log.last_snapshot_index:
            return {
                "type": "InstallSnapshot",
                "term": self.current_term,
                "leader_id": self.node_id,
                "last_included_index": self.log.last_snapshot_index,
                "last_included_term": self.log.last_snapshot_term
            }
        
        prev_term = self.log.last_snapshot_term
        if prev_idx > self.log.last_snapshot_index:
            entry = self.log.get_entry(prev_idx)
            prev_term = entry.term if entry else self.log.last_snapshot_term

        entries_sliced = self.log.slice_from(next_idx)
        entries_dict = [
            {"term": e.term, "index": e.index, "command": e.command} for e in entries_sliced
        ]
        
        return {
            "type": "AppendEntries",
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_idx,
            "prev_log_term": prev_term,
            "entries": entries_dict,
            "leader_commit": self.commit_index
        }
