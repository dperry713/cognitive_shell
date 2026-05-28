import asyncio
import json
import os
import random
import time
from raft.log import RaftLog
from raft.consensus import handle_request_vote, handle_append_entries, handle_install_snapshot
from log.state_engine import reduce_log

class RaftNode:
    def __init__(self, node_id, peers, port):
        self.node_id = str(node_id)
        self.peers = {str(k): int(v) for k, v in peers.items()}
        self.port = int(port)
        
        # Raft state variables
        self.state = "follower"  # follower, candidate, leader
        self.current_term = 0
        self.voted_for = None
        self.current_leader = None
        
        # Log and State Machine
        self.log = RaftLog()
        self.commit_index = 0
        self.last_applied = 0
        
        # Actual state reduced from committed logs
        self.actual_state = {}
        
        # Snapshot variables
        self.snapshot_state = {}
        self.snapshot_index = 0
        self.snapshot_term = 0
        
        # Leader volatile state
        self.next_index = {}
        self.match_index = {}
        
        # Timing trackers
        self.last_heartbeat_time = time.time()
        self.running = False
        self.server = None
        self.background_tasks = []
        
        # Load state if it exists on disk
        self.load_state()

    def reset_election_timer(self):
        self.last_heartbeat_time = time.time()

    def save_state(self):
        """Persist node state to disk for recovery from total wipe."""
        os.makedirs("node_data", exist_ok=True)
        filename = f"node_data/node_{self.node_id}.json"
        data = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log_entries": self.log.entries,
            "last_snapshot_index": self.log.last_snapshot_index,
            "last_snapshot_term": self.log.last_snapshot_term,
            "snapshot_state": self.snapshot_state,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied
        }
        try:
            with open(filename, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[Node {self.node_id}] Failed to save state to {filename}: {e}")

    def load_state(self):
        """Restore state from disk on reboot."""
        filename = f"node_data/node_{self.node_id}.json"
        if not os.path.exists(filename):
            return
        try:
            with open(filename, "r") as f:
                data = json.load(f)
            self.current_term = data.get("current_term", 0)
            self.voted_for = data.get("voted_for", None)
            self.log.entries = data.get("log_entries", [])
            self.log.last_snapshot_index = data.get("last_snapshot_index", 0)
            self.log.last_snapshot_term = data.get("last_snapshot_term", 0)
            self.snapshot_state = data.get("snapshot_state", {})
            self.commit_index = data.get("commit_index", 0)
            self.last_applied = data.get("last_applied", 0)
            
            # Reconstruct memory state from snapshot and logs
            self.rebuild_state_from_snapshot()
            print(f"[Node {self.node_id}] Restored state from disk. Term: {self.current_term}, Log index: {self.log.last_log_index()}, Commit index: {self.commit_index}")
        except Exception as e:
            print(f"[Node {self.node_id}] Error restoring state: {e}")

    def rebuild_state_from_snapshot(self):
        """Deterministic replay of committed log tail on top of snapshot."""
        # Deep copy snapshot
        self.actual_state = json.loads(json.dumps(self.snapshot_state))
        # Get remaining entries in log
        entries = self.log.slice_from(self.log.last_snapshot_index + 1)
        # Keep only committed ones
        committed = [e for e in entries if e["index"] <= self.commit_index]
        # Re-apply
        self.actual_state = reduce_log(self.actual_state, committed)

    def apply_committed(self):
        """Applies newly committed entries to actual state machine."""
        while self.last_applied < self.commit_index:
            next_to_apply = self.last_applied + 1
            entry = self.log.get_entry(next_to_apply)
            if entry:
                self.actual_state = reduce_log(self.actual_state, [entry])
                self.last_applied = next_to_apply
                print(f"[Node {self.node_id}] Applied log entry {next_to_apply} | Cmd: {entry['command']} | Machine State: {self.actual_state}")
                
                # Check snapshot trigger
                # We snapshot every 5 commits to test log compaction in simulations
                if self.last_applied > 0 and self.last_applied % 5 == 0:
                    self.take_snapshot()
            else:
                break
        self.save_state()

    def take_snapshot(self):
        """Prunes local logs and saves snapshot state to disk."""
        if self.last_applied <= self.log.last_snapshot_index:
            return
        entry = self.log.get_entry(self.last_applied)
        term = entry["term"] if entry else self.log.last_snapshot_term
        
        print(f"[Node {self.node_id}] Taking snapshot at index {self.last_applied}")
        self.snapshot_state = json.loads(json.dumps(self.actual_state))
        self.snapshot_index = self.last_applied
        self.snapshot_term = term
        
        self.log.compact(self.snapshot_index, self.snapshot_term)
        self.save_state()

    async def start(self):
        self.running = True
        self.reset_election_timer()
        self.server = await asyncio.start_server(self.handle_connection, '127.0.0.1', self.port)
        print(f"[Node {self.node_id}] TCP Server listening on port {self.port}")
        
        # Start loops
        self.background_tasks.append(asyncio.create_task(self.election_timer_loop()))
        self.background_tasks.append(asyncio.create_task(self.heartbeat_loop()))

    async def stop(self):
        self.running = False
        self.state = "follower"
        self.current_leader = None
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for task in self.background_tasks:
            task.cancel()
        await asyncio.gather(*self.background_tasks, return_exceptions=True)
        self.background_tasks.clear()
        print(f"[Node {self.node_id}] Stopped.")

    async def handle_connection(self, reader, writer):
        try:
            line = await reader.readline()
            if not line:
                return
            msg = json.loads(line.decode('utf-8'))
            resp = await self.process_rpc(msg)
            if resp:
                writer.write((json.dumps(resp) + "\n").encode('utf-8'))
                await writer.drain()
        except Exception as e:
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def send_rpc(self, peer_id, msg):
        peer_port = self.peers.get(peer_id)
        if not peer_port:
            return None
        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', peer_port)
            writer.write((json.dumps(msg) + "\n").encode('utf-8'))
            await writer.drain()
            
            line = await asyncio.wait_for(reader.readline(), timeout=0.1)
            writer.close()
            await writer.wait_closed()
            if line:
                return json.loads(line.decode('utf-8'))
        except Exception:
            # Peer offline/refused connection, fail silently
            pass
        return None

    async def process_rpc(self, msg):
        msg_type = msg.get("type")
        term = msg.get("term", 0)
        from_id = msg.get("from")
        payload = msg.get("payload", {})
        
        if msg_type == "RequestVote":
            term, granted = handle_request_vote(
                self, term, from_id, 
                payload.get("last_log_index"), 
                payload.get("last_log_term")
            )
            return {"term": term, "vote_granted": granted}
            
        elif msg_type == "AppendEntries":
            term, success = handle_append_entries(
                self, term, from_id,
                payload.get("prev_log_index"),
                payload.get("prev_log_term"),
                payload.get("entries", []),
                payload.get("leader_commit")
            )
            return {"term": term, "success": success, "match_index": self.log.last_log_index()}
            
        elif msg_type == "InstallSnapshot":
            term, success = handle_install_snapshot(
                self, term, from_id,
                payload.get("last_included_index"),
                payload.get("last_included_term"),
                payload.get("data")
            )
            return {"term": term, "success": success}
            
        return None

    async def election_timer_loop(self):
        while self.running:
            # Randomized timeout for this election cycle
            timeout = random.uniform(0.150, 0.300)
            cycle_start = self.last_heartbeat_time
            
            while self.running:
                try:
                    await asyncio.sleep(0.010)
                    if self.state == "leader":
                        break
                    # If last_heartbeat_time was updated, reset election timer cycle
                    if self.last_heartbeat_time > cycle_start:
                        break
                    if time.time() - cycle_start >= timeout:
                        await self.start_election()
                        break
                except asyncio.CancelledError:
                    return
                except Exception as e:
                    print(f"[Node {self.node_id}] Error in election timer: {e}")
                    break

    async def start_election(self):
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.reset_election_timer()
        self.save_state()
        
        term_at_start = self.current_term
        print(f"[Node {self.node_id}] Initiating election for term {term_at_start}...")
        
        votes = {self.node_id}
        needed = (len(self.peers) + 1) // 2 + 1
        
        # Prepare RequestVote payload
        msg = {
            "type": "RequestVote",
            "from": self.node_id,
            "term": self.current_term,
            "payload": {
                "last_log_index": self.log.last_log_index(),
                "last_log_term": self.log.last_log_term()
            }
        }
        
        async def ask_peer(peer_id):
            resp = await self.send_rpc(peer_id, msg)
            if resp:
                resp_term = resp.get("term", 0)
                if resp_term > self.current_term:
                    self.current_term = resp_term
                    self.state = "follower"
                    self.voted_for = None
                    self.save_state()
                    return False, peer_id
                if resp.get("vote_granted"):
                    return True, peer_id
            return False, peer_id

        # Run peer voting in parallel
        futures = [ask_peer(p) for p in self.peers]
        
        for future in asyncio.as_completed(futures):
            vote_granted, peer_id = await future
            # If state changed or term advanced while waiting, stop election
            if self.state != "candidate" or self.current_term != term_at_start:
                return
            if vote_granted:
                votes.add(peer_id)
                if len(votes) >= needed:
                    self.state = "leader"
                    self.current_leader = self.node_id
                    print(f"\n>>>> [Node {self.node_id}] Elected LEADER for term {self.current_term}! <<<<\n")
                    
                    # Initialize leader state
                    last_idx = self.log.last_log_index()
                    for peer in self.peers:
                        self.next_index[peer] = last_idx + 1
                        self.match_index[peer] = 0
                    
                    await self.send_heartbeats()
                    return

    async def heartbeat_loop(self):
        while self.running:
            try:
                await asyncio.sleep(0.050)  # Heartbeats/replication check every 50ms
                if self.state == "leader":
                    await self.send_heartbeats()
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[Node {self.node_id}] Error in heartbeat loop: {e}")

    async def send_heartbeats(self):
        for peer in self.peers:
            asyncio.create_task(self.replicate_to_peer(peer))

    async def replicate_to_peer(self, peer):
        next_idx = self.next_index[peer]
        prev_idx = next_idx - 1
        
        # Check if index is in the compacted snapshot
        if prev_idx < self.log.last_snapshot_index:
            # Must install snapshot
            msg = {
                "type": "InstallSnapshot",
                "from": self.node_id,
                "term": self.current_term,
                "payload": {
                    "last_included_index": self.log.last_snapshot_index,
                    "last_included_term": self.log.last_snapshot_term,
                    "data": self.snapshot_state
                }
            }
            await self.send_snapshot_to_peer(peer, msg)
        else:
            # Send regular entries
            prev_term = self.log.last_snapshot_term
            if prev_idx > self.log.last_snapshot_index:
                entry = self.log.get_entry(prev_idx)
                prev_term = entry["term"] if entry else self.log.last_snapshot_term
            
            entries = self.log.slice_from(next_idx)
            msg = {
                "type": "AppendEntries",
                "from": self.node_id,
                "term": self.current_term,
                "payload": {
                    "prev_log_index": prev_idx,
                    "prev_log_term": prev_term,
                    "entries": entries,
                    "leader_commit": self.commit_index
                }
            }
            await self.send_entries_to_peer(peer, msg, next_idx, len(entries))

    async def send_snapshot_to_peer(self, peer, msg):
        resp = await self.send_rpc(peer, msg)
        if not resp:
            return
        term = resp.get("term", 0)
        if term > self.current_term:
            self.current_term = term
            self.state = "follower"
            self.voted_for = None
            self.save_state()
            return
        
        if resp.get("success") or True:  # InstallSnapshot succeeded or we update indices
            self.next_index[peer] = self.log.last_snapshot_index + 1
            self.match_index[peer] = self.log.last_snapshot_index

    async def send_entries_to_peer(self, peer, msg, sent_next_index, sent_count):
        resp = await self.send_rpc(peer, msg)
        if not resp:
            return
        term = resp.get("term", 0)
        if term > self.current_term:
            self.current_term = term
            self.state = "follower"
            self.voted_for = None
            self.save_state()
            return
            
        if self.state != "leader":
            return
            
        if resp.get("success"):
            # Update next and match index
            self.next_index[peer] = sent_next_index + sent_count
            self.match_index[peer] = sent_next_index + sent_count - 1
            self.update_leader_commit_index()
        else:
            # Log inconsistency, decrement nextIndex and retry later
            self.next_index[peer] = max(1, self.next_index[peer] - 1)

    def update_leader_commit_index(self):
        """Finds if a majority of logs are replicated on peer match indices."""
        # Leader automatically matches its own last log index
        my_last = self.log.last_log_index()
        matches = [my_last]
        for peer in self.peers:
            matches.append(self.match_index[peer])
            
        matches.sort()
        # Find median (which represents a majority commit)
        majority_idx = matches[len(matches) // 2]
        
        if majority_idx > self.commit_index:
            # Ensure the committed index belongs to the current term (Raft term safety rule)
            if majority_idx == self.log.last_snapshot_index:
                self.commit_index = majority_idx
                self.apply_committed()
            else:
                entry = self.log.get_entry(majority_idx)
                if entry and entry["term"] == self.current_term:
                    self.commit_index = majority_idx
                    self.apply_committed()

    def propose(self, command):
        """External client proposing state transition commands."""
        if self.state != "leader":
            return False
            
        # Append to leader's log local state
        self.log.append(self.current_term, command)
        self.save_state()
        return True
