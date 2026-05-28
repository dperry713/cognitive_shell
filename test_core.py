import unittest
import json
from raft.log import RaftLog
from raft.consensus import handle_request_vote, handle_append_entries, handle_install_snapshot
from log.state_engine import reduce_log
from control_plane.reconciler import Reconciler

class DummyNode:
    def __init__(self):
        self.current_term = 0
        self.voted_for = None
        self.state = "follower"
        self.log = RaftLog()
        self.commit_index = 0
        self.last_applied = 0
        self.actual_state = {}
        self.snapshot_state = {}
        self.snapshot_index = 0
        self.snapshot_term = 0
        self.applied_entries = []
        self.saved = False

    def save_state(self):
        self.saved = True

    def apply_committed(self):
        while self.last_applied < self.commit_index:
            next_to_apply = self.last_applied + 1
            entry = self.log.get_entry(next_to_apply)
            if entry:
                self.applied_entries.append(entry)
                self.actual_state = reduce_log(self.actual_state, [entry])
                self.last_applied = next_to_apply
            else:
                break

    def rebuild_state_from_snapshot(self):
        self.actual_state = json.loads(json.dumps(self.snapshot_state))
        entries = self.log.slice_from(self.log.last_snapshot_index + 1)
        committed = [e for e in entries if e["index"] <= self.commit_index]
        self.actual_state = reduce_log(self.actual_state, committed)

    def reset_election_timer(self):
        pass


class TestRaftLog(unittest.TestCase):
    def test_log_indexing_and_compaction(self):
        log = RaftLog()
        self.assertEqual(log.last_log_index(), 0)
        self.assertEqual(log.last_log_term(), 0)

        # Append entries
        log.append(1, "cmd1")
        log.append(1, "cmd2")
        log.append(2, "cmd3")

        self.assertEqual(log.last_log_index(), 3)
        self.assertEqual(log.last_log_term(), 2)
        self.assertEqual(log.get_entry(2)["command"], "cmd2")

        # Compact up to index 2
        success = log.compact(2, 1)
        self.assertTrue(success)
        self.assertEqual(log.last_snapshot_index, 2)
        self.assertEqual(log.last_snapshot_term, 1)
        
        # Check that index 1 and 2 are compacted and return None
        self.assertIsNone(log.get_entry(1))
        self.assertIsNone(log.get_entry(2))
        
        # Check index 3 is still accessible
        self.assertEqual(log.get_entry(3)["command"], "cmd3")
        self.assertEqual(log.last_log_index(), 3)
        self.assertEqual(log.last_log_term(), 2)

    def test_log_truncation(self):
        log = RaftLog()
        log.append(1, "cmd1")
        log.append(1, "cmd2")
        log.append(2, "cmd3")

        log.truncate_from(2)
        self.assertEqual(log.last_log_index(), 1)
        self.assertEqual(log.get_entry(1)["command"], "cmd1")
        self.assertIsNone(log.get_entry(2))


class TestStateEngine(unittest.TestCase):
    def test_pure_reduction(self):
        entries = [
            {"command": {"type": "DESIRED_STATE_UPDATE", "payload": {"tasks": []}}},
            {"command": {"type": "TASK_RUNNING", "payload": {"id": "t1"}}},
            {"command": {"type": "TASK_DONE", "payload": {"id": "t1", "result": "ok"}}}
        ]
        state = reduce_log(entries)
        self.assertEqual(state.get("t1"), "done")
        self.assertEqual(state.get("desired_state"), {"tasks": []})

    def test_reduction_with_snapshot(self):
        snapshot = {"t1": "running", "desired_state": {"tasks": []}}
        entries = [
            {"command": {"type": "TASK_DONE", "payload": {"id": "t1", "result": "ok"}}},
            {"command": {"type": "TASK_RUNNING", "payload": {"id": "t2"}}}
        ]
        state = reduce_log(snapshot, entries)
        self.assertEqual(state.get("t1"), "done")
        self.assertEqual(state.get("t2"), "running")


class TestConsensusRPC(unittest.TestCase):
    def test_request_vote_term_check(self):
        node = DummyNode()
        node.current_term = 2
        
        # Candidate term is lower
        term, granted = handle_request_vote(node, 1, "cand1", 0, 0)
        self.assertFalse(granted)
        self.assertEqual(term, 2)

        # Candidate term is higher, log is fresh
        term, granted = handle_request_vote(node, 3, "cand1", 0, 0)
        self.assertTrue(granted)
        self.assertEqual(node.current_term, 3)
        self.assertEqual(node.voted_for, "cand1")

    def test_append_entries_success(self):
        node = DummyNode()
        node.current_term = 1
        
        # Populate log with valid command dicts
        node.log.append(1, {"type": "TASK_RUNNING", "payload": {"id": "task-1"}}) # index 1
        
        # Successful AppendEntries (prevLogIndex=1, prevLogTerm=1)
        term, success = handle_append_entries(
            node, 1, "leader1", 1, 1,
            [{"term": 1, "index": 2, "command": {"type": "TASK_DONE", "payload": {"id": "task-1", "result": "ok"}}}],
            2
        )
        self.assertTrue(success)
        self.assertEqual(node.log.last_log_index(), 2)
        self.assertEqual(node.commit_index, 2)
        self.assertEqual(len(node.applied_entries), 2)


if __name__ == "__main__":
    unittest.main()
