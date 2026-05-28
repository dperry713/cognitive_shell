def handle_request_vote(node, term, candidate_id, last_log_index, last_log_term):
    """
    Raft RequestVote RPC handler.
    Returns (term, vote_granted).
    """
    # 1. Reply false if term < current_term
    if term < node.current_term:
        return node.current_term, False

    # If term > current_term, update current_term and become follower
    if term > node.current_term:
        node.current_term = term
        node.state = "follower"
        node.voted_for = None
        node.save_state()  # Persist state changes if desired

    # 2. If voted_for is null or candidate_id, and candidate’s log is at least
    # as up-to-date as receiver’s log, grant vote
    my_last_term = node.log.last_log_term()
    my_last_index = node.log.last_log_index()

    # Determine log freshness
    log_ok = False
    if last_log_term > my_last_term:
        log_ok = True
    elif last_log_term == my_last_term:
        if last_log_index >= my_last_index:
            log_ok = True

    if (node.voted_for is None or node.voted_for == candidate_id) and log_ok:
        node.voted_for = candidate_id
        node.save_state()
        return node.current_term, True

    return node.current_term, False


def handle_append_entries(node, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
    """
    Raft AppendEntries RPC handler.
    Returns (term, success).
    """
    # 1. Reply false if term < current_term
    if term < node.current_term:
        return node.current_term, False

    # If term > current_term or we are a candidate in the same term, become follower
    if term > node.current_term or (term == node.current_term and node.state == "candidate"):
        node.current_term = term
        node.state = "follower"
        node.voted_for = None
        node.save_state()

    # We recognized a valid leader, update current leader ID
    node.current_leader = leader_id
    node.reset_election_timer()

    # 2. Reply false if log doesn’t contain an entry at prev_log_index matching prev_log_term
    # Check if prev_log_index falls in the compacted snapshot
    if prev_log_index == node.log.last_snapshot_index:
        if prev_log_term != node.log.last_snapshot_term:
            return node.current_term, False
    elif prev_log_index > 0:
        entry = node.log.get_entry(prev_log_index)
        if entry is None or entry["term"] != prev_log_term:
            return node.current_term, False

    # 3. If an existing entry conflicts with a new one (same index but different terms),
    # delete the existing entry and all that follow it
    for entry in entries:
        idx = entry["index"]
        existing = node.log.get_entry(idx)
        if existing:
            if existing["term"] != entry["term"]:
                # Conflict: truncate from this index onwards
                node.log.truncate_from(idx)
                # Append this entry and any following it
                node.log.append(entry["term"], entry["command"])
        else:
            # Entry doesn't exist, append it.
            # Safety check: ensure we append it at the correct index
            if idx == node.log.last_log_index() + 1:
                node.log.append(entry["term"], entry["command"])

    # 4. If leader_commit > commit_index, set commit_index = min(leader_commit, index of last new entry)
    if leader_commit > node.commit_index:
        node.commit_index = min(leader_commit, node.log.last_log_index())
        # Apply newly committed entries to the state machine
        node.apply_committed()

    return node.current_term, True


def handle_install_snapshot(node, term, leader_id, last_included_index, last_included_term, data):
    """
    Raft InstallSnapshot RPC handler.
    Returns (term, success).
    """
    # 1. Reply false if term < current_term
    if term < node.current_term:
        return node.current_term, False

    if term > node.current_term or (term == node.current_term and node.state == "candidate"):
        node.current_term = term
        node.state = "follower"
        node.voted_for = None
        node.save_state()

    node.current_leader = leader_id
    node.reset_election_timer()

    # Save snapshot metadata and state data
    node.snapshot_state = data
    node.snapshot_index = last_included_index
    node.snapshot_term = last_included_term

    # Compact the local log
    node.log.compact(last_included_index, last_included_term)

    # 7. Reset state machine using snapshot contents
    # Update commit and apply indices
    node.commit_index = max(node.commit_index, last_included_index)
    node.last_applied = max(node.last_applied, last_included_index)
    
    # Reload/Rebuild the state engine using the snapshot state
    node.rebuild_state_from_snapshot()

    return node.current_term, True
