import time

class ControlLoop:
    def __init__(self, ai, reconciler, state_engine, desired_state, log):

        self.ai = ai
        self.reconciler = reconciler
        self.state_engine = state_engine
        self.desired_state = desired_state
        self.log = log

    def run(self):

        while True:

            committed = self.log.get_committed()
            actual_state = self.state_engine(committed)

            # AI MODIFIES DESIRED STATE
            new_spec = self.ai.evaluate_and_update(
                actual_state,
                self.desired_state
            )

            self.desired_state.update(new_spec)

            # RECONCILIATION LOOP
            self.reconciler.reconcile(
                self.desired_state,
                actual_state
            )

            time.sleep(2)
