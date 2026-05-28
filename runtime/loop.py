import asyncio

class ControlLoop:
    def __init__(self, node, reconciler, ai_controller, controller_manager=None, check_interval=2.0):
        self.node = node
        self.reconciler = reconciler
        self.ai = ai_controller
        self.manager = controller_manager
        self.check_interval = check_interval
        self.running = False

    async def start(self):
        """Starts the control loop as an asynchronous background task."""
        self.running = True
        asyncio.create_task(self.run_loop())

    def stop(self):
        """Stops the control loop."""
        self.running = False

    async def run_loop(self):
        while self.running:
            try:
                await asyncio.sleep(self.check_interval)
                
                # Check leadership and align controller states
                if self.manager:
                    active = self.manager.check_leadership_and_align()
                    if not active:
                        continue
                elif self.node.state != "leader":
                    continue

                # Host node is the active leader: retrieve latest committed actual state
                actual_state = self.node.actual_state
                desired_spec = actual_state.get("desired_state", {
                    "tasks": [],
                    "priority": "normal",
                    "replicas": 3
                })

                # Trigger bounded AI update
                try:
                    new_spec = self.ai.evaluate_and_update(actual_state, desired_spec)
                    self.node.propose({
                        "type": "DESIRED_STATE_UPDATE",
                        "payload": new_spec
                    })
                except Exception as e:
                    print(f"[ControlLoop] AI generation check failed: {e}")

                # Trigger reconciler task check
                self.reconciler.reconcile(desired_spec, actual_state)

            except Exception as e:
                print(f"[ControlLoop] Loop exception encountered: {e}")
