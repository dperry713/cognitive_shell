class ControllerManager:
    def __init__(self, node, reconciler, ai_controller, evaluator):
        self.node = node
        self.reconciler = reconciler
        self.ai = ai_controller
        self.evaluator = evaluator
        self.active = False

    def check_leadership_and_align(self):
        """
        Monitors host node state and spawns/stops manager loop modules.
        """
        is_leader = (self.node.state == "leader")
        if is_leader and not self.active:
            self.active = True
            print(f"[ControllerManager] Leader controllers activated on Node {self.node.node_id}.")
        elif not is_leader and self.active:
            self.active = False
            print(f"[ControllerManager] Leader controllers deactivated on Node {self.node.node_id}.")
            
        return self.active
