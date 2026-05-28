class AIEvaluator:
    def __init__(self):
        pass

    def evaluate_performance(self, actual_state):
        """
        Evaluates task execution status and returns system control recommendations.
        """
        recommendations = {}
        total_tasks = 0
        failed_tasks = 0
        
        for key, value in actual_state.items():
            if key == "desired_state":
                continue
            
            # Check for failure flags in recorded task states
            if isinstance(value, dict):
                result = value.get("result", {})
                if "error" in result or result.get("returncode", 0) != 0:
                    failed_tasks += 1
                total_tasks += 1
            elif value in ["missing", "failed"]:
                failed_tasks += 1
                total_tasks += 1
                    
        if total_tasks > 0:
            failure_rate = failed_tasks / total_tasks
            if failure_rate > 0.3:
                # If failure rate is above 30%, elevate priority and request retries
                recommendations["priority"] = "high"
                recommendations["suggest_retry"] = True
                
        return recommendations
