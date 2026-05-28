import os
import yaml
from typing import Dict, Any, Union

class ConfigError(ValueError):
    """Raised when there is a configuration type check or validation error."""
    pass

class NodeConfig:
    def __init__(self, host: str, raft_port: int, api_port: int) -> None:
        self.host = host
        self.raft_port = raft_port
        self.api_port = api_port
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.host, str) or not self.host:
            raise ConfigError("node config 'host' must be a non-empty string")
        if not isinstance(self.raft_port, int) or not (1024 <= self.raft_port <= 65535):
            raise ConfigError(f"node config 'raft_port' must be an integer between 1024 and 65535, got {self.raft_port}")
        if not isinstance(self.api_port, int) or not (1024 <= self.api_port <= 65535):
            raise ConfigError(f"node config 'api_port' must be an integer between 1024 and 65535, got {self.api_port}")

class RaftConfig:
    def __init__(self, election_timeout_min: float, election_timeout_max: float, heartbeat_interval: float) -> None:
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval
        self.validate()

    def validate(self) -> None:
        # Validate types
        for k in ["election_timeout_min", "election_timeout_max", "heartbeat_interval"]:
            val = getattr(self, k)
            if not isinstance(val, (int, float)):
                raise ConfigError(f"Raft parameter '{k}' must be float/int, got {type(val)}")
        
        # Safe configurable limits: Election timeout 0.8 - 1.5 seconds, Heartbeat interval 0.15 - 0.3 seconds
        if self.election_timeout_min < 0.3:
            raise ConfigError("Raft 'election_timeout_min' must be at least 0.3 seconds (300ms)")
        if self.election_timeout_min >= self.election_timeout_max:
            raise ConfigError("Raft 'election_timeout_min' must be strictly less than 'election_timeout_max'")
        if self.heartbeat_interval <= 0.0:
            raise ConfigError("Raft 'heartbeat_interval' must be positive")
        if self.heartbeat_interval >= self.election_timeout_min:
            raise ConfigError("Raft 'heartbeat_interval' must be strictly less than 'election_timeout_min'")

class CognitiveConfig:
    def __init__(self, decay_rate: float, latency_threshold_slow: float, latency_threshold_dead: float) -> None:
        self.decay_rate = decay_rate
        self.latency_threshold_slow = latency_threshold_slow
        self.latency_threshold_dead = latency_threshold_dead
        self.validate()

    def validate(self) -> None:
        for k in ["decay_rate", "latency_threshold_slow", "latency_threshold_dead"]:
            val = getattr(self, k)
            if not isinstance(val, (int, float)):
                raise ConfigError(f"Cognitive parameter '{k}' must be float/int, got {type(val)}")
        
        if not (0.0 <= self.decay_rate <= 1.0):
            raise ConfigError("Cognitive 'decay_rate' must be between 0.0 and 1.0")
        if self.latency_threshold_slow <= 0.0:
            raise ConfigError("Cognitive 'latency_threshold_slow' must be positive")
        if self.latency_threshold_slow >= self.latency_threshold_dead:
            raise ConfigError("Cognitive 'latency_threshold_slow' must be less than 'latency_threshold_dead'")

class ClusterConfig:
    def __init__(
        self,
        nodes: Dict[str, NodeConfig],
        raft: RaftConfig,
        cognitive: CognitiveConfig,
        logging_level: str
    ) -> None:
        self.nodes = nodes
        self.raft = raft
        self.cognitive = cognitive
        self.logging_level = logging_level
        self.validate()

    def validate(self) -> None:
        if not self.nodes:
            raise ConfigError("ClusterConfig must define at least one node")
        if not isinstance(self.logging_level, str) or self.logging_level not in ["DEBUG", "INFO", "WARN", "WARNING", "ERROR"]:
            raise ConfigError(f"logging level must be a standard severity level string, got {self.logging_level}")

def load_and_validate_config(filepath: str) -> ClusterConfig:
    """
    Loads YAML config from path, overrides values with environment variables,
    performs strong schema validation, and returns ClusterConfig.
    """
    if not os.path.exists(filepath):
        raise ConfigError(f"Configuration file not found at: {filepath}")

    with open(filepath, "r") as f:
        try:
            data = yaml.safe_load(f)
        except Exception as e:
            raise ConfigError(f"Failed to parse YAML configuration: {e}")

    if not isinstance(data, dict):
        raise ConfigError("YAML configuration root must be a dictionary")

    # Detect if this is a legacy peers map and reconstruct the full dictionary structure
    if "cluster" not in data:
        is_peers_map = True
        for key, val in data.items():
            if not isinstance(val, dict) or ("raft_port" not in val and "host" not in val):
                is_peers_map = False
                break
        if is_peers_map:
            data = {
                "cluster": {
                    "nodes": data
                },
                "raft": {
                    "election_timeout_min": 0.800,
                    "election_timeout_max": 1.500,
                    "heartbeat_interval": 0.200
                },
                "cognitive": {
                    "decay_rate": 0.900,
                    "latency_threshold_slow": 0.050,
                    "latency_threshold_dead": 0.250
                },
                "logging": {
                    "level": "INFO"
                }
            }

    # 1. Environment variable override support
    # e.g., RAFT_ELECTION_TIMEOUT_MIN overrides raft.election_timeout_min
    raft_data = data.setdefault("raft", {})
    raft_data["election_timeout_min"] = float(os.environ.get("RAFT_ELECTION_TIMEOUT_MIN", raft_data.get("election_timeout_min", 0.800)))
    raft_data["election_timeout_max"] = float(os.environ.get("RAFT_ELECTION_TIMEOUT_MAX", raft_data.get("election_timeout_max", 1.500)))
    raft_data["heartbeat_interval"] = float(os.environ.get("RAFT_HEARTBEAT_INTERVAL", raft_data.get("heartbeat_interval", 0.200)))

    cog_data = data.setdefault("cognitive", {})
    cog_data["decay_rate"] = float(os.environ.get("COGNITIVE_DECAY_RATE", cog_data.get("decay_rate", 0.900)))
    cog_data["latency_threshold_slow"] = float(os.environ.get("COGNITIVE_LATENCY_THRESHOLD_SLOW", cog_data.get("latency_threshold_slow", 0.050)))
    cog_data["latency_threshold_dead"] = float(os.environ.get("COGNITIVE_LATENCY_THRESHOLD_DEAD", cog_data.get("latency_threshold_dead", 0.250)))

    logging_data = data.setdefault("logging", {})
    logging_level = os.environ.get("LOGGING_LEVEL", logging_data.setdefault("level", "INFO"))

    cluster_data = data.setdefault("cluster", {})
    nodes_raw = cluster_data.setdefault("nodes", {})
    if not isinstance(nodes_raw, dict):
        raise ConfigError("cluster.nodes must be a dictionary")

    nodes: Dict[str, NodeConfig] = {}
    for node_id, node_dict in nodes_raw.items():
        if not isinstance(node_dict, dict):
            raise ConfigError(f"Node definition for '{node_id}' must be a dictionary")
        
        # Fallback environment overrides per node (e.g. NODE_1_RAFT_PORT)
        host = os.environ.get(f"NODE_{node_id}_HOST", node_dict.get("host", "127.0.0.1"))
        raft_port = int(os.environ.get(f"NODE_{node_id}_RAFT_PORT", node_dict.get("raft_port", 5000 + int(node_id))))
        api_port = int(os.environ.get(f"NODE_{node_id}_API_PORT", node_dict.get("api_port", 6000 + int(node_id))))

        nodes[str(node_id)] = NodeConfig(host=host, raft_port=raft_port, api_port=api_port)

    # 2. Build Typed Objects with Schema Validation
    raft_cfg = RaftConfig(
        election_timeout_min=raft_data["election_timeout_min"],
        election_timeout_max=raft_data["election_timeout_max"],
        heartbeat_interval=raft_data["heartbeat_interval"]
    )

    cog_cfg = CognitiveConfig(
        decay_rate=cog_data["decay_rate"],
        latency_threshold_slow=cog_data["latency_threshold_slow"],
        latency_threshold_dead=cog_data["latency_threshold_dead"]
    )

    return ClusterConfig(
        nodes=nodes,
        raft=raft_cfg,
        cognitive=cog_cfg,
        logging_level=logging_level
    )
