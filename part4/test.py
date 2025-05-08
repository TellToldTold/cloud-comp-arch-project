from cluster_manager import setup_cluster, deploy_memcached, get_memcached_ip
from mcperf_manager import setup_mcperf_agents, start_load_agent, preload, run_mcperf_load, stop_mcperf_agents
import os
if __name__ == "__main__":
    # Define the cluster name and state store
    cluster_name = "part4.k8s.local"
    state_store = "gs://cca-eth-2025-group-092-fbaldin/"
    cluster_config_yaml = "part4.yaml"
    ssh_key_path = os.path.expanduser("~/.ssh/cloud-computing")

    # Setup the cluster
    # setup_cluster(state_store, cluster_config_yaml)
    
    # # Deploy memcached
    # memcached_ip = deploy_memcached(
    #     memory_limit=1024,
    #     thread_count=4,
    # )

    memcached_ip = get_memcached_ip(ssh_key_path)

    # # Setup mcperf agents
    clients_info = setup_mcperf_agents()

    preload(clients_info, memcached_ip)

    start_load_agent(clients_info)

    run_mcperf_load(clients_info, memcached_ip, "./test")

    # stop_mcperf_agents()