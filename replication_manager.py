import logging
from typing import Dict, Set, List, Tuple
from dynamic_hash_ring import DynamicHashRing

class ReplicationManager:
    def __init__(self):
        """Initialize replication manager with hash ring and data tracking."""
        self.hash_ring = DynamicHashRing()
        self.data_locations = {}  
        self.deleted_lists = set()  
        self.logger = logging.getLogger('replication_manager')

    def get_list_locations(self, list_id: str) -> Set[str]:
        """Get actual workers that have this list"""
        return self.data_locations.get(list_id, set())

    def record_list_location(self, list_id: str, worker_addr: str):
        """Record that a worker has this list"""
        if list_id not in self.data_locations:
            self.data_locations[list_id] = set()
        self.data_locations[list_id].add(worker_addr)

    def remove_list_location(self, list_id: str, worker_addr: str):
        """Remove record of list from a worker"""
        if list_id in self.data_locations:
            self.data_locations[list_id].discard(worker_addr)
            if not self.data_locations[list_id]:  
                del self.data_locations[list_id]

    def record_deletion(self, list_id: str):
        """Record that a list has been deleted"""
        self.deleted_lists.add(list_id)
        
        if list_id in self.data_locations:
            del self.data_locations[list_id]

    def get_data_placement(self, data_key: str) -> Tuple[str, List[str]]:
        """
        Determine primary and replica workers for a piece of data.
        Returns (primary_worker, [replica_workers])
        
        For N workers:
        - 1 worker:  (primary, [])
        - 2 workers: (primary, [replica1])
        - 3+ workers: (primary, [replica1, replica2])
        """
        
        if data_key in self.deleted_lists:
            return None, []

        if not self.hash_ring.workers:
            raise Exception("No workers available")

        # Get primary using consistent hashing
        primary = self.hash_ring.get_primary(data_key)
        replicas = []

        # Add replicas based on available workers
        worker_count = len(self.hash_ring.workers)
        if worker_count > 1:
            
            available_workers = list(self.hash_ring.workers - {primary})
            
            
            max_replicas = min(len(available_workers), 2)
            replicas = available_workers[:max_replicas]

        self.logger.info(
            f"Data placement for {data_key}: "
            f"primary={primary}, replicas={replicas}"
        )
        return primary, replicas

    def handle_new_worker(self, worker: str) -> Dict[str, Tuple[str, str]]:
        """
        Handle a new worker joining the system.
        Returns dict of data that needs replication:
        {
            data_key: (source_worker, target_worker)
        }
        """
        if worker in self.hash_ring.workers:
            return {}

        self.logger.info(f"Handling new worker: {worker}")
        
        # Add to hash ring
        self.hash_ring.add_worker(worker)
        replication_needed = {}

        # Check existing data 
        for data_key, current_locations in self.data_locations.items():
            if data_key not in self.deleted_lists:
                primary, replicas = self.get_data_placement(data_key)
                
                
                if primary and worker in [primary] + replicas:
                    
                    current_copy_count = len(current_locations)
                    if current_copy_count < 3 and worker not in current_locations:
                        
                        if current_locations:
                            source = next(iter(current_locations))
                            replication_needed[data_key] = (source, worker)
                            self.logger.info(
                                f"Will replicate {data_key} from {source} to {worker}"
                            )

        return replication_needed

    def handle_worker_removal(self, worker: str) -> Dict[str, Set[str]]:
        """
        Handle a worker leaving the system.
        Returns dict of affected data:
        {
            data_key: remaining_locations
        }
        """
        if worker not in self.hash_ring.workers:
            return {}

        self.logger.info(f"Handling worker removal: {worker}")
        
        
        affected_data = {}
        for data_key, locations in self.data_locations.items():
            if worker in locations:
                locations.remove(worker)
                if locations:  
                    affected_data[data_key] = locations.copy()
                elif data_key not in self.deleted_lists:  
                    self.logger.warning(f"Lost all copies of data {data_key}")

        
        self.hash_ring.remove_worker(worker)

        #
        for data_key, locations in list(self.data_locations.items()):
            if not locations:
                del self.data_locations[data_key]

        return affected_data

    def get_replication_status(self, data_key: str) -> Dict:
        """
        Get current replication status for a piece of data.
        Returns:
        {
            'desired_primary': str,
            'desired_replicas': List[str],
            'current_locations': List[str],
            'replication_complete': bool,
            'is_deleted': bool
        }
        """
        current_locations = self.get_list_locations(data_key)
        is_deleted = data_key in self.deleted_lists

        try:
            desired_primary, desired_replicas = self.get_data_placement(data_key)
        except Exception:
            return {
                'desired_primary': None,
                'desired_replicas': [],
                'current_locations': list(current_locations),
                'replication_complete': False,
                'is_deleted': is_deleted
            }

        return {
            'desired_primary': desired_primary,
            'desired_replicas': desired_replicas,
            'current_locations': list(current_locations),
            'replication_complete': (
                desired_primary in current_locations and
                all(r in current_locations for r in desired_replicas)
            ),
            'is_deleted': is_deleted
        }

    def needs_replication(self, data_key: str) -> bool:
        """Check if a piece of data needs additional replication."""
        if data_key in self.deleted_lists:
            return False
        status = self.get_replication_status(data_key)
        return not status['replication_complete']

    def get_all_workers(self) -> List[str]:
        """Get list of all active workers."""
        return list(self.hash_ring.workers)

    def get_worker_count(self) -> int:
        """Get number of active workers."""
        return len(self.hash_ring.workers)

    def is_deleted(self, list_id: str) -> bool:
        """Check if a list has been marked as deleted."""
        return list_id in self.deleted_lists

    