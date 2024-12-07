import hashlib
import bisect

class DynamicHashRing:
    def __init__(self, replicas=3):
        self.replicas = replicas  
        self.ring = {}           
        self.sorted_keys = []    

    def _hash(self, key):
        """Generate a hash for a given key."""
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def add_worker(self, worker):
        """Add a worker and its replicas to the hash ring."""
        for i in range(self.replicas):
            replica_key = f"{worker}:{i}"
            hashed_key = self._hash(replica_key)
            self.ring[hashed_key] = worker
            bisect.insort(self.sorted_keys, hashed_key)

    def remove_worker(self, worker):
        """Remove a worker and its replicas from the hash ring."""
        for i in range(self.replicas):
            replica_key = f"{worker}:{i}"
            hashed_key = self._hash(replica_key)
            if hashed_key in self.ring:
                del self.ring[hashed_key]
                self.sorted_keys.remove(hashed_key)

    def get_workers(self, key, num_replicas=1):
        """
        Determine the primary and replica workers for a given key.
        """
        if not self.ring:
            raise Exception("No workers available")

        hashed_key = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, hashed_key)

        # Find the primary and replicas
        workers = []
        for _ in range(num_replicas):
            if idx == len(self.sorted_keys):
                idx = 0
            worker = self.ring[self.sorted_keys[idx]]
            if worker not in workers:
                workers.append(worker)
            idx += 1

        return workers