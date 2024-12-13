import hashlib
import bisect

class DynamicHashRing:
    def __init__(self):
        self.ring = {}           
        self.sorted_keys = []    
        self.workers = set()

    def _hash(self, key):
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def add_worker(self, worker):
        self.workers.add(worker)
        for i in range(3):  # 3 virtual nodes per worker
            replica_key = f"{worker}:{i}"
            hashed_key = self._hash(replica_key)
            self.ring[hashed_key] = worker
            bisect.insort(self.sorted_keys, hashed_key)

    def remove_worker(self, worker):
        if worker in self.workers:
            self.workers.remove(worker)
            to_remove = []
            for key in self.ring:
                if self.ring[key] == worker:
                    to_remove.append(key)
            for key in to_remove:
                self.sorted_keys.remove(key)
                del self.ring[key]

    def get_primary(self, key):
        if not self.workers:
            raise Exception("No workers available")
        hashed_key = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, hashed_key)
        if idx == len(self.sorted_keys):
            idx = 0
        return self.ring[self.sorted_keys[idx]]