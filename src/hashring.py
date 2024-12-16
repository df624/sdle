import hashlib

class HashRing:
    def __init__(self, nodes=None, replicas=3):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key):
        return int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node):
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            self.ring[key] = node
            self.sorted_keys.append(key)
        self.sorted_keys.sort()

    def remove_node(self, node):
        for i in range(self.replicas):
            key = self._hash(f"{node}:{i}")
            del self.ring[key]
            self.sorted_keys.remove(key)

    def get_node(self, key):
        if not self.ring:
            return None
        hash_key = self._hash(key)
        for ring_key in self.sorted_keys:
            if hash_key <= ring_key:
                return self.ring[ring_key]
        return self.ring[self.sorted_keys[0]]
        	
    #return multiple replicas for a given key
    def get_nodes(self, key, count=3):
        """Return `count` nodes responsible for a given key."""
        hash_key = self._hash(key)
        result = []
        for ring_key in self.sorted_keys:
            if hash_key <= ring_key:
                result.append(self.ring[ring_key])
                if len(result) == count:
                    break
        if len(result) < count:
            result.extend(self.ring[self.sorted_keys[i]] for i in range(count - len(result)))
        return result