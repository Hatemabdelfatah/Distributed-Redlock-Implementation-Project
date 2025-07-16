import redis
import time
import uuid
import multiprocessing

client_processes_waiting = [1, 0, 1, 1, 4]

class Redlock:
    def __init__(self, redis_nodes):
        self.redis_nodes = [
            redis.Redis(host=host, port=port, socket_connect_timeout=0.1, socket_timeout=0.1)
            for host, port in redis_nodes
        ]
    def acquire_lock(self, resource, ttl):
        lock_id = str(uuid.uuid4())  # Unique identifier for the lock
        # every lock is “signed” with a random value, so the lock will be removed only if it is still the one that was set by the client trying to remove it
        acquire_count = 0
        start_time = time.perf_counter(); #more accurate than time()
        # Try to acquire the lock on each Redis node
        for node in self.redis_nodes:
            try:
                #lock with a TTL if it does not already exist
                if node.set(resource, lock_id, nx=True, px=ttl):
                    acquire_count += 1
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
                pass
       
        time_passed = (time.perf_counter() - start_time)*1000
        # if acquired on quorum of nodes and ttl not passed
        if acquire_count >= (len(self.redis_nodes) // 2) + 1 and time_passed < ttl :
            return True, lock_id
        
        # release if not acquired by quorum or ttl passed
        self.release_lock(resource, lock_id)
        return False, None

    def release_lock(self, resource, lock_id):
        # this is a Lua script to ensure atomicity when releasing the lock
        # and to avoid non atomic methods 
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        for node in self.redis_nodes:
            try:
                node.eval(script, 1, resource, lock_id)
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
                continue

def client_process(redis_nodes, resource, ttl, client_id):
    """
    Function to simulate a single client process trying to acquire and release a lock.
    """
    time.sleep(client_processes_waiting[client_id]) 

    redlock = Redlock(redis_nodes)
    print(f"\nClient-{client_id}: Attempting to acquire lock...")
    lock_acquired, lock_id = redlock.acquire_lock(resource, ttl)

    if lock_acquired:
        print(f"\nClient-{client_id}: Lock acquired! Lock ID: {lock_id}")
        # Simulate critical section
        time.sleep(3)  # Simulate some work
        redlock.release_lock(resource, lock_id)
        print(f"\nClient-{client_id}: Lock released!")
    else:
        print(f"\nClient-{client_id}: Failed to acquire lock.")

if __name__ == "__main__":
    # Define Redis node addresses (host, port)
    redis_nodes = [
        ("localhost", 63791),
        ("localhost", 63792),
        ("localhost", 63793),
        ("localhost", 63794),
        ("localhost", 63795),
    ]

    resource = "shared_resource"
    ttl = 5000  # Lock TTL in milliseconds (5 seconds)

    # Number of client processes
    num_clients = 5

    # Start multiple client processes
    processes = []
    for i in range(num_clients):
        process = multiprocessing.Process(target=client_process, args=(redis_nodes, resource, ttl, i))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()