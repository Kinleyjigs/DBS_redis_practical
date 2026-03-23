import redis

# Create Redis client (defaults to localhost:6379)
r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)

def main():
    # Simple health check
    pong = r.ping()
    print(f"Redis PING response: {pong}")

    # Basic SET/GET
    r.set("dbs302:test:key", "hello-redis")
    value = r.get("dbs302:test:key")
    print(f"Value from Redis: {value}")

if __name__ == "__main__":
    main()
