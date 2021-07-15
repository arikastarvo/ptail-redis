# ptail-redis - redis stream reader (using XREADGROUP)

Ad-hoc copy/paste ball of mud for following redis streams. Multiple stream key's can be follwed by using glob patterns. New key's will be discovered periodically (search interval configurable).

not yet ready!

```
Usage of ./ptail-redis:
  -consumer string
        consumer name for redis xreadgroup command (default: consumer) (default "consumer")
  -glob int
        interval in seconds for re-running glob search (default is 0 - disabled; only initially found files will be monitored). Will be auto-set to 1 if globbing detected.
  -group string
        group name for redis xreadgroup command (default: group) (default "group")
  -host string
        redis host name (default: localhost) (default "localhost")
  -key value
        key (or glob pattern) to tail (can be used multiple times); (default: '*' - all keys
  -log string
        enable logging. "-" for stdout, filename otherwise
  -port int
        redis port (default: 6379) (default 6379)
  -wait
        wait for keys to appear, don't exit program if monitored (and actually existing) key count is 0 (default true)
```