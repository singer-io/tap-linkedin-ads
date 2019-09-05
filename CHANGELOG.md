# Changelog

## 0.0.3
  * Fix sync datetime conversion issue when reading from state. Using singer-python utils strptime_to_utc.

## 0.0.2
  * Re-work sync.py child node to simplify and bulkify. Fix client.py permissions issue with check_access_token. Simplify transform.py, leverage singer-io datetime conversions for unix ms integers.

## 0.0.1
  * Initial commit
