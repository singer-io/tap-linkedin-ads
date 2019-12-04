# Changelog

## 0.0.6
  * Fix 400 error ad_analytics_by_campaign/creative not paging beyond first page.

## 0.0.5
  * Add missing fields to the Ad Analytics endpoint streams: clicks, reactions, sends, etc.

## 0.0.4
  * Change bookmarking to update after ALL batches for child nodes. Change paging logic to use next URL in links. Add 7 days buffer to account for changes/delays in ads analytics report data. 

## 0.0.3
  * Fix sync datetime conversion issue when reading from state. Using singer-python utils strptime_to_utc.

## 0.0.2
  * Re-work sync.py child node to simplify and bulkify. Fix client.py permissions issue with check_access_token. Simplify transform.py, leverage singer-io datetime conversions for unix ms integers.

## 0.0.1
  * Initial commit
