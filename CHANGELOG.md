# Changelog

## 2.0.0
  * Added missing fields in the schemas and upgraded API version [#47](https://github.com/singer-io/tap-linkedin-ads/pull/47)
    * Creative endpoint changed to rest/creatives [57] (https://github.com/singer-io/tap-linkedin-ads/pull/57)
  * Dictionary based to class based implementation [#48](https://github.com/singer-io/tap-linkedin-ads/pull/48)
  * Add backoff error handling for 5xx [#45](https://github.com/singer-io/tap-linkedin-ads/pull/45)
  * Remove version pins for dev requirements [#38](https://github.com/singer-io/tap-linkedin-ads/pull/38)
  * Rewrite access_token into the config to reuse in the next sync if it is updated [#52](https://github.com/singer-io/tap-linkedin-ads/pull/52)
  * Add missing tap tester [#46](https://github.com/singer-io/tap-linkedin-ads/pull/46)

## 1.2.7
  * Checks Access Token expiration before attempting to refresh token.
  * Adds backoff case for token expiration.
  * Adds unittests and unittest related functions.
  [#50](https://github.com/singer-io/tap-linkedin-ads/pull/50)

## 1.2.6
  * Fixed 1.2.5 release issues [#43](https://github.com/singer-io/tap-linkedin-ads/pull/43)

## 1.2.5
  * Auto access-token refresh [#41](https://github.com/singer-io/tap-linkedin-ads/pull/41)
  
## 1.2.4
  * Add Request Timeout [#36](https://github.com/singer-io/tap-linkedin-ads/pull/36)
  * Handling 4xx responses for adCampaignGroup [#28](https://github.com/singer-io/tap-linkedin-ads/pull/28)
  * Check Invalid account in discovery mode [#35](https://github.com/singer-io/tap-linkedin-ads/pull/35)
  * Make Replication Key automatic [#33](https://github.com/singer-io/tap-linkedin-ads/pull/33)
  * Improve test coverage [#30](https://github.com/singer-io/tap-linkedin-ads/pull/30)

## 1.2.3
  * Changes multipleOf to singer.decimal in schemas and bumps singer-python version
    [#26](https://github.com/singer-io/tap-linkedin-ads/pull/26)

## 1.2.2
  * Increase the precision on floating point numbers from 1e-8 to 1e-20
    [#24](https://github.com/singer-io/tap-linkedin-ads/pull/24)

## 1.2.1
  * Backoff tuning [#22](https://github.com/singer-io/tap-linkedin-ads/pull/22)

## 1.2.0
  * Add date windowing for the `ad_analytics_by_creative` stream
  * Add the ability to select all metrics for the
    `ad_analytics_by_creative` stream
  * [#20](https://github.com/singer-io/tap-linkedin-ads/pull/20)

## 1.1.0
  * Add date windowing for the `ad_analytics_by_campaign` stream
  * Add the ability to select all metrics for the
    `ad_analytics_by_campaign` stream
  * [#18](https://github.com/singer-io/tap-linkedin-ads/pull/18)

## 1.0.2
  * Fix typo in the `ad_analytics_by_campaign` schema
  * [#15](https://github.com/singer-io/tap-linkedin-ads/pull/15)

## 1.0.1
  * Update request format for `ad_analytics_by_campaigns` and `ad_analytics_by_creative`
  * Update schemas for: `campaigns`, `ad_analytics_by_campaigns`, and `ad_analytics_by_creative`
  * Add assertion around how many fields can be selected for `ad_analytics_by_campaigns` and `ad_analytics_by_creative`
  * Prettified all JSON schema files
  * [#12](https://github.com/singer-io/tap-linkedin-ads/pull/12)

## 1.0.0
  * Preparing for v1.0.0 release

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
