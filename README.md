# tap-linkedin-ads

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [LinkedIn Marketing Ads 2.0 API](https://docs.microsoft.com/en-us/linkedin/marketing/)
- Extracts the following resources:
  - [Ad Accounts](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-accounts#search-for-accounts)
    - [Ad Account Users](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-account-users#find-ad-account-users-by-accounts)
    - [Video Ads](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/advertising-targeting/create-and-manage-video#finders)
  - [Campaign Groups](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaign-groups#search-for-campaign-groups)
  - [Campaigns](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-campaigns#search-for-campaigns)
  - [Creatives](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads/account-structure/create-and-manage-creatives#search-for-creatives)
  - [Ad Analytics by Campaign](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder)
  - [Ad Analytics by Creativ](https://docs.microsoft.com/en-us/linkedin/marketing/integrations/ads-reporting/ads-reporting#analytics-finder)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-linkedin-ads
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)
3. Create your tap's `config.json` file which should look like the following:

    ```json
    {
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-linkedin-ads <api_user_email@your_company.com>",
        "access_token": "YOUR_ACCESS_TOKEN",
        "accounts": "id1, id2, id3"
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "creatives",
        "bookmarks": {
            "accounts": "2019-06-11T13:37:55Z",
            "account_users": "2019-06-19T19:48:42Z",
            "video_ads": "2019-06-18T18:23:58Z",
            "campaign_groups": "2019-06-20T00:52:46Z",
            "campaigns": "2019-06-19T19:48:44Z",
            "creatives": "2019-06-11T13:37:55Z",
            "ad_analytics_by_campaign": "2019-06-11T13:37:55Z",
            "ad_analytics_by_creative": "2019-06-11T13:37:55Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-linkedin-ads --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-linkedin-ads --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-linkedin-ads --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-linkedin-ads --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the Linkedin Ads tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_linkedin_ads -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 10.00/10 (previous run: 9.97/10, +0.03)
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-linkedin-ads --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid... TBD
    ```
---

Copyright &copy; 2019 Stitch
