# Data Dictionary

> Complete column-level reference for raw, staging, and marts layers. All datasets in `digital-marketing-509604`. Partition columns and grain noted per table. Metric formulas from PRD §8.4.

---

## Raw Layer (`digital_marketing_raw`)

### `raw_ga4_events`
**Partition:** `event_date` (DATE) | **Grain:** 1 row per event (`event_id` unique) | **Cluster (P2):** `channel_id, event_name`

| Column | Type | Mode | Notes |
|--------|------|------|-------|
| `event_id` | STRING | REQUIRED | UUID; unique; natural PK |
| `event_date` | DATE | REQUIRED | Partition column = data date of the run |
| `event_timestamp` | TIMESTAMP | REQUIRED | May spill into next day (PRD §3.3: session crosses midnight); **never assert** `date(event_timestamp) = event_date`. Allowed: `[event_date, event_date + 2 days)` |
| `event_name` | STRING | REQUIRED | `session_start`, `page_view`, `scroll`, `add_to_cart`, `begin_checkout`, `purchase` |
| `user_pseudo_id` | STRING | NULLABLE | ~2% null (consented/anonymous users) |
| `session_id` | STRING | REQUIRED | Format `YYYYMMDD-<10 digits>` |
| `channel_id` | INT64 | REQUIRED | FK → `raw_dim_channel` (1–7) |
| `page_location` | STRING | NULLABLE | Null for all `session_start` + ~5% other events |
| `event_value` | FLOAT64 | NULLABLE | Only for `purchase`; ≥ 0 |
| `inserted_at` | TIMESTAMP | REQUIRED | UTC; set once per DAG run |

---

### `raw_ads_campaign_daily`
**Partition:** `spend_date` (DATE) | **Grain:** (campaign_id, ad_group_id, ad_id, spend_date) | **Cluster (P2):** `campaign_id`

| Column | Type | Mode | Notes |
|--------|------|------|-------|
| `campaign_id` | INT64 | REQUIRED | FK → `raw_dim_campaign` |
| `channel_id` | INT64 | REQUIRED | FK → `raw_dim_channel` (1–7) |
| `ad_group_id` | INT64 | REQUIRED | Stable per campaign after D7 fix |
| `ad_id` | INT64 | REQUIRED | Stable per campaign after D7 fix |
| `spend_date` | DATE | REQUIRED | Partition column |
| `spend_usd` | FLOAT64 | REQUIRED | ≥ 0; campaign-day spend ≤ ~1.3× daily_budget_usd (D8) |
| `impressions` | INT64 | REQUIRED | ≥ 0 |
| `clicks` | INT64 | REQUIRED | 0 ≤ clicks ≤ impressions |
| `conversions` | INT64 | REQUIRED | 0 ≤ conversions ≤ clicks |
| `avg_order_value` | FLOAT64 | REQUIRED | > 0 |
| `inserted_at` | TIMESTAMP | REQUIRED | UTC; set once per DAG run |
| `_load_date` | DATE | REQUIRED | = the batch's data date (`spend_date`); **not** wall-clock load date |

---

### `raw_youtube_video_daily`
**Partition:** `video_date` (DATE) | **Grain:** (video_id, video_date) | **Cluster (P2):** `video_id`

| Column | Type | Mode | Notes |
|--------|------|------|-------|
| `video_id` | STRING | REQUIRED | FK → `raw_dim_video` |
| `video_date` | DATE | REQUIRED | Partition column |
| `views` | INT64 | REQUIRED | ≥ 0 |
| `watch_time_min` | INT64 | REQUIRED | ≥ 0 |
| `likes` | INT64 | REQUIRED | 0 ≤ likes ≤ views |
| `comments` | INT64 | REQUIRED | ≥ 0 |
| `shares` | INT64 | REQUIRED | ≥ 0 |
| `subscribers_gained` | INT64 | REQUIRED | 0 ≤ subscribers_gained ≤ views |
| `inserted_at` | TIMESTAMP | REQUIRED | UTC; set once per DAG run |
| `_load_date` | DATE | REQUIRED | = the batch's data date (`video_date`) |

---

### `raw_dim_campaign`
**Partition:** `_snapshot_date` (DATE) | **Grain:** (campaign_id, _snapshot_date) | Full snapshot per day

| Column | Type | Mode | Notes |
|--------|------|------|-------|
| `campaign_id` | INT64 | REQUIRED | PK within snapshot |
| `campaign_name` | STRING | REQUIRED | |
| `campaign_type` | STRING | REQUIRED | `Search`, `Display`, `Shopping`, `Video` |
| `status` | STRING | REQUIRED | `Active`, `Ended` — derived as-of `_snapshot_date` |
| `start_date` | DATE | REQUIRED | |
| `end_date` | DATE | REQUIRED | end_date ≥ start_date |
| `daily_budget_usd` | FLOAT64 | REQUIRED | > 0 |
| `inserted_at` | TIMESTAMP | REQUIRED | UTC; set once per DAG run |
| `_snapshot_date` | DATE | REQUIRED | Partition column = snapshot date |

---

### `raw_dim_channel`
**Partition:** `_snapshot_date` (DATE) | **Grain:** (channel_id, _snapshot_date) | 7 fixed rows per snapshot

| Column | Type | Mode | Notes |
|--------|------|------|-------|
| `channel_id` | INT64 | REQUIRED | 1–7 |
| `channel_name` | STRING | REQUIRED | `paid_search`, `organic_search`, `direct`, `social`, `email`, `referral`, `organic_video` |
| `channel_group` | STRING | REQUIRED | `Search`, `Search`, `Direct`, `Social`, `Email`, `Referral`, `Organic Video` |
| `inserted_at` | TIMESTAMP | REQUIRED | UTC |
| `_snapshot_date` | DATE | REQUIRED | Partition column |

---

### `raw_dim_video`
**Partition:** `_snapshot_date` (DATE) | **Grain:** (video_id, _snapshot_date) | Full snapshot per day

| Column | Type | Mode | Notes |
|--------|------|------|-------|
| `video_id` | STRING | REQUIRED | |
| `video_title` | STRING | REQUIRED | |
| `published_at` | DATE | REQUIRED | |
| `video_duration_min` | INT64 | REQUIRED | |
| `inserted_at` | TIMESTAMP | REQUIRED | UTC |
| `_snapshot_date` | DATE | REQUIRED | Partition column |

---

## Staging Layer (`digital_marketing_staging` — Views)

### `stg_ga4__events`
**Grain:** `event_id` | **Source:** `raw_ga4_events`

| Column | Derived from | Notes |
|--------|--------------|-------|
| `event_id` | `event_id` | dedupe by `event_id` (latest `inserted_at`) |
| `event_date` | `event_date` | |
| `event_timestamp` | `event_timestamp` | |
| `event_name` | `event_name` | |
| `user_pseudo_id` | `user_pseudo_id` | |
| `session_id` | `session_id` | |
| `channel_id` | `channel_id` | |
| `page_location` | `page_location` | |
| `page_path` | `page_location` | `regexp_extract(page_location, r'//[^/]+(/[^?#]*)')` |
| `is_anonymous_user` | `user_pseudo_id` | `user_pseudo_id IS NULL` |
| `event_hour` | `event_timestamp` | `EXTRACT(HOUR FROM event_timestamp)` |
| `event_value` | `event_value` | |

---

### `stg_google_ads__campaign_daily`
**Grain:** `ad_performance_key` | **Source:** `raw_ads_campaign_daily`

| Column | Derived from | Notes |
|--------|--------------|-------|
| `ad_performance_key` | surrogate(campaign_id, ad_group_id, ad_id, spend_date) | dedupe on key |
| `campaign_id` | `campaign_id` | |
| `channel_id` | `channel_id` | |
| `ad_group_id` | `ad_group_id` | |
| `ad_id` | `ad_id` | |
| `spend_date` | `spend_date` | |
| `spend_usd` | `spend_usd` | |
| `impressions` | `impressions` | |
| `clicks` | `clicks` | |
| `conversions` | `conversions` | |
| `avg_order_value` | `avg_order_value` | |
| `estimated_revenue_usd` | `conversions * avg_order_value` | **estimated** |
| `inserted_at` | `inserted_at` | |

---

### `stg_youtube__video_daily`
**Grain:** `video_daily_key` | **Source:** `raw_youtube_video_daily`

| Column | Derived from | Notes |
|--------|--------------|-------|
| `video_daily_key` | surrogate(video_id, video_date) | dedupe on key |
| `video_id` | `video_id` | |
| `video_date` | `video_date` | |
| `views` | `views` | |
| `watch_time_min` | `watch_time_min` | |
| `likes` | `likes` | |
| `comments` | `comments` | |
| `shares` | `shares` | |
| `subscribers_gained` | `subscribers_gained` | |
| `inserted_at` | `inserted_at` | |

---

### `stg_google_ads__campaigns`
**Grain:** `campaign_id` | **Source:** `raw_dim_campaign`

| Column | Derived from | Notes |
|--------|--------------|-------|
| `campaign_id` | `campaign_id` | latest snapshot per key: max `_snapshot_date`, tie-break `inserted_at` |
| `campaign_name` | `campaign_name` | |
| `campaign_type` | `campaign_type` | |
| `status` | `status` | |
| `start_date` | `start_date` | |
| `end_date` | `end_date` | |
| `daily_budget_usd` | `daily_budget_usd` | |
| `inserted_at` | `inserted_at` | |

---

### `stg_youtube__videos`
**Grain:** `video_id` | **Source:** `raw_dim_video`

| Column | Derived from | Notes |
|--------|--------------|-------|
| `video_id` | `video_id` | latest snapshot per key |
| `video_title` | `video_title` | |
| `published_at` | `published_at` | |
| `video_duration_min` | `video_duration_min` | |
| `inserted_at` | `inserted_at` | |

---

### `stg_reference__channels`
**Grain:** `channel_id` | **Source:** `raw_dim_channel`

| Column | Derived from | Notes |
|--------|--------------|-------|
| `channel_id` | `channel_id` | latest snapshot per key |
| `channel_name` | `channel_name` | |
| `channel_group` | `channel_group` | |
| `inserted_at` | `inserted_at` | |

---

## Marts Core (`digital_marketing_marts`)

### `dim_date`
**Materialization:** table | **Grain:** 1 row per date | **Range:** `dim_date_start` (2026-01-01) … `dim_date_end` (2026-12-31)

| Column | Type | Notes |
|--------|------|-------|
| `date` | DATE | |
| `date_sk` | INT64 | `YYYYMMDD` (star-schema KB) |
| `year` | INT64 | |
| `quarter` | INT64 | 1–4 |
| `month` | INT64 | 1–12 |
| `month_name` | STRING | `January`…`December` |
| `week_start` | DATE | Monday of the week |
| `day_of_week` | INT64 | 1=Mon … 7=Sun |
| `is_weekend` | BOOL | |

---

### `dim_channel`
**Materialization:** table | **Grain:** `channel_id`

| Column | Type | Notes |
|--------|------|-------|
| `channel_id` | INT64 | |
| `channel_name` | STRING | |
| `channel_group` | STRING | `group` is reserved in BigQuery |

---

### `dim_campaign`
**Materialization:** table | **Grain:** `campaign_id`

| Column | Type | Notes |
|--------|------|-------|
| `campaign_id` | INT64 | |
| `campaign_name` | STRING | |
| `campaign_type` | STRING | |
| `status` | STRING | |
| `start_date` | DATE | |
| `end_date` | DATE | |
| `daily_budget_usd` | FLOAT64 | |
| `campaign_duration_days` | INT64 | `end_date - start_date` |

---

### `dim_video`
**Materialization:** table | **Grain:** `video_id`

| Column | Type | Notes |
|--------|------|-------|
| `video_id` | STRING | |
| `video_title` | STRING | |
| `published_at` | DATE | |
| `video_duration_min` | INT64 | |

---

### `fct_web_events`
**Materialization:** **incremental** (`insert_overwrite`) | **Partition:** `event_date` | **Cluster:** `channel_id` | **Grain:** `event_id` | **Lookback:** 3 days (`run_date - 3`)

| Column | FK | Notes |
|--------|----|-------|
| `event_id` | — | PK |
| `event_date` | `dim_date.date` | partition |
| `date_sk` | `dim_date.date_sk` | |
| `event_timestamp` | — | |
| `event_name` | — | |
| `user_pseudo_id` | — | |
| `session_id` | — | |
| `channel_id` | `dim_channel.channel_id` | |
| `page_path` | — | |
| `is_anonymous_user` | — | |
| `event_hour` | — | |
| `event_value` | — | |

---

### `fct_web_sessions`
**Materialization:** **incremental** (`insert_overwrite`) | **Partition:** `session_date` | **Grain:** `session_id` | **Lookback:** 3 days

| Column | FK | Notes |
|--------|----|-------|
| `session_id` | — | PK |
| `session_date` | `dim_date.date` | partition = min `event_date` of session |
| `date_sk` | `dim_date.date_sk` | |
| `session_start_ts` | — | |
| `session_end_ts` | — | |
| `session_duration_min` | — | |
| `user_pseudo_id` | — | |
| `channel_id` | `dim_channel.channel_id` | channel of first event (`session_start`) |
| `event_count` | — | |
| `page_view_count` | — | |
| `has_add_to_cart` | — | bool |
| `has_begin_checkout` | — | bool |
| `has_purchase` | — | bool |
| `purchase_value` | — | sum `event_value` where `event_name = 'purchase'` |

---

### `fct_ad_performance_daily`
**Materialization:** **incremental** (`insert_overwrite`) | **Partition:** `spend_date` | **Cluster:** `campaign_id` | **Grain:** `ad_performance_key` | **Lookback:** 3 days

| Column | FK | Notes |
|--------|----|-------|
| `ad_performance_key` | — | PK |
| `spend_date` | `dim_date.date` | partition |
| `date_sk` | `dim_date.date_sk` | |
| `campaign_id` | `dim_campaign.campaign_id` | |
| `channel_id` | `dim_channel.channel_id` | |
| `ad_group_id` | — | |
| `ad_id` | — | |
| `spend_usd` | — | |
| `impressions` | — | |
| `clicks` | — | |
| `conversions` | — | |
| `avg_order_value` | — | |
| `estimated_revenue_usd` | — | conversions × avg_order_value (**estimated**) |

---

### `fct_youtube_video_daily`
**Materialization:** **incremental** (`insert_overwrite`) | **Partition:** `video_date` | **Cluster:** `video_id` | **Grain:** (video_id, video_date) | **Lookback:** 3 days

| Column | FK | Notes |
|--------|----|-------|
| `video_id` | `dim_video.video_id` | |
| `video_date` | `dim_date.date` | partition |
| `date_sk` | `dim_date.date_sk` | |
| `views` | — | |
| `watch_time_min` | — | |
| `likes` | — | |
| `comments` | — | |
| `shares` | — | |
| `subscribers_gained` | — | |
| `days_since_published` | — | `video_date - published_at` (from `dim_video`) |

---

## Marts Reporting (`digital_marketing_marts` — Tables)

### `rpt_channel_performance_daily`
**Grain:** date × channel | **Zero-fills:** yes (left join dim_date × dim_channel)

| Column | Formula / Source | Notes |
|--------|------------------|-------|
| `stat_date` | `dim_date.date` | |
| `channel_id` | `dim_channel.channel_id` | |
| `channel_name` | `dim_channel.channel_name` | |
| `channel_group` | `dim_channel.channel_group` | |
| `sessions` | count(distinct session_id) | GA4 |
| `users` | count(distinct user_pseudo_id) WHERE user_pseudo_id IS NOT NULL | GA4 |
| `purchases` | sum(has_purchase) | GA4 |
| `revenue_usd` | sum(purchase_value) | GA4 |
| `session_to_purchase_rate` | `purchases / sessions` | `safe_divide` |
| `ad_spend_usd` | sum(spend_usd) | Ads |
| `ad_impressions` | sum(impressions) | Ads |
| `ad_clicks` | sum(clicks) | Ads |
| `ad_conversions` | sum(conversions) | Ads |
| `ad_estimated_revenue_usd` | sum(estimated_revenue_usd) | Ads |
| `ctr` | `ad_clicks / ad_impressions` | `safe_divide` |
| `cpc` | `ad_spend_usd / ad_clicks` | `safe_divide` |
| `roas_estimated` | `ad_estimated_revenue_usd / ad_spend_usd` | `safe_divide` |

---

### `rpt_campaign_performance_daily`
**Grain:** date × campaign | **Zero-fills:** NO (only rows where campaign has spend on that date)

| Column | Formula / Source | Notes |
|--------|------------------|-------|
| `stat_date` | `dim_date.date` | |
| `campaign_id` | `dim_campaign.campaign_id` | |
| `campaign_name` | `dim_campaign.campaign_name` | |
| `campaign_type` | `dim_campaign.campaign_type` | |
| `status` | `dim_campaign.status` | |
| `spend_usd` | sum(spend_usd) | Ads |
| `daily_budget_usd` | `dim_campaign.daily_budget_usd` | |
| `budget_utilization` | `spend_usd / daily_budget_usd` | `safe_divide` |
| `impressions` | sum(impressions) | |
| `clicks` | sum(clicks) | |
| `conversions` | sum(conversions) | |
| `estimated_revenue_usd` | sum(estimated_revenue_usd) | **estimated** |
| `ctr` | `clicks / impressions` | `safe_divide` |
| `cpc` | `spend_usd / clicks` | `safe_divide` |
| `cvr` | `conversions / clicks` | `safe_divide` |
| `cpa` | `spend_usd / conversions` | `safe_divide` |
| `roas` | `estimated_revenue_usd / spend_usd` | `safe_divide` |

---

### `rpt_youtube_video_performance_daily`
**Grain:** date × video | **Zero-fills:** yes (left join dim_date × dim_video)

| Column | Formula / Source | Notes |
|--------|------------------|-------|
| `stat_date` | `dim_date.date` | |
| `video_id` | `dim_video.video_id` | |
| `video_title` | `dim_video.video_title` | |
| `published_at` | `dim_video.published_at` | |
| `days_since_published` | `stat_date - published_at` | |
| `views` | sum(views) | |
| `watch_time_min` | sum(watch_time_min) | |
| `likes` | sum(likes) | |
| `comments` | sum(comments) | |
| `shares` | sum(shares) | |
| `subscribers_gained` | sum(subscribers_gained) | |
| `avg_view_duration_min` | `watch_time_min / views` | `safe_divide` |
| `engagement_rate` | `(likes + comments + shares) / views` | `safe_divide` |
| `cumulative_views` | sum(views) OVER (PARTITION BY video_id ORDER BY stat_date) | |
| `cumulative_watch_time_min` | sum(watch_time_min) OVER (PARTITION BY video_id ORDER BY stat_date) | |

---

### `rpt_conversion_funnel_daily`
**Grain:** date × channel | **Note:** funnel as % of sessions (NOT step-to-step — PRD §3.3)

| Column | Formula / Source | Notes |
|--------|------------------|-------|
| `stat_date` | `dim_date.date` | |
| `channel_id` | `dim_channel.channel_id` | |
| `channel_name` | `dim_channel.channel_name` | |
| `sessions` | count(distinct session_id) | denominator |
| `sessions_with_page_view` | count(distinct session_id WHERE has_page_view) | % of sessions |
| `sessions_with_add_to_cart` | count(distinct session_id WHERE has_add_to_cart) | % of sessions |
| `sessions_with_begin_checkout` | count(distinct session_id WHERE has_begin_checkout) | % of sessions |
| `sessions_with_purchase` | count(distinct session_id WHERE has_purchase) | % of sessions |
| `page_view_rate` | `sessions_with_page_view / sessions` | `safe_divide` |
| `add_to_cart_rate` | `sessions_with_add_to_cart / sessions` | `safe_divide` |
| `begin_checkout_rate` | `sessions_with_begin_checkout / sessions` | `safe_divide` |
| `purchase_rate` | `sessions_with_purchase / sessions` | `safe_divide` |

---

## Metric Definitions (Single Source of Truth — PRD §8.4)

| Metric | Formula | Note |
|--------|---------|------|
| `ctr` | `clicks / impressions` | `safe_divide` |
| `cpc` | `spend / clicks` | `safe_divide` |
| `cvr` | `conversions / clicks` | `safe_divide` |
| `cpa` | `spend / conversions` | `safe_divide` |
| `roas` | `estimated_revenue / spend` | `safe_divide`; *estimated* |
| `budget_utilization` | `spend / daily_budget_usd` | `safe_divide` |
| `estimated_revenue_usd` | `conversions × avg_order_value` | **estimated** (label in descriptions) |
| `session_to_purchase_rate` | `purchases / sessions` | `safe_divide` |
| `engagement_rate` | `(likes + comments + shares) / views` | `safe_divide` |
| `avg_view_duration_min` | `watch_time_min / views` | `safe_divide` |

**All ratios:** use `dbt_utils.safe_divide`; computed from summed numerators/denominators at target grain; **never average ratios**.

---

## Important Notes

- **Ads channel_id limbo (D11):** Ads `channel_id` is only ever 1 (`paid_search`) or 4 (`social`), even for Display/Shopping/Video campaigns. Documented in dbt column descriptions; left as-is.
- **Funnel % of sessions:** Per PRD §3.3, funnel metrics are "% of sessions with event X" (denominator = sessions), never step-to-step rates.
- **GA4 purchases vs Ads conversions:** Generated independently; **do not reconcile** — that's expected.
- **D9 caveat:** Raw schemas declare `REQUIRED` mode; Parquet columns are nullable. Pending M2 real-BigQuery load test. If rejected: explicit PyArrow non-nullable schema OR relax to `NULLABLE` + enforce via GX/dbt.
- **`_load_date` semantics:** In raw tables, `_load_date` = the batch's data date (e.g., `spend_date` for ads); **not** wall-clock load date.
- **`channel_group` not `group`:** BigQuery reserved word avoidance (see decisions.md #14).
- **Partition filter (P2):** `require_partition_filter` on `raw_ga4_events` is optional; not yet enabled.