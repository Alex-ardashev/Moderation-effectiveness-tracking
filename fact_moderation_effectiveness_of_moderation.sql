CREATE TABLE IF NOT EXISTS hive.odyn_css.fact_moderation_effectiveness_of_moderation_test
(
  ad_id                             varchar,
  site_code                         varchar,  
  time_actived                      timestamp,
  time_moderated                    timestamp,
  previous_status                   varchar,
  status                            varchar,
  next_status                       varchar,
  lifetime_hours                    integer,
  lifetime_days                     integer,
  tool_1st_decision                 varchar,
  reason_1st_decision               varchar,
  tool_2nd_decision                 varchar,
  moderation_category_2nd_decision  varchar,
  reason_2nd_decision               varchar,
  category_2nd_decision             varchar,
  page_views                        integer,
  total_messages                    integer,
  partition_date                    date
)

WITH (
    FORMAT = 'PARQUET',
    partitioned_by = ARRAY ['partition_date']
);

DELETE
FROM hive.odyn_css.fact_moderation_effectiveness_of_moderation_test
WHERE partition_date = DATE('${vars:date}');



INSERT INTO hive.odyn_css.fact_moderation_effectiveness_of_moderation_test


WITH ads AS
(
       SELECT *,
              date_diff('hour', valid_from, valid_to) AS lifetime_hours
       FROM   hive.odyn_eu_bi.dim_ads_status
       WHERE  site_code IN ('olxbg',
                            'olxro',
                            'olxuz',
                            'olxpl',
                            'olxua',
                            'olxkz',
                            'olxpt')
       AND    status      IN ('active')
       AND    next_status IN ('moderated') -- looking only ad ads that were directly rejected, not at removed_by_moderator
       AND    DATE(valid_to) = DATE('${vars:date}')
       ),


-- ads that were autoapproved or sent to premoderation
adscorer AS
(
                SELECT DISTINCT ad_id,
                                site_code,
                                CASE
                                                WHEN adscorer_action IS NULL THEN 'autoapproval'
                                                WHEN adscorer_action = 'premoderate' THEN 'manual_approval'
                                                ELSE 'no_error'
                                END AS reason
                FROM            glue.odyn_css.fact_adscorer_relevant_ads
                WHERE           (
                                                adscorer_action = 'premoderate'
                                OR              adscorer_action IS NULL)
                AND             date(decision_date) = date('${vars:date}')
                ),

adscorer_aggregated      AS 
(
               SELECT          ad_id,
                               site_code,
                               max(reason) as reason -- if the ad was had manual_approval then use that, else use autoapproval
               FROM            adscorer
               GROUP BY 1,2
),

-- select any kind of postmoderated ads
ads_fe AS
(
                SELECT DISTINCT params_content_event_id as event_id,
                                params_content_event_adid AS ad_id,
                                params_content_event_sitecode AS site_code,
                                params_content_event_category as event_category,
                                params_content_event_moderationcategory AS event_moderationcategory,
                                params_content_event_type as event_type,
                                params_content_event_reason as event_reason,
                                CAST(Replace(Substr(params_content_meta_received, 1, 19), 'T', ' ') AS TIMESTAMP) as time_decision
                FROM            hydra.global_css_decisions
                WHERE           cast(concat(year,'-',month,'-',day) AS date) = date('${vars:date}')
                and             params_content_meta_sender = 'ad-moderation-queue'
                -- including all kinds of post moderation
                AND             params_content_event_moderationcategory IN ('ad_search',
                                                                            'ad_detail',
                                                                            'ad_bulk_actions', 
                                                                            'reported_ads', 
                                                                            'duplicate_ads',
                                                                            'default')
                AND             params_content_event_type = 'reject' -- only look at rejections in Neo, not edited ads
                AND             params_content_event_category != 'undefined'
                AND             params_content_event_reason IS NOT NULL
                ),

ads_fe_ordered as 
(                SELECT DISTINCT event_id,
                                ad_id,
                                site_code,
                                event_category,
                                event_moderationcategory,
                                event_type,
                                event_reason,
                                time_decision,
                                LEAD(time_decision) OVER (PARTITION BY site_code, ad_id ORDER BY time_decision) as time_next_decision
FROM ads_fe
),

-- ads that were autoapproved or sent to premoderation, were active, and then got banned
ads_final AS
(
          SELECT    ads.*,
                    reason,
                    event_category,
                    event_moderationcategory,
                    event_type,
                    event_reason
          FROM      ads
          LEFT JOIN adscorer_aggregated adsc
          ON        ads.ad_id = cast(adsc.ad_id AS bigint)
          AND       ads.site_code = adsc.site_code
          LEFT JOIN ads_fe_ordered fe
          ON        ads.ad_id = cast(fe.ad_id AS bigint)
          AND       ads.site_code = fe.site_code
          -- if multiple rejection were made in NEO on that day for the ad, use the one that happened directly before the change in ad status from active to moderated
          -- if there's only one rejection, use that one rejection
          AND       (
                     (time_next_decision IS NULL AND ads.valid_to > time_decision) 
                     OR 
                     (time_next_decision IS NOT NULL AND ads.valid_to between time_decision AND time_next_decision)
                     )
           ),
-- total views
views AS
(
       SELECT ad_id,
              site_code,
              sum(page_views) as page_views
       FROM   odyn_css.fact_malicious_ads_page_views
       WHERE  date(partition_date) between date('${vars:date}') - interval '14' day and date('${vars:date}')
       AND    site_code IN ('olxpt',
                            'olxua',
                            'olxkz',
                            'olxuz',
                            'olxpl',
                            'olxbg',
                            'olxro')
       GROUP BY 1,2
       ),
-- total messages
messages AS
(
         SELECT   listing_nk AS ad_id,
                  replace(site_sk,'|eu|','') AS site_code,
                  count(*)                   AS total_messages
         FROM     olxgroup_reservoir_ares.reservoirs_olxgroup_reservoir_eu_bi_eu_bi_fact_all_messages_platform
         WHERE    date(year||'-'||month||'-'||day) between date('${vars:date}') - interval '14' day and date('${vars:date}')
                       -- trying to use partitions of year, month and day for query
         AND      site_sk IN ('olx|eu|pt',
                              'olx|eu|ua',
                              'olx|eu|kz',
                              'olx|eu|uz',
                              'olx|eu|pl',
                              'olx|eu|pt',
                              'olx|eu|bg',
                              'olx|eu|ro')
         AND      is_read = 't'
         GROUP BY 1,2),

fact_moderation_effectiveness_of_moderation AS
(
          SELECT    DISTINCT
                    cast(ads.ad_id AS varchar) as ad_id,
                    ads.site_code,
                    valid_from,
                    valid_to,
                    previous_status,
                    status,
                    next_status,
                    lifetime_hours,
                    cast(floor(CAST(lifetime_hours as double)/24) as integer) as lifetime_days,
                    CASE WHEN reason IS NULL then 'Hermes' else 'Neo' end as tool_1st_decision,
                    CASE WHEN reason IS NULL then 'Hermes' else reason end as reason_1st_decision,
                    CASE WHEN event_moderationcategory IS NULL then 'Hermes' else 'Neo' end as tool_2nd_decision,
                    event_moderationcategory as moderation_category_2nd_decision,
                    event_reason as reason_2nd_decision,
                    event_category as category_2nd_decision,
                    page_views,
                    total_messages,
                    date('${vars:date}') AS partition_date
          FROM      ads_final ads
          LEFT JOIN views
          ON        ads.ad_id = cast(views.ad_id AS bigint)
          AND       ads.site_code = views.site_code
          LEFT JOIN messages
          ON        ads.ad_id = cast(messages.ad_id AS bigint)
          AND       ads.site_code = messages.site_code )

SELECT *
FROM   fact_moderation_effectiveness_of_moderation
;
