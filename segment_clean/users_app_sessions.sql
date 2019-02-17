
BEGIN;
TRUNCATE TABLE segment_clean.users_app_sessions ;
INSERT INTO segment_clean.users_app_sessions


WITH a AS (
SELECT
  distinct anonymous_id
  ,acctid
  ,device
  ,event_text
  ,context_traits_base_currency
  ,context_os_name
  , created
 , COUNT(reset)OVER(partition by coalesce(acctid,anonymous_id) order by created,reset  ROWS UNBOUNDED PRECEDING ) AS SESSION_ID
FROM
 (
  SELECT
  anonymous_id
 ,acctid
  , context_device_type as device
  ,event_text
  ,context_traits_base_currency
  ,context_os_name
  , created
  ,CASE WHEN (created - LAG(created) OVER (PARTITION BY coalesce(acctid,anonymous_id) ORDER BY created) > INTERVAL '30 minute') THEN 1 END AS reset
  FROM segment_funnel.segment_ios_android_pages
  ORDER BY acctid
           ,created
  )
  ORDER BY acctid
           , created
           , SESSION_ID
)
, b AS (
SELECT distinct session_id
  ,anonymous_id
  ,acctid
  , min(created) as session_start_time
  , max(created) as session_end_time
  , datediff(second,min(created),max(created)) :: float8 as session_length_second
  , count(session_id) as number_pages
FROM a
GROUP BY 1,2,3
ORDER BY 3,1,2
)

SELECT b.session_id
  ,b.anonymous_id
  ,b.acctid
  ,c.uid
  ,b.session_start_time
  ,b.session_end_time
  ,b.session_length_second
  ,a.event_text
  ,a.context_traits_base_currency
  ,a.context_os_name
  , CASE WHEN c.created IS NULL THEN 'FALSE' ELSE 'TRUE' END AS Registered
  , CASE WHEN d.created IS NULL THEN 'FALSE' ELSE 'TRUE' END AS Has_Funded
  , CASE WHEN e.created IS NULL THEN 'FALSE' ELSE 'TRUE' END AS Has_Traded
  ,c.created as Registered_time
  ,d.created as Funded_time
  ,e.created as traded_time
  ,a.device


FROM b
LEFT JOIN a on a.session_id = b.session_id and a.anonymous_id = b.anonymous_id and a.acctid = b.acctid and b.session_start_time = a.created
LEFT JOIN cs.users c on b.acctid = c.acctid and c.created < b.session_end_time
LEFT JOIN (SELECT DISTINCT created,acctid FROM segment_funnel.segment_funding_aws) d on b.acctid = d.acctid and d.created < b.session_end_time
LEFT JOIN (SELECT DISTINCT created,acctid FROM segment_funnel.segment_trading_aws) e on b.acctid = e.acctid and e.created < b.session_end_time
ORDER BY 3,1,2;
COMMIT;
GRANT SELECT ON segment_clean.users_app_sessions TO etl_pipeline;
