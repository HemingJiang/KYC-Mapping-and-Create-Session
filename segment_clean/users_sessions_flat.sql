WITH a AS (
SELECT
  distinct anonymous_id
  ,acctid
  , context_user_agent
  ,device
  ,browser
  ,context_campaign_term
  ,context_campaign_name
  ,context_campaign_medium
  ,context_campaign_source
  ,context_campaign_content
  ,context_page_path
  ,context_page_title
 ,created
 , COUNT(reset)OVER(partition by coalesce(acctid,anonymous_id) order by created,reset  ROWS UNBOUNDED PRECEDING ) AS SESSION_ID
FROM
 (
  SELECT
  anonymous_id
 ,acctid
  , context_user_agent
  ,     case when lower(context_user_agent) similar to '%(iphone|ipad)%' then 'ios'
          when lower(context_user_agent) similar to '%(android)%' then 'android'
          when lower(context_user_agent) similar to '%(window|x11)%' then 'windows'
          when lower(context_user_agent) similar to '%(mac)%' then 'mac'
          ELSE 'other' end device
  ,case when lower(context_user_agent) similar to '%(chrome)%' then 'chrome'
          when lower(context_user_agent) similar to '%(firefox)%' then 'firefox'
          when (lower(context_user_agent) similar to '%(safari)%' and lower(context_user_agent) not like '%chrome%') then 'safari'
          ELSE 'other' end browser
  ,context_campaign_term
  ,context_campaign_name
  ,context_campaign_medium
  ,context_campaign_source
  ,context_campaign_content
  ,context_page_path
  ,context_page_title
         ,timestamp as created
         ,CASE WHEN (timestamp - LAG(timestamp) OVER (PARTITION BY coalesce(acctid,anonymous_id) ORDER BY timestamp) > INTERVAL '30 minute') THEN 1 END AS reset
  FROM segment_funnel.segment_pages
  ORDER BY acctid
           ,timestamp
  LIMIT 10000
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
  , datediff(second,min(created),max(created)) as session_length_second
  , count(session_id) as number_pages
FROM a
GROUP BY 1,2,3
ORDER BY 3,1,2
)

SELECT b.session_id
  ,b.anonymous_id
  ,b.acctid
  ,c.uid
  ,EXTRACT(month FROM b.session_start_time) AS month
  ,DATE_PART(dow,b.session_start_time) AS Day_of_Week
  ,b.session_start_time
  ,b.session_end_time
  ,b.session_length_second
  ,a.context_campaign_term
  ,a.context_campaign_name
  ,a.context_campaign_medium
  ,a.context_campaign_source
  ,a.context_campaign_content
  ,a.context_page_path
  ,a.context_page_title
  , CASE WHEN c.created IS NULL THEN 'FALSE' ELSE 'TRUE' END AS Registered
  , CASE WHEN d.created IS NULL THEN 'FALSE' ELSE 'TRUE' END AS Has_Funded
  , CASE WHEN e.created IS NULL THEN 'FALSE' ELSE 'TRUE' END AS Has_Traded
  ,c.created as Registered_time
  ,d.created as Funded_time
  ,e.created as traded_time
  ,a.device
  ,a.browser

FROM b
LEFT JOIN a on a.session_id = b.session_id and a.anonymous_id = b.anonymous_id and a.acctid = b.acctid and b.session_start_time = a.created
LEFT JOIN cs.users c on b.acctid = c.acctid and c.created < b.session_end_time
LEFT JOIN (SELECT DISTINCT created,acctid FROM segment_funnel.segment_funding_aws) d on b.acctid = d.acctid and d.created < b.session_end_time
LEFT JOIN (SELECT DISTINCT created,acctid FROM segment_funnel.segment_trading_aws) e on b.acctid = e.acctid and e.created < b.session_end_time
ORDER BY 3,1,2
limit 10
