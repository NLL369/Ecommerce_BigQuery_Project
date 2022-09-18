#standardSQL

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

select 
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(totals.visits) as visits,
  sum(totals.pageviews) as pagevies,
  sum(totals.transactions) as transactions,
  sum(totals.totalTransactionRevenue) / power(10,6) as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170101' and '20170331'
group by 1
order by 1;



-- Query 02: Bounce rate per traffic source in July 2017

-- Bounce session is the session that user does not raise any click after landing on the website
-- Bounce_rate = num_bounce/total_visit
select 
  trafficSource.source as source,
  count(totals.visits) as total_visit,
  count(totals.bounces) as total_bounce,
  count(totals.bounces) / count(totals.visits) *100 as bounce_rate
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by 1
order by 2 desc;


-- Query 3: Revenue by traffic source by week, by month in June 2017

-- separate month and week data then union all
with week_rev as
    (select 
      'Week' as time_type,
      format_date("%Y%w", parse_date("%Y%m%d", date)) as time,
      trafficSource.source as source,
      sum(totals.totalTransactionRevenue) / power(10,6) as revenue
    from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
    group by 1,2,3
    ),
  month_rev as
    (select 
      'Month' as time_type,
      format_date("%Y%m", parse_date("%Y%m%d", date)) as time,
        trafficSource.source as source,
        sum(totals.totalTransactionRevenue) / power(10,6) as revenue
      from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
      group by 1,2,3)
select * from month_rev
union all 
select * from week_rev
order by 4 desc


--Query 04: Average number of product pageviews by purchaser type in June, July 2017

-- Avg pageview = total pageview / number unique user
-- purchaser type: purchasers vs non-purchasers
-- totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser

with pc as 
    (select 
      format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
      sum(totals.pageviews) / count(distinct fullVisitorId) as avg_pageviews_purchase
    from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    where _table_suffix between '20170601' and '20170731'
      and totals.transactions >=1
    group by 1
    ),

    npc as
    (
      select 
      format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
      sum(totals.pageviews) / count(distinct fullVisitorId) as avg_pageviews_non_purchase
      from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      where _table_suffix between '20170601' and '20170731'
        and totals.transactions is null
      group by 1 
    )
select * from pc
join npc using(month)
order by 1 



-- Query 05: Average number of transactions per user that made a purchase in July 2017

select
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  sum(totals.transactions) / count(distinct fullVisitorId) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions is not null
group by 1



-- Query 06: Average amount of money spent per session
select 
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  (sum(totals.totalTransactionRevenue) / power(10,6)) /count(totals.visits) as avg_revenue_by_user_per_visit    
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions is not null
group by 1




-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)



select
  product.v2ProductName as other_purchased_products,
  sum(product.productQuantity) as quantity
from 
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  unnest(hits) as hits,
  unnest(hits.product) as product
where fullVisitorId in
                    (select fullVisitorId 
                    from 
                      `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                      unnest(hits) as hits,
                      unnest(hits.product) as product
                    where product.v2ProductName = "YouTube Men's Vintage Henley"
                        and product.productRevenue is not null 
                    )
and product.v2ProductName != "YouTube Men's Vintage Henley"
and product.productRevenue is not null 
group by 1
order by 2 desc





--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month


-- hits.eCommerceAction.action_type = '2' is view product page
-- hits.eCommerceAction.action_type = '3' is add to cart
-- hits.eCommerceAction.action_type = '6' is purchase

with pv as 
          (select
              format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
              count(product.v2ProductName) as num_product_view
            from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                  unnest(hits) hits,
                  unnest(hits.product) product
            where _table_suffix between '20170101' and '20170331'
            and hits.eCommerceAction.action_type = '2'
          group by 1
            ),
    atc as
          (select
              format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
              count(product.v2ProductName) as num_addtocart
          from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                unnest(hits) hits,
                unnest(hits.product) product
          where _table_suffix between '20170101' and '20170331'
            and hits.eCommerceAction.action_type = '3'
          group by 1
          ),
      pc as
          (select
              format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
              count(product.v2ProductName) as num_purchase
            from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
                  unnest(hits) hits,
                  unnest(hits.product) product
            where _table_suffix between '20170101' and '20170331'
              and hits.eCommerceAction.action_type = '6'
            group by 1)

select pv.month, 
      num_product_view,
      num_addtocart,
      num_purchase,
      round((num_addtocart / num_product_view)*100,2) as add_to_cart_rate,
      round((num_purchase / num_product_view)*100,2) as purchase_rate
from pv, atc, pc
where pv.month = atc.month and atc.month = pc.month 
order by 1; 