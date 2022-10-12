# seeds/seed_slack_users.csv
seed_slack_users_csv = """
user_id,joined_at,is_active_past_quarter,has_messaged
1,2021-01-01,true,true
2,2021-02-03,false,true
3,2021-04-01,false,false
4,2021-04-08,false,false
""".lstrip()

# seeds/fact_orders_source.csv
fact_orders_source_csv = """
order_id,order_country,order_total,had_discount,customer_id,order_date
4,France,1,true,3,2022-01-06
5,France,1,false,4,2022-01-08
3,France,1,false,1,2022-01-13
2,Japan,1,false,2,2022-01-20
6,Japan,1,false,5,2022-01-21
7,Japan,1,true,2,2022-01-22
1,France,2,false,1,2022-01-28
9,Japan,1,false,2,2022-02-03
10,Japan,1,false,3,2022-02-13
8,France,4,true,1,2022-02-15
""".lstrip()

# seeds/dim_customers_source.csv
dim_customers_source_csv = """
customer_id,first_name,last_name,email,gender,is_new_customer,date_added
1,Geodude,Hills,bhills0@altervista.org,Male,FALSE,2022-01-01
2,Mew,Coxhead,mcoxhead1@symantec.com,Genderfluid,TRUE,2022-01-06
3,Mewtwo,Redish,aredish2@last.fm,Genderqueer,FALSE,2022-01-13
4,Charizard,Basant,lbasant3@dedecms.com,Female,TRUE,2022-02-01
5,Snorlax,Pokemon,the_email@dedecms.com,Male,TRUE,2022-02-03
""".lstrip()

# seeds/mock_purchase_data.csv
mock_purchase_data_csv = """
purchased_at,payment_type,payment_total
2021-02-14,maestro,10
2021-02-15,jcb,10
2021-02-15,solo,10
2021-02-16,americanexpress,10
2021-02-17,americanexpress,10
""".lstrip()

# models/custom_calendar.sql
custom_calendar_sql = """
{{ config(materialized='table') }}
with days as (
    {{ metrics.metric_date_spine(
    datepart="day",
    start_date="cast('2010-01-01' as date)",
    end_date="cast('2030-01-01' as date)"
   )
    }}
),
final as (
    select 
        cast(date_day as date) as date_day,
        {% if target.type == 'bigquery' %}
            --BQ starts its weeks on Sunday. I don't actually care which day it runs on for auto testing purposes, just want it to be consistent with the other seeds
            cast({{ date_trunc('week(MONDAY)', 'date_day') }} as date) as date_week,
        {% else %}
            cast({{ date_trunc('week', 'date_day') }} as date) as date_week,
        {% endif %}
        cast({{ date_trunc('month', 'date_day') }} as date) as date_month,
        cast({{ date_trunc('quarter', 'date_day') }} as date) as date_quarter,
        cast({{ date_trunc('year', 'date_day') }} as date) as date_year,
        true as is_weekend
    from days
)
select * from final
"""

# models/fact_orders.sql
fact_orders_sql = """
select 
    *
    ,round(order_total - (order_total/2)) as discount_total
from {{ref('fact_orders_source')}}
"""

# models/dim_customers.sql
dim_customers_sql = """
select * from {{ref('dim_customers_source')}}
"""

# models/combined__orders_customers.sql
combined__orders_customers_sql = """
with orders as (
    select * from {{ ref('fact_orders') }}
)
,
customers as (
    select * from {{ ref('dim_customers') }}
)
,
final as (
    select *
    from orders
    left join customers using (customer_id)
)
select * from final
"""


# models/fact_orders.yml
fact_orders_yml = """
version: 2 
models: 
  - name: fact_orders
    columns:
      - name: order_id
        description: TBD
      - name: order_country
        description: TBD
      - name: order_total
        description: TBD
      - name: had_discount
        description: TBD
      - name: customer_id
        description: TBD
      - name: order_date
        description: TBD
"""

# models/dim_customers.yml
dim_customers_yml = """
version: 2 
models: 
  - name: dim_customers
    columns:
      - name: customer_id
        description: TBD
      - name: first_name
        description: TBD
      - name: last_name
        description: TBD
      - name: email
        description: TBD
      - name: gender
        description: TBD
        
      - name: is_new_customer
        description: TBD
"""

# packages.yml
packages_yml = """
  - package: calogica/dbt_expectations
    version: [">=0.6.0", "<0.7.0"]

  - package: dbt-labs/dbt_utils
    version: [">=0.9.0", "<1.0.0"]
"""

# seeds/events.csv
events_source_csv = """
id,country,timestamp_field
1,FR,2022-01-01
2,UK,2022-02-01
""".lstrip()

# models/event.sql
event_sql = """
with source as (
    select * from {{ ref('events_source') }}
)
,
final as (
    select *
    from source 
)
select * from final
"""

# models/event.yml
event_yml = """
version: 2 
models: 
  - name: event
    columns:
      - name: id
        description: TBD
      - name: country
        description: TBD
      - name: timestamp_field
        description: TBD
"""