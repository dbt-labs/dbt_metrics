# seeds/seed_slack_users.csv
seed_slack_users_csv = """
user_id,joined_at,is_active_past_quarter,has_messaged
1,2021-01-01 14:18:27,true,true
2,2021-02-03 17:18:55,false,true
3,2021-04-01 11:01:28,false,false
4,2021-04-08 22:43:09,false,false
""".lstrip()

# seeds/fact_orders_source.csv
fact_orders_source_csv = """
order_id,order_country,order_total,had_discount,customer_id,order_date
1,Russia,1,false,1,01/28/2022
2,Mauritius,1,false,2,01/20/2022
3,Peru,1,false,1,01/13/2022
4,Kazakhstan,1,true,3,01/06/2022
5,Portugal,1,false,4,01/08/2022
6,China,1,false,5,01/21/2022
7,Germany,1,true,2,01/22/2022
8,Greenland,1,true,1,02/15/2022
9,Bangladesh,1,false,2,02/03/2022
10,Sweden,1,false,3,02/13/2022
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
2021-02-14 17:52:36,maestro,10
2021-02-15 04:16:50,jcb,10
2021-02-15 11:30:45,solo,10
2021-02-16 13:08:18,americanexpress,10
2021-02-17 05:41:34,americanexpress,10
""".lstrip()

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
    version: [">=0.5.0", "<0.6.0"]
"""