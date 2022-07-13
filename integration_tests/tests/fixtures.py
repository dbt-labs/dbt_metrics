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
1,Russia,10,false,643,04/28/2022
2,Mauritius,10,false,84,01/20/2022
3,Peru,2,false,802,05/13/2022
4,Kazakhstan,5,true,803,01/06/2022
5,Portugal,10,false,6,03/08/2022
6,China,5,false,966,01/21/2022
7,Germany,10,true,971,04/22/2022
8,Greenland,8,true,789,05/15/2022
9,Bangladesh,20,false,997,03/03/2022
10,Sweden,10,false,92,03/13/2022
""".lstrip()

# models/fact_orders.sql
fact_orders_sql = """
select 
    *
    ,round(order_total - (order_total/2)) as discount_total
from {{ref('fact_orders_source')}}
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

