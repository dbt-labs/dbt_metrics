
select *,
    case
        when order_total < 50 then 'small'
        else 'large'
    end as order_total_band

from PARTNER_MODE.RAW.ORDERS
