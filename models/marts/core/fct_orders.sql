with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments')}}
),

payment_amount as (
    select order_id, 
    SUM(case when status = 'success' then amount end) as amount 
    from payments
    group by 1
),

final as (
    select orders.order_id,
    orders.customer_id,
    orders.order_date,
    coalesce(payment_amount.amount, 0) as amount
    from orders
    left join payment_amount using (order_id)
)

select * from final