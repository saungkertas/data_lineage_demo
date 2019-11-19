{{ config(materialized='table') }}

with trans as (select * from {{ ref('trans')}}),
account as (select * from {{ ref('account')}}),
district as (select * from {{ ref('district')}}),
disp as (select * from {{ ref('disp')}}),
client as (select * from {{ ref('client')}})
select trans.date, trans.type as type_trans, trans.operation,
trans.amount, trans.balance, trans.k_symbol, trans.bank, trans.account,account.frequency, district.*, disp.type,
client.gender, client.birth_date, client.client_id
from trans join account on trans.account_id = account.account_id
join district on account.district_id = district.district_id
join disp on disp.account_id = account.account_id
join client on disp.client_id = client.client_id