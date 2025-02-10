with base_table as (
select
	"network order id",
	"buyer np name",
	"seller np name",
	"fulfillment status",
	row_number() over (partition by ("network order id" ||
		(case
		when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" like '%dominos%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "item consolidated category" like 'Agri%'
		or "domain" like '%AGR%' then 'Agriculture'
		when "seller np name" like '%agrevolution%' then 'Agriculture'
		when "seller np name" like '%enam.gov%' then 'Agriculture'
		when "seller np name" like '%crofarm%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'Grocery'
		when "seller np name" like '%rebelfoods%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" like '%uengage%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" = 'api.kiko.live/ondc-seller'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'Grocery'
		when "item category" = 'F&B' then 'F&B'
		when "item category" = 'Grocery' then 'Grocery'
		when "item category" is not null
		and "item consolidated category" is null then 'Others'
		when "item category" is null then 'Undefined'
		else "item consolidated category"
	end))
order by
		(
	case
		when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" like '%dominos%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "item consolidated category" like 'Agri%'
		or "domain" like '%AGR%' then 'Agriculture'
		when "seller np name" like '%agrevolution%' then 'Agriculture'
		when "seller np name" like '%enam.gov%' then 'Agriculture'
		when "seller np name" like '%crofarm%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'Grocery'
		when "seller np name" like '%rebelfoods%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" like '%uengage%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" = 'api.kiko.live/ondc-seller'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'Grocery'
		when "item category" = 'F&B' then 'F&B'
		when "item category" = 'Grocery' then 'Grocery'
		when "item category" is not null
		and "item consolidated category" is null then 'Others'
		when "item category" is null then 'Undefined'
		else "item consolidated category"
	end) ) max_record_key,
	case
		when "seller np name" = 'webapi.magicpin.in/oms_partner/ondc'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" like '%dominos%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "item consolidated category" like 'Agri%'
		or "domain" like '%AGR%' then 'Agriculture'
		when "seller np name" like '%agrevolution%' then 'Agriculture'
		when "seller np name" like '%enam.gov%' then 'Agriculture'
		when "seller np name" like '%crofarm%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'Grocery'
		when "seller np name" like '%rebelfoods%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" like '%uengage%'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" = 'api.esamudaay.com/ondc/sdk/bpp/retail/lespl'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'F&B'
		when "seller np name" = 'api.kiko.live/ondc-seller'
		and "item consolidated category" is null
		or "item consolidated category" = '' then 'Grocery'
		when "item category" = 'F&B' then 'F&B'
		when "item category" = 'Grocery' then 'Grocery'
		when "item category" is not null
		and "item consolidated category" is null then 'Others'
		when "item category" is null then 'Undefined'
		else "item consolidated category"
	end as "item consolidated category",
	date(date_parse("O_Created Date & Time",
		'%Y-%m-%dT%H:%i:%s')) as "Date",
	case
		when not("order status" in ('Cancelled', 'Completed')
		or ("order status" like '%Return%')) then 
	(case
			when ("o_completed on date & time" is not null
			or "F_Order Delivered at Date & Time From Fulfillments" is not null)
			and "o_cancelled at date & time" is null then 'Completed'
			when "o_completed on date & time" is null
			and "F_Order Delivered at Date & Time From Fulfillments" is null
			and "o_cancelled at date & time" is not null then 'Cancelled'
			when ("o_completed on date & time" is not null
			or "F_Order Delivered at Date & Time From Fulfillments" is not null)
			and "o_cancelled at date & time" is not null then "order status"
			else "order status"
		end)
		else "order status"
	end as "order status"
from
	"default".shared_order_fulfillment_nhm_fields_view_hudi
where
	(case 
				when upper(on_confirm_sync_response) = 'NACK' then 1
		when on_confirm_error_code is not null then 1
		else 0
	end) = 0
	and date(date_parse("O_Created Date & Time",
		'%Y-%m-%dT%H:%i:%s')) >= DATE('2024-01-01'))
		,							
table1 as (
select
	"seller np name" as "Seller NP",
	"buyer np name" as "Buyer NP",
	"max_record_key",
	"Date",
	"fulfillment status",
	"item consolidated category",
	"network order id" as "Network order id",
	case
		when "order status" is null
			or "order status" = '' then 'In Process'
			when "order status" = 'Cancelled' then 'Cancelled'
			when "order status" = 'Completed' then 'Delivered'
			when lower("order status") = 'delivered' then 'Delivered'
			when "order status" like 'Liquid%' then 'Delivered'
			when "order status" like '%leted' then 'Delivered'
			when "order status" like 'Return%' then 'Delivered'
			when "fulfillment status" like 'RTO%' then 'Cancelled'
			else 'In Process'
		end as "ONDC order_status"
	from
			base_table),
merger_table as (
select
	"Network order id",
	ARRAY_JOIN(ARRAY_AGG(distinct "ONDC order_status"
order by
	"ONDC order_status"),
	',') as "ONDC order_status",
	ARRAY_JOIN(ARRAY_AGG(distinct "item consolidated category"
order by
	"item consolidated category"),
	',') as "item consolidated category"
from
	table1
group by
	1),
trial as 
(
select
	t1."Buyer NP",
	t1."Seller NP",
	t1."Network order id",
	t1."Date",
	mt."ONDC order_status",
	mt."item consolidated category"
from
	table1 t1
join merger_table mt on
	t1."Network order id" = mt."Network order id"
where
	t1."max_record_key" = 1),
table_l as (
select
	"Buyer NP",
	"Seller NP",
	"Network order id",
	case
		when "ONDC order_status" like '%,%' then 'In Process'
		else "ONDC order_status"
	end as "ONDC order_status",	
	case
		when "item consolidated category" like '%F&B%'
			and "item consolidated category" like '%Undefined%' then 'F&B'
			when "item consolidated category" like '%,%' then 'Multi Category'
			else "item consolidated category"
		end as "Category",
		max("Date") as "Date",
		case
			when row_number() over (partition by "Network order id"
		order by
				MAX("Date") desc) > 1 then 0
			else 1
		end as "no_key"
	from
		trial
	group by
		1,
		2,
		3,
		4,
		5)
select
	"Date",
	"Seller NP",
	"Buyer NP",
	"Category",
	SUM("no_key") as confirmed,
	SUM(case when "ONDC order_status" = 'Delivered' then "no_key" else 0 end) as delivered
from
	table_l
group by
	1,
	2,
	3,
	4
