# dbt Advanced ILT — Lab Solutions

## Lab 5: Snapshots and Seeds

### Exercise 1 — Snapshot the customers table

In `snapshots/customers_snapshot.yml`, add:

```yaml
snapshots:
  - name: customers_snapshot
    relation: source('ecomm', 'customers')
    config:
      unique_key: id
      strategy: check
      check_cols: all
      schema: snapshots
```

---

### Exercise 2 — Snapshot the orders table

In `snapshots/orders_snapshot.yml`, add:

```yaml
snapshots:
  - name: orders_snapshot
    relation: source('ecomm', 'orders')
    config:
      unique_key: id
      strategy: timestamp
      updated_at: _synced_at
      schema: snapshots
```

---

### Exercise 3 — Add the stores seed file to our project

Upload `stores.csv` to the `seeds/` folder and run `dbt seed -s stores`.

**stores.csv**
```csv
store_id,store_name
1,United States
2,Germany
3,Australia
```

Update the `orders` model to join with the stores seed:

```sql
{{ 
    config(
        materialized='table'
    ) 
}}

with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
),

deliveries as (
    select *
    from {{ ref('stg_ecomm__deliveries') }}
),

deliveries_filtered as (
    select *
    from deliveries
    where delivery_status = 'delivered'
),

stores as ( -- added
    select *
    from {{ ref('stores') }}
),

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection,
        stores.store_name -- added
    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
    left join stores -- added
        on orders.store_id = stores.store_id
),

final as (
    select *
    from joined
)

select *
from final
```

---

### [Bonus] Exercise 4 — Cleaning a messy CASE-WHEN statement

Add `order_status.csv` to `seeds/`:

```csv
order_status,order_status_normalized
ordered,Ordered
order_created,Ordered
shipped,Shipped
sent,Shipped
pending,Pending
waiting,Pending
processing,Pending
payment_pending,Pending
canceled,Canceled
cancelled,Canceled
delivered,Delivered
```

Replace the CASE statement in `models/staging/stg_ecomm__orders` with the `order_status` seed:

```sql
with source as (
    select *
    from {{ source('ecomm', 'orders') }}
),

renamed as (
    select
        *,
        id as order_id,
        created_at as ordered_at,
        lower(status) as order_status -- updated
    from source
),

order_status as ( -- added
    select *
    from {{ ref('order_status') }}
),

normalize_order_status as (
    select
        renamed.*,
        coalesce(order_status.order_status_normalized, 'Unknown') as order_status_normalized -- added
    from renamed
    left join order_status -- added
        on renamed.order_status = order_status.order_status
),

final as (
    select *
    from normalize_order_status
)

select *
from final
```

---

## Lab 6: Incremental Models

### Exercise 1 — Make the orders model incremental

Create a new `orders_incremental` model (duplicate from `orders`) and add the `incremental` materialization with the `is_incremental()` macro:

```sql
{{
    config(
        materialized='incremental'
    )
}}

with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
    {% if is_incremental() %} -- added
        where ordered_at > (select max(ordered_at) from {{ this }})
    {% endif %}
),

deliveries as (
    select *
    from {{ ref('stg_ecomm__deliveries') }}
),

deliveries_filtered as (
    select *
    from deliveries
    where delivery_status = 'delivered'
),

stores as (
    select *
    from {{ ref('stores') }}
),

joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection,
        stores.store_name
    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
    left join stores
        on orders.store_id = stores.store_id
),

final as (
    select *
    from joined
)

select *
from final
```

> On the **first run**, dbt creates a table and the WHERE clause is ignored. On **subsequent runs**, dbt inserts only the filtered new rows.

---

### Exercise 2 — Pre-analysis: check how late orders arrive in our system

Run in Snowflake to understand the lag distribution:

```sql
select
    datediff('day', created_at, _synced_at) as days_lag,
    count(*)
from raw.ecomm.orders_us
group by 1
order by 1
```

---

### Exercise 3 — Add a lookback window to the orders model to account for late-arriving facts

Add `unique_key` and a 3-day lookback window to `orders_incremental`:

```sql
{{
    config(
        materialized='incremental',
        unique_key = 'order_id' -- added
    )
}}

-- ... (same CTEs as above)

{% if is_incremental() %}
    where ordered_at > (select dateadd('day', -3, max(ordered_at)) from {{ this }}) -- updated
{% endif %}
```

---

### [Bonus] Exercise 4 — Explore your options on a schema change

Add a `last_updated` column and configure `on_schema_change` with `sync_all_columns` option:

```sql
{{
    config(
        materialized='incremental',
        unique_key = 'order_id',
        on_schema_change = 'sync_all_columns' -- added
    )
}}

with orders as (
    select *
    from {{ ref('stg_ecomm__orders') }}
    {% if is_incremental() %}
        where ordered_at > (select dateadd('day', -3, max(ordered_at)) from {{ this }})
    {% endif %}
),

-- ... (same CTEs as above)

final as (
    select
        *,
        current_timestamp() as last_updated -- added
    from joined
)

select *
from final
```

When dbt detects the new column, you'll see an `ALTER TABLE` in the logs:

```sql
alter table "BREAKOUT_LABS"."DBT_ADVANCED_PATRICIA"."ORDERS_INCREMENTAL"
  add column "LAST_UPDATED" TIMESTAMP_LTZ
```

---

## Lab 7: Advanced SQL in dbt

### Exercise 1 — Add a `days_since_last_order` column to the orders model

Add the `days_since_last_order` column using `lag` window function to the `orders` model:

```sql
joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.ordered_at,
        orders.order_status,
        orders.total_amount,
        datediff(
            'minutes', orders.ordered_at, deliveries_filtered.delivered_at
        ) as delivery_time_from_order,
        datediff(
            'minutes',
            deliveries_filtered.picked_up_at,
            deliveries_filtered.delivered_at
        ) as delivery_time_from_collection,
        datediff(
            'days',
            lag(orders.ordered_at) over (partition by orders.customer_id order by orders.ordered_at),
            orders.ordered_at
        ) as days_since_last_order -- added
    from orders
    left join deliveries_filtered
        on orders.order_id = deliveries_filtered.order_id
),
```

---

### Exercise 2 — Investigate duplicate orders

Query to identify duplicates in the source:

```sql
select *
from raw.ecomm.orders_au_duped
where id in (
    select id
    from raw.ecomm.orders_au_duped
    group by 1
    having count(*) > 1
)
order by id
```

> All column values are identical except for `_synced_at`, indicating duplicates introduced by the EL process rather than the source system.

Create an ephemeral staging model to deduplicate using `QUALIFY`:

```sql
-- models/staging/ecomm/_orders_au_deduped.sql

{{ 
    config(
        materialized='ephemeral'
    ) 
}}

select *
from {{ source('ecomm', 'orders_au_duped') }}
qualify row_number() over (
    partition by id
    order by _synced_at desc
) = 1
```

---

### Exercise 3 — Extract JSON fields from products data

Create `products.sql` in the `analyses/` folder:

```sql
select
    id,
    name,
    category,
    subcategory,
    unit_price,
    is_active,
    created_at,
    _synced_at,
    variant.value:title::string as product_title
from raw.ecomm.products,
    lateral flatten(input => products.variants) as variant
```

---

## Lab 8: Optimizing dbt for Snowflake

### Exercise 1 — Configure the orders model to use a bigger warehouse

Add `snowflake_warehouse` to the `orders` model config:

```sql
{{
    config(
        materialized='table',
        snowflake_warehouse = 'TRANSFORMING_S'
    )
}}
```

Confirm in the dbt logs:

```sql
use warehouse TRANSFORMING_S;
```

Then verify in Snowflake under **Monitoring > Query History**.

---

### Exercise 2 — Add query tags to all dbt models

In `dbt_project.yml` add a query tag for all the models:

```yaml
models:
  dbt_training:
    +query_tag: 'dbt_course' -- added
    +materialized: view
```

Confirm in the logs:

```sql
alter session set query_tag = 'dbt_course'
```

Query Snowflake's query history by tag:

```sql
select
    query_tag,
    query_id,
    database_name,
    schema_name,
    query_type,
    user_name,
    warehouse_name,
    warehouse_size,
    bytes_scanned,
    rows_produced,
    partitions_scanned
from snowflake.account_usage.query_history
where query_tag = 'dbt_course'
limit 10;
```

---

### Exercise 3 — Debug a join explosion in Snowflake

Run the provided query in Snowflake and inspect the Query Profile — you'll see a **CartesianJoin**:

```sql
SELECT
    o.*
        EXCLUDE (_synced_at)
        RENAME (id as order_id),
    c.email as customer_email
FROM (
    SELECT
        o.*,
        d.delivered_at
    FROM raw.ecomm.orders_us o
    INNER JOIN raw.ecomm.deliveries d ON (o.id = d.order_id)
) o
JOIN raw.ecomm.customers c  -- missing ON clause causes the explosion
```

**Fix** — add the missing `ON` clause:

```sql
JOIN raw.ecomm.customers c ON (o.customer_id = c.id)
```

Refactored as a clean CTE model:

```sql
with orders as (
    select * from raw.ecomm.orders_us
),

customers as (
    select * from raw.ecomm.customers
),

deliveries as (
    select * from raw.ecomm.deliveries
)

select
    orders.id as order_id,
    orders.total_amount,
    orders.status,
    orders.store_id,
    orders.created_at,
    orders.customer_id,
    deliveries.delivered_at,
    customers.email as customer_email
from orders
inner join deliveries on (orders.id = deliveries.order_id)
inner join customers on (orders.customer_id = customers.id)
```

---

## Lab 9: Project Conventions and dbt Packages

### Exercise 1 — Add orders table for new ecommerce stores

Update `sources.yml` to add the German and Australian stores:

```yaml
version: 2

sources:
  - name: ecomm
    database: raw
    tables:
      - name: customers
        description: Each record represents a customer in our ecommerce application.
        columns:
          - name: id
            description: '{{ doc("customer_id") }}'
      - name: orders_us
        description: Each record represents an order from the US store.
        columns:
          - name: id
            tests:
              - unique
              - not_null
      - name: orders_de
        description: Each record represents an order from the Germany store.
      - name: orders_au
        description: Each record represents an order from the Australia store.
      - name: deliveries
        description: Each record represents an order delivery.
```

Update `stg_ecomm__orders.sql` to reference `orders_us`:

```sql
with source as (
    select *
    from {{ source('ecomm', 'orders_us') }}  -- updated
),
```

Update `orders_snapshot.yml`:

```yaml
snapshots:
  - name: orders_snapshot
    relation: source('ecomm', 'orders_us')  -- updated
    config:
      unique_key: id
      strategy: timestamp
      updated_at: _synced_at
      schema: snapshots
```

---

### Exercise 2 — Add the `dbt_utils` package

Create `packages.yml` with the following content:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.3
```

Run `dbt deps` to install.

---

### Exercise 3 — Create an unified orders model

Use `dbt_utils.union_relations` to combine all three store tables in `stg_ecomm__orders`:

```sql
with sources as (
{{
    dbt_utils.union_relations(
        relations=[
            source('ecomm', 'orders_us'),
            source('ecomm', 'orders_de'),
            source('ecomm', 'orders_au')
        ],
    )
}}
)
```

Add `store_id` column derived from `_dbt_source_relation` column generated by the `union_relations` macro:

```sql 
add_store_id as (
    select
        * exclude (store_id),    -- Omit original store_id column
        case
            when right(_dbt_source_relation, 2) = 'us' then 1
            when right(_dbt_source_relation, 2) = 'de' then 2
            when right(_dbt_source_relation, 2) = 'au' then 3
        end as store_id            -- Add calculated store_id
    from sources
),
```

Add tests to the model:

```yaml
models:
  - name: stg_ecomm__orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: store_id
        tests:
          - not_null
```

---

### [Bonus] Exercise 4 — Explore the Elementary data observability package

Add Elementary package to `packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.3
  - package: elementary-data/elementary
    version: 0.20.1
```

> Elementary `on-run-start` and `on-run-end` hooks are now part of the invocation. Everytime you run your models, Elementary models get executed. A couple of new tables are created: `dbt_monitoring_metrics`, `dbt_columns`, `dbt_run_results`, etc

> In the table `dbt_models` (created by Elementy package) there is a column - `depends_on_nodes` - that represents the dbt lineage as visualized in the DAG. 

> You can use this lineage information from that table to calculate the cost of a model, including upstream dependencies, to define data product pricing or monitor resource utilization.

Query to calculate the average model execution times (avaiable in `model_run_results` table):

```sql
select
    unique_id,
    avg(execution_time) as avg_execution_time
from <your-database>.<your-schema>_elementary.model_run_results
where status = 'success'
  and package_name = 'dbt_training'
group by unique_id
order by avg_execution_time desc
```

---

## Lab 10: Model Governance and Data Products

### Exercise 1 — Prepare the orders model

Add SDC standard columns - `pk_orders`, `hk_customer`, `source_last_updated`, and `last_updated` - to the `orders` model:

```sql
select
    {{ dbt_utils.generate_surrogate_key(['order_id']) }}       as pk_orders,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }}    as hk_customer,
    greatest_ignore_nulls(
        orders._synced_at,
        deliveries_filtered._synced_at
    )                                                           as source_last_updated,
    current_timestamp()                                         as last_updated,
    -- ... rest of columns
```

---

### Exercise 2 — Prepare the orders YAML

Add `codegen` to `packages.yml`:

```yaml
packages:
  - package: dbt-labs/codegen
    version: 0.13.1
```

Use the `generate_model_yaml` macro to generate the YAML skeleton (compile only — don't run):

```sql
{{ codegen.generate_model_yaml(['orders']) }}
```

Complete YAML with descriptions, data types, and tests:

```yaml
version: 2

models:
  - name: orders
    description: |
      Orders data product from the dbt Training Lab.
      Each record represents an order from the e-commerce system.

    columns:
      - name: pk_orders
        data_type: varchar
        description: Unique Primary Key based on order_id
        tests:
          - unique
          - not_null

      - name: hk_customer
        data_type: varchar
        description: Foreign Key to the customer based on customer_id

      - name: order_id
        data_type: varchar
        description: Source system generated order ID

      - name: customer_id
        data_type: number
        description: Source system generated customer ID

      - name: ordered_at
        data_type: timestamp_ntz
        description: Timestamp of the generated order

      - name: order_status
        data_type: varchar
        description: "Cleaned order status [Ordered, Shipped, Pending, Canceled, Delivered, Unknown]"

      - name: total_amount
        data_type: float
        description: Order total amount

      - name: delivery_time_from_order
        data_type: number
        description: Delivery time in minutes from order creation
        tests:
          - greater_than_zero

      - name: delivery_time_from_collection
        data_type: number
        description: Delivery time in minutes from parcel pickup
        tests:
          - greater_than_zero

      - name: store_name
        data_type: varchar
        description: Store name (country)

      - name: source_last_updated
        data_type: timestamp_ntz
        description: Latest ingestion timestamp across all included sources

      - name: last_updated
        data_type: timestamp_ltz
        description: Current timestamp of table creation/refresh
```

Enable `persist_docs` in `dbt_project.yml` to push metadata to Snowflake (and from there to Collibra):

```yaml
models:
  dbt_training:
    +persist_docs:
      relation: true
      columns: true
```

Verify the metadata landed in Snowflake:

```sql
desc table <your-database>.<your-schema>.orders;
show tables like 'orders' in schema <your-database>.<your-schema>
```

---

### Exercise 3 — Model contract and version

Enforce the contract for `orders` model in the YAML:

```yaml
- name: orders
  config:
    contract:
      enforced: true
```

Add a `primary_key` constraint to `pk_orders`:

```yaml
- name: pk_orders
  constraints:
    - type: primary_key
  tests:
    - unique
```

Add version v1:

```yaml
models:
  - name: orders
    versions:
      - v: 1
```

Run both versions together. Check the new table name on Snowflake - it's not only `orders` anymore.

```bash
dbt run -s my_data_product
```

Or target a specific version:

```bash
dbt run -s my_data_product_v1
```
