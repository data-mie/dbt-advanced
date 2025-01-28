{% snapshot customer_snapshot %}

{{
    config(
        unique_key = 'ID' ,
        strategy = 'check',
        check_cols = 'all', 
        schema = 'snapshots' 
         
    )
}}

Select * from {{source('ecomm','customers')}}

{% endsnapshot %}

-- instead of "all" i can write te columns i want to check like ['status','amount']
-- if you write "_snapshot" and click enter ig gives the full logic 
