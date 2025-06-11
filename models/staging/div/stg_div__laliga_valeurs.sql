with 

source as (

    select * from {{ source('div', 'laliga_valeurs') }}

),

renamed as (

    select
        player_name,
        team,
        market_value_eur

    from source

)

select * from renamed
