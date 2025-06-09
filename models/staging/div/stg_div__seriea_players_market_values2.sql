with 

source as (

    select * from {{ source('div', 'seriea_players_market_values2') }}

),

renamed as (

    select
        player_name,
        team,
        market_value_eur

    from source

)

select * from renamed
