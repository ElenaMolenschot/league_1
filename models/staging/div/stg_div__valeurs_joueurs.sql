with 

source as (

    select * from {{ source('div', 'valeurs_joueurs') }}

),

renamed as (

    select
        player_name,
        team,
        market_value_eur

    from source

)

select * from renamed
