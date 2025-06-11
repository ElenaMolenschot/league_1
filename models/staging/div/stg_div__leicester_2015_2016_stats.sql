with 

source as (

    select * from {{ source('div', 'leicester_2015_2016_stats') }}

),

renamed as (

    select
        league,
        match_date,
        matchweek,
        home_team,
        home_formation,
        home_score,
        home_xg,
        home_goals,
        home_yellow_cards,
        home_red_cards,
        away_team,
        away_formation,
        away_score,
        away_xg,
        away_goals,
        away_yellow_cards,
        away_red_cards,
        game_url,
        team,
        home_away,
        player,
        player_num,
        nation,
        pos,
        age,
        min,
        gls,
        ast,
        pk,
        pkatt,
        sh,
        sot,
        crdy,
        crdr,
        fls,
        fld,
        off,
        crs,
        tklw,
        int,
        og,
        pkwon,
        pkcon

    from source

)

select * from renamed
