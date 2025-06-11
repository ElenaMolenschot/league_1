with 

source as (

    select * from {{ source('div', 'gk_bundesliga') }}

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
        nation,
        age,
        min,
        sota_shot_stopping,
        ga_shot_stopping,
        saves_shot_stopping,
        save_percent_shot_stopping,
        psxg_shot_stopping,
        cmp_launched,
        att_launched,
        cmp_percent_launched,
        att_gk_passes,
        thr_passes,
        launch_percent_passes,
        avglen_passes,
        att_goal_kicks,
        launch_percent_goal_kicks,
        avglen_goal_kicks,
        opp_crosses,
        stp_crosses,
        stp_percent_crosses,
        player_numopa_sweeper,
        avgdist_sweeper

    from source

)

select * from renamed
