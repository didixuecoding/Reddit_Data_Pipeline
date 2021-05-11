class SqlQueries:
    dim_author_insert = ("""
INSERT INTO {table:} (
    "author_id",
    "name",
    "created",
    "karma_posts",
    "karma_comments",
    "karma",
    "deleted"
)
SELECT DISTINCT
    LOWER(sa."author"),
    sa."author",
    TIMESTAMP 'epoch' + sa."created" * INTERVAL '1 second',
    sa."karma_posts",
    sa."karma_comments",
    (sa."karma_posts" + sa."karma_comments"),
    CASE WHEN
            (sa."created" IS NOT NULL
            AND sa."karma_posts" IS NOT NULL
            AND sa."karma_comments" IS NOT NULL)
        THEN false ELSE true END
FROM staging_authors sa
WHERE sa."author_valid" = '1';
""")


    dim_subreddit_insert = ("""
INSERT INTO {table:} (
    "subreddit_id",
    "accounts_active",
    "accounts_active_is_fuzzed",
    "active_user_count",
    "advertiser_category",
    "all_original_content",
    "allow_discovery",
    "allow_images",
    "allow_videogifs",
    "allow_videos",
    "can_assign_link_flair",
    "can_assign_user_flair",
    "comment_score_hide_mins",
    "community_icon",
    "created",
    "description",
    "display_name",
    "display_name_prefixed",
    "emojis_enabled",
    "free_form_reports",
    "header_img",
    "header_title",
    "hide_ads",
    "key_color",
    "lang",
    "name",
    "notification_level",
    "original_content_tag_enabled",
    "over_18",
    "primary_color",
    "public_description",
    "public_traffic",
    "quarantine",
    "show_media",
    "show_media_preview",
    "spoilers_enabled",
    "submission_type",
    "submit_link_label",
    "submit_text",
    "submit_text_label",
    "subreddit_type",
    "subscribers",
    "suggested_comment_sort",
    "title",
    "url",
    "videostream_links_count",
    "whitelist_status",
    "wiki_enabled",
    "wls"
)
SELECT DISTINCT
    LOWER(sr."display_name"),
    sr."accounts_active",
    sr."accounts_active_is_fuzzed",
    sr."active_user_count",
    sr."advertiser_category",
    sr."all_original_content",
    sr."allow_discovery",
    sr."allow_images",
    sr."allow_videogifs",
    sr."allow_videos",
    sr."can_assign_link_flair",
    sr."can_assign_user_flair",
    sr."comment_score_hide_mins",
    sr."community_icon",
    TIMESTAMP 'epoch' + sr."created_utc" * INTERVAL '1 second',
    sr."description",
    sr."display_name",
    sr."display_name_prefixed",
    sr."emojis_enabled",
    sr."free_form_reports",
    sr."header_img",
    sr."header_title",
    sr."hide_ads",
    sr."key_color",
    sr."lang",
    sr."name",
    sr."notification_level",
    sr."original_content_tag_enabled",
    sr."over18",
    sr."primary_color",
    sr."public_description",
    sr."public_traffic",
    sr."quarantine",
    sr."show_media",
    sr."show_media_preview",
    sr."spoilers_enabled",
    sr."submission_type",
    sr."submit_link_label",
    sr."submit_text",
    sr."submit_text_label",
    sr."subreddit_type",
    sr."subscribers",
    sr."suggested_comment_sort",
    sr."title",
    sr."url",
    sr."videostream_links_count",
    sr."whitelist_status",
    sr."wiki_enabled",
    sr."wls"
FROM staging_subreddits sr;""")


    fact_submission_insert = ("""
INSERT INTO fact_submission (
    "submission_id",
    "author_id",
    "subreddit_id",
    "archived",
    "can_gild",
    "can_mod_post",
    "category",
    "contest_mode",
    "created",
    "discussion_type",
    "domain",
    "edited",
    "event_end",
    "event_is_live",
    "event_start",
    "gilded",
    "hidden",
    "is_crosspostable",
    "is_meta",
    "is_original_content",
    "is_reddit_media_domain",
    "is_robot_indexable",
    "is_self",
    "is_video",
    "locked",
    "no_follow",
    "num_comments",
    "num_crossposts",
    "over_18",
    "permalink",
    "pinned",
    "post_hint",
    "quarantine",
    "removal_reason",
    "score",
    "selftext",
    "spoiler",
    "stickied",
    "suggested_sort",
    "thumbnail",
    "thumbnail_height",
    "thumbnail_width",
    "title",
    "total_awards_received",
    "url",
    "whitelist_status"
)
SELECT DISTINCT
    ss."id",
    LOWER(ss."author"),
    LOWER(ss."subreddit"),
    ss."archived",
    ss."can_gild",
    ss."can_mod_post",
    ss."category",
    ss."contest_mode",
    TIMESTAMP 'epoch' + ss."created_utc" * INTERVAL '1 second',
    ss."discussion_type",
    ss."domain",
    CASE WHEN ss."edited" IS NOT NULL THEN true ELSE false END,
    CASE WHEN ss."event_end" IS NOT NULL THEN 
        TIMESTAMP 'epoch' + ss."event_end" * INTERVAL '1 second'
        ELSE NULL END,
    CASE WHEN ss."event_is_live" IS NULL THEN false ELSE ss."event_is_live" END,
    CASE WHEN ss."event_start" IS NOT NULL THEN 
        TIMESTAMP 'epoch' + ss."event_start" * INTERVAL '1 second'
        ELSE NULL END,
    ss."gilded",
    ss."hidden",
    ss."is_crosspostable",
    ss."is_meta",
    ss."is_original_content",
    ss."is_reddit_media_domain",
    ss."is_robot_indexable",
    ss."is_self",
    ss."is_video",
    ss."locked",
    ss."no_follow",
    ss."num_comments",
    ss."num_crossposts",
    ss."over_18",
    ss."permalink",
    ss."pinned",
    ss."post_hint",
    ss."quarantine",
    ss."removal_reason",
    ss."score",
    ss."selftext",
    ss."spoiler",
    ss."stickied",
    CASE WHEN ss."suggested_sort" IS NULL THEN 'default' else ss."suggested_sort" END,
    ss."thumbnail",
    ss."thumbnail_height",
    ss."thumbnail_width",
    ss."title",
    ss."total_awards_received",
    ss."url",
    CASE WHEN ss."whitelist_status" IS NULL THEN 'default' else ss."whitelist_status" END
FROM staging_submissions ss;""")

    staging_times_insert = ("""
INSERT INTO {table:} ( "start_time")
SELECT DISTINCT created
    FROM dim_author
    WHERE created IS NOT NULL;
INSERT INTO {table:} ( "start_time")
SELECT DISTINCT created FROM dim_subreddit;
INSERT INTO {table:} ( "start_time")
SELECT DISTINCT created FROM fact_submission;
INSERT INTO {table:} ( "start_time")
SELECT DISTINCT event_start
    FROM fact_submission
    WHERE event_start IS NOT NULL;
INSERT INTO {table:} ( "start_time")
SELECT DISTINCT event_end
    FROM fact_submission
    WHERE event_start IS NOT NULL;""")


    dim_time_insert = ("""
INSERT INTO {table:} (
    "start_time",
    "hour",
    "day",
    "week",
    "month",
    "year",
    "weekday"
)
SELECT DISTINCT
    "start_time",
    CAST(DATE_PART('hour',  "start_time") as INT),
    CAST(DATE_PART('day',   "start_time") as INT),
    CAST(DATE_PART('week',  "start_time") as INT),
    CAST(DATE_PART('month', "start_time") as INT),
    CAST(DATE_PART('year',  "start_time") as INT),
    CAST(DATE_PART('dow',   "start_time") as INT)
FROM staging_times;""")