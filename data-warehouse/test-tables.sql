select count(*) from staging_songs;

select * from staging_events where user_id is null;

select distinct(auth) from staging_events;

select * from users;

select * from songs;