WITH
 -- parse source string
 srcjson AS (
   SELECT $1::JSONB
   AS data
 ),
 -- split into data / included
 matches AS (
   SELECT JSONB_ARRAY_ELEMENTS(srcjson.data->'data') AS data FROM srcjson
 ),
 includes AS (
   SELECT JSONB_ARRAY_ELEMENTS(srcjson.data->'included') AS data FROM srcjson
 ),
 -- filter included by type
 rosters AS (
   SELECT includes.data AS data FROM includes WHERE includes.data->>'type'='roster'
 ),
 participants AS (
   SELECT includes.data AS data FROM includes WHERE includes.data->>'type'='participant'
 ),
 players AS (
   SELECT includes.data AS data FROM includes WHERE includes.data->>'type'='player'
 ),
 -- link participant-player
 linked_participants AS (
   SELECT
     JSONB_BUILD_OBJECT(
       'data', participants.data - 'relationships',
       'relations', participants.data->'relationships'->'player'
       -- stop: don't embed player object, it will be put into a seperate table
       -- instead, only reference the player ids in a jsonb array
     ) AS data
     FROM participants
  ),
 -- link roster-participants
 linked_rosters AS (
   SELECT
     JSONB_BUILD_OBJECT(
     'data', rosters.data - 'relationships',
     'relations', TO_JSONB(ARRAY(
       SELECT * FROM linked_participants
       WHERE rosters.data->'relationships'->'participants'->'data' @> JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('id', linked_participants.data->'data'->>'id', 'type', linked_participants.data->'data'->>'type'))
     ))
   ) AS data
   FROM rosters
 ),
 -- link match-rosters
 linked_matches AS (
   SELECT
   matches.data->>'id' AS id,
   matches.data->>'type' AS type,
   matches.data->'attributes' AS attributes,
   TO_JSONB(ARRAY(
     SELECT * FROM linked_rosters
     WHERE matches.data->'relationships'->'rosters'->'data' @> JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('id', linked_rosters.data->'data'->>'id', 'type', linked_rosters.data->'data'->>'type'))
   )) AS relations
  FROM matches
 ),
 -- cleanup players
 linked_players AS (
   SELECT
   players.data->>'id' AS id,
   players.data->>'type' AS type,
   players.data->'attributes' AS attributes
   FROM players
 ),
 -- insert!
 insert_matches AS (
   INSERT INTO match SELECT * FROM linked_matches
   ON CONFLICT(id) DO NOTHING
   RETURNING id -- TODO conflict shouldn't happen in prod
 ),
 insert_players AS (
   INSERT INTO player SELECT * FROM linked_players
   ON CONFLICT(id) DO
   UPDATE SET attributes=EXCLUDED.attributes
   WHERE (player.attributes->'stats'->>'played')::int < (EXCLUDED.attributes->'stats'->>'played')::int
   RETURNING id
 )
 SELECT * FROM insert_matches
 UNION
 SELECT * FROM insert_players
