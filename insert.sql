WITH
-- parse source string
srcjson AS (
  SELECT $1::JSONB AS data
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
-- cleanup players
linked_players AS (
  SELECT
    JSONB_BUILD_OBJECT(
      'data', players.data-'relationships'
    ) AS data
  FROM players
),
-- link participant-player
linked_participants AS (
  SELECT
    JSONB_BUILD_OBJECT(
      'data', participants.data-'relationships',
      'relations',  TO_JSONB(ARRAY(
        SELECT * FROM linked_players
        WHERE participants.data->'relationships'->'player'->'data' = JSONB_BUILD_OBJECT('id', linked_players.data->'data'->>'id', 'type', linked_players.data->'data'->>'type')
      ))
    ) AS data
  FROM participants
),
 -- link roster-participants
linked_rosters AS (
  SELECT
    JSONB_BUILD_OBJECT(
      'data', rosters.data-'relationships',
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
    JSONB_BUILD_OBJECT(
      'data', matches.data-'relationships',
      'relations', TO_JSONB(ARRAY(
        SELECT * FROM linked_rosters
        WHERE matches.data->'relationships'->'rosters'->'data' @> JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('id', linked_rosters.data->'data'->>'id', 'type', linked_rosters.data->'data'->>'type'))
      ))
    ) AS data
  FROM matches
),
-- insert!
insert_matches AS (
  INSERT INTO match(id, data) SELECT * FROM linked_matches WHERE linked_matches.data->'data'->'attributes'->>'gameMode'='casual' OR linked_matches.data->'data'->'attributes'->>'gameMode'='ranked'
  ON CONFLICT(id) DO NOTHING
  RETURNING id
)
SELECT DISTINCT id FROM insert_matches
