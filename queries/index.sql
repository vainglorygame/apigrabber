CREATE INDEX ON match((data->>'id'));
CREATE INDEX ON roster((data->>'id'));
CREATE INDEX ON participant((data->>'id'));
CREATE INDEX ON match((data->'attributes'->>'createdAt'));
