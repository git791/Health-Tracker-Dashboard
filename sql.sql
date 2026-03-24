-- Grant the app's service principal access to the tables
GRANT USE CATALOG ON CATALOG workspace TO `b4c537c8-98ac-49bb-a0a1-abd5dbb1c45d`;
GRANT USE SCHEMA ON SCHEMA workspace.default TO `b4c537c8-98ac-49bb-a0a1-abd5dbb1c45d`;
GRANT SELECT ON TABLE workspace.default.disease_hotspots_geo TO `b4c537c8-98ac-49bb-a0a1-abd5dbb1c45d`;
GRANT SELECT ON TABLE workspace.default.processed_disease_mentions TO `b4c537c8-98ac-49bb-a0a1-abd5dbb1c45d`;
GRANT SELECT ON TABLE workspace.default.raw_news_articles TO `b4c537c8-98ac-49bb-a0a1-abd5dbb1c45d`;
GRANT SELECT ON TABLE workspace.default.outbreak_clusters TO `b4c537c8-98ac-49bb-a0a1-abd5dbb1c45d`;
