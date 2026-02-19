This is a docker-compose setup

db_scripts houses scripts to fetch data from api and then build derivative tables.
These tables power backend api and then frontend service pulls in data from backend api.

For new features, Most logic should be handled as part of the db_scripts. Sometimes although not ideal that may be housed in backend api. And very rarely in the frontend . Frontend should almost always just render and draw not be responsible for business logic.