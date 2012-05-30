boss_db:start([{adapter, dynamodb}, {ddb_eventually_consistent_tables, [<<"boss_db_test_parent_models">>]}]).
boss_news:start().
boss_record_compiler:compile(filename:join(["../", "priv", "test_models", "boss_db_test_parent_model.erl"])).
X = boss_db_test_parent_model:new("boss_db_test_parent_model-id1", "foo"). %% ID must start with module name
X:save().
Y = boss_db:find(X:id()).
Y.
boss_db:delete(X:id()).
Z = boss_db:find(X:id()).
Z.