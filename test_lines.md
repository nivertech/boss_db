
boss_db DDB tests lines:

``` erlang
boss_db:start([{adapter, dynamodb}, {ddb_eventually_consistent_tables, [<<"boss_db_test_parent_models">>]}]).
boss_news:start().
boss_record_compiler:compile(filename:join(["../", "priv", "test_models", "boss_db_test_parent_model.erl"])).
X = boss_db_test_parent_model:new("boss_db_test_parent_model-id1", "foo"). %% ID must start with module name
X:save().
boss_db:find(X:id()).
X = boss_db_test_parent_model:new("boss_db_test_parent_model-id2", 23). %% ID must start with module name
X:save().
boss_db:find(X:id()).
X = boss_db_test_parent_model:new("boss_db_test_parent_model-id3", {string_set, sets:from_list([<<"kuku">>, <<"pupu">>])}). %% ID must start with module name
X:save().
boss_db:find(X:id()).
X = boss_db_test_parent_model:new("boss_db_test_parent_model-id4", {number_set, sets:from_list([1089, 523])}). %% ID must start with module name
X:save().
boss_db:find(X:id()).
```
