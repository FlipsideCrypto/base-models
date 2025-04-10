DBT_TARGET ?= dev

cleanup_time:
	rm -f package-lock.yml && dbt clean && dbt deps

deploy_github_actions:
	dbt run -s livequery_base.deploy.marketplace.github --vars '{"UPDATE_UDFS_AND_SPS":True}' -t $(DBT_TARGET)
	dbt run -m "fsc_evm,tag:gha_tasks" --full-refresh -t $(DBT_TARGET)
	dbt run-operation fsc_evm.create_gha_tasks --vars '{"START_GHA_TASKS":False}' -t $(DBT_TARGET)

deploy_new_github_action:
	dbt run-operation fsc_evm.drop_github_actions_schema -t $(DBT_TARGET)
	dbt run -m "fsc_evm,tag:gha_tasks" --full-refresh -t $(DBT_TARGET)
	dbt run-operation fsc_evm.create_gha_tasks --vars '{"START_GHA_TASKS":False}' -t $(DBT_TARGET)

deploy_livequery:
	dbt run-operation fsc_evm.drop_livequery_schemas --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET)
	dbt run -m livequery_base.deploy.core --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET)
	dbt run-operation fsc_evm.livequery_grants --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET)

deploy_chain_phase_1:
	dbt run -m livequery_base.deploy.core --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET)
	dbt run-operation fsc_evm.livequery_grants --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET)
	dbt run-operation fsc_evm.create_evm_streamline_udfs --vars '{"UPDATE_UDFS_AND_SPS": true}' -t $(DBT_TARGET)
	dbt run-operation fsc_evm.call_sample_rpc_node -t $(DBT_TARGET)
	dbt run -m "fsc_evm,tag:phase_1" --full-refresh --vars '{"GLOBAL_STREAMLINE_FR_ENABLED": true}' -t $(DBT_TARGET)
	# kick chainhead workflow
	# wait ~10 minutes
	# run deploy_chain_phase_2

deploy_chain_phase_2:
	dbt run -m "fsc_evm,tag:phase_2" --full-refresh --vars '{"GLOBAL_STREAMLINE_FR_ENABLED": true, "GLOBAL_BRONZE_FR_ENABLED": true, "GLOBAL_SILVER_FR_ENABLED": true, "GLOBAL_GOLD_FR_ENABLED": true}' -t $(DBT_TARGET)
	make deploy_github_actions -t $(DBT_TARGET)
	# tasks set to SUSPEND by default
	# kick alter_gha_task workflow to RESUME individual tasks, as needed

deploy_chain_phase_3:
	dbt run -m "fsc_evm,tag:phase_3" --full-refresh --vars '{"GLOBAL_BRONZE_FR_ENABLED": true, "GLOBAL_SILVER_FR_ENABLED": true, "GLOBAL_GOLD_FR_ENABLED": true}' -t $(DBT_TARGET)
	# kick alter_gha_task workflow to RESUME individual tasks, as needed

.PHONY: deploy_github_actions cleanup_time deploy_new_github_action deploy_chain_phase_1 deploy_chain_phase_2 deploy_chain_phase_3 deploy_livequery