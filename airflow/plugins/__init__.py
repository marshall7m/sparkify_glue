# from airflow.plugins_manager import AirflowPlugin

# from operator_functions.dim_branch import dim_branch
# from operator_functions.load_stl_error_dict import get_error_tables
# from operators.created_table_check import CreatedTableOperator
# from operators.data_quality import DataQualityOperator
# from operators.fact_branch import FactBranchOperator
# from operators.load_dimension import LoadDimensionOperator
# from operators.load_fact import LoadFactOperator
# from operators.stage_redshift import StageToRedshiftOperator
# from operators.stl_load_errors_tables import CreateSTLErrorTablesOperator

# from subdag_operators.fact_dag import create_fact_tables
# from subdag_operators.stage_dim_dag import stage_dim_s3_to_redshift
# from subdag_operators.stage_fact_dag import stage_fact_s3_to_redshift

# from operator_queries import SqlQueries

# class CustomPlugins(AirflowPlugin):
#     name = "CustomPlugins"
#     operators = [
#         StageToRedshiftOperator,
#         LoadFactOperator,
#         LoadDimensionOperator,
#         DataQualityOperator,
#         CreatedTableOperator,
#         CreateSTLErrorTablesOperator,
#         FactBranchOperator,
#     ]

#     functions = [
#         dim_branch,
#         get_error_tables
#     ]

#     subdag_operators = [
#         create_fact_tables,
#         stage_dim_s3_to_redshift,
#         stage_fact_s3_to_redshift
#     ]

#     operator_queries = SqlQueries