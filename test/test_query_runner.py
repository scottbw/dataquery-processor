from dataquery_processor import QueryBuilder, OdbcQueryRunner


def test_create_and_run_simple_query():
    manifest = {
        "datasource": "Students by FPE",
        "measure": "FPE",
        "items":
            [
                {"fieldName": "Ethnicity"},
                {"fieldName": "Fruit"}
            ],
        "years": [
                "2020/21"
            ]
    }
    qb = QueryBuilder(manifest)
    q = qb.create_query()
    odbc = OdbcQueryRunner()
    odbc.run_query_and_save_results(q, "output/test_query_runner.csv")


def test_create_and_run_simple_query_2():
    manifest = {
        "datasource": "Students by FPE",
        "measure": "FPE",
        "items":
            [
                {"fieldName": "Fruit"}
            ],
        "years": [
                "2019/20", "2020/21"
            ]
    }
    qb = QueryBuilder(manifest)
    q = qb.create_query()
    odbc = OdbcQueryRunner()
    odbc.run_query_and_save_results(q, "output/test_query_runner_2.csv")