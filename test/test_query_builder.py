from dataquery_processor import QueryBuilder


def test_simple_query():
    manifest = {
        "datasource": "Students by FPE",
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
    assert q == 'SELECT [\'Ethnicity\',\'Fruit\'] FROM "dbo.idd_v_students_by_fpe" GROUP BY "Ethnicity","Fruit"'


def test_simple_query_with_constraint():
    manifest = {
        "datasource": "Students by FPE",
        "items":
            [
                {"fieldName": "Ethnicity"},
                {"fieldName": "Fruit", "allowedValues": ["banana"]}
            ],
        "years": [
                "2020/21"
            ]
    }
    qb = QueryBuilder(manifest)
    q = qb.create_query()
    assert q == 'SELECT [\'Ethnicity\'] FROM "dbo.idd_v_students_by_fpe" WHERE "Fruit"=\'banana\' GROUP BY "Ethnicity"'
