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
    assert q == 'SELECT "Ethnicity","Fruit" FROM "dbo"."v_IIDD_dd5" WHERE "Academic year" IN (\'2020/21\') GROUP BY "Ethnicity","Fruit"'


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
    assert q == 'SELECT "Ethnicity" FROM "dbo"."v_IIDD_dd5" WHERE "Fruit"=\'banana\' AND "Academic year" IN (\'2020/21\') GROUP BY "Ethnicity"'


def test_simple_query_with_constraints_2():
    manifest = {
        "datasource": "Students by FPE",
        "items":
            [
                {"fieldName": "AcademicYearStart"},
                {"fieldName": "Domicile (UK county/ Non-UK by country/ Unknown)"},
                {"fieldName": "First year marker", "allowedValues": ["First year"]},
                {"fieldName": "First year marker"}
            ],
        "years": [
                "2019/20",
                "2020/21"
            ]
    }
    qb = QueryBuilder(manifest)
    q = qb.create_query()
    print(q)
    assert q == 'SELECT "AcademicYearStart","Domicile (UK county/ Non-UK by country/ Unknown)","First year marker" FROM "dbo"."v_IIDD_dd5" WHERE "First year marker"=\'First year\' AND "Academic year" IN (\'2019/20\',\'2020/21\') GROUP BY "AcademicYearStart","Domicile (UK county/ Non-UK by country/ Unknown)","First year marker"'

