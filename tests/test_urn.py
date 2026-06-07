from cardcompass import urn


def test_dataset_urn_carries_pid_as_platform_instance():
    assert urn.dataset_urn("CARD_STG", "card_auth") == (
        "urn:li:dataset:(urn:li:dataPlatform:mssql,TEST.DCF_DB.CARD_STG.card_auth,PROD)"
    )


def test_column_urn_nests_dataset_urn():
    u = urn.column_urn("CARD_INT", "card_txn", "auth_amount")
    assert "TEST.DCF_DB.CARD_INT.card_txn" in u
    assert u.endswith(",auth_amount)")


def test_file_urn_uses_file_platform_and_pid():
    u = urn.file_urn("card_auth_inbound")
    assert "dataPlatform:file" in u
    assert "TEST.card_auth_inbound" in u
