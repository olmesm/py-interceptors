from examples import csv_import_pipeline as example

from py_interceptors import Runtime

CSV = """\
email,amount
ada@example.com,10
not-an-email,12
grace@example.com,15
linus@example.com,abc
"""


def test_csv_import_example_summarizes_accepted_and_rejected_rows() -> None:
    with Runtime() as runtime:
        result = runtime.run_sync(example.workflow, CSV)

    assert [(row.email, row.amount) for row in result.accepted] == [
        ("ada@example.com", 10),
        ("grace@example.com", 15),
    ]
    assert [(row.line_number, row.reason) for row in result.rejected] == [
        (3, "invalid email"),
        (5, "invalid amount"),
    ]
    assert result.total_amount == 25
