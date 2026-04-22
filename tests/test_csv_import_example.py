from _example_loader import load_example_module

from py_interceptors import Runtime


def test_csv_import_example_summarizes_accepted_and_rejected_rows() -> None:
    example = load_example_module("csv_import_pipeline")
    runtime = Runtime()
    ctx = example.CsvText(
        text=(
            "email,amount\n"
            "ada@example.com,10\n"
            "not-an-email,12\n"
            "grace@example.com,15\n"
            "linus@example.com,abc\n"
        )
    )

    try:
        result = runtime.run_sync(example.workflow, ctx)
    finally:
        runtime.shutdown()

    assert [(row.email, row.amount) for row in result.accepted] == [
        ("ada@example.com", 10),
        ("grace@example.com", 15),
    ]
    assert [(row.line_number, row.reason) for row in result.rejected] == [
        (3, "invalid email"),
        (5, "invalid amount"),
    ]
    assert result.total_amount == 25
