import csv
from collections.abc import Iterable
from dataclasses import dataclass
from io import StringIO

from py_interceptors import Interceptor, Runtime, StreamInterceptor, chain, stream_chain


@dataclass
class RawRow:
    line_number: int
    data: dict[str, str]


@dataclass
class AcceptedRow:
    email: str
    amount: int


@dataclass
class RejectedRow:
    line_number: int
    reason: str
    data: dict[str, str]


@dataclass
class ImportSummary:
    accepted: list[AcceptedRow]
    rejected: list[RejectedRow]
    total_amount: int


class ParseCsv(Interceptor[str, list[RawRow]]):
    input_type = str
    output_type = list[RawRow]

    def enter(self, ctx: str) -> list[RawRow]:
        reader = csv.DictReader(StringIO(ctx))
        return [
            RawRow(index, {key: value or "" for key, value in row.items()})
            for index, row in enumerate(reader, start=2)
        ]


class SplitRows(
    StreamInterceptor[list[RawRow], RawRow, AcceptedRow | RejectedRow, ImportSummary]
):
    input_type = list[RawRow]
    emit_type = RawRow
    collect_type = AcceptedRow | RejectedRow
    output_type = ImportSummary

    def stream(self, ctx: list[RawRow]) -> Iterable[RawRow]:
        return ctx

    def collect(
        self, ctx: list[RawRow], items: Iterable[AcceptedRow | RejectedRow]
    ) -> ImportSummary:
        results = list(items)
        accepted = [item for item in results if isinstance(item, AcceptedRow)]
        rejected = [item for item in results if isinstance(item, RejectedRow)]
        return ImportSummary(accepted, rejected, sum(row.amount for row in accepted))


class ValidateRow(Interceptor[RawRow, AcceptedRow | RejectedRow]):
    input_type = RawRow
    output_type = AcceptedRow | RejectedRow

    def enter(self, ctx: RawRow) -> AcceptedRow | RejectedRow:
        email = ctx.data.get("email", "").strip()
        if "@" not in email:
            return RejectedRow(ctx.line_number, "invalid email", ctx.data)
        try:
            amount = int(ctx.data.get("amount", ""))
        except ValueError:
            return RejectedRow(ctx.line_number, "invalid amount", ctx.data)
        if amount <= 0:
            return RejectedRow(ctx.line_number, "amount must be positive", ctx.data)
        return AcceptedRow(email, amount)


validate_rows = chain("validate row").use(ValidateRow).build()

import_stage = stream_chain("import rows").stream(SplitRows).map(validate_rows).build()

workflow = chain("csv import").use(ParseCsv).use(import_stage).build()

EXAMPLE_CSV = """\
email,amount
ada@example.com,10
broken,20
grace@example.com,15
linus@example.com,-1
"""


def run_example() -> ImportSummary:
    with Runtime() as runtime:
        return runtime.run_sync(workflow, EXAMPLE_CSV)


if __name__ == "__main__":
    print(run_example())
