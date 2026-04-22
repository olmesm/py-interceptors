from __future__ import annotations

import csv
from collections.abc import Iterable
from dataclasses import dataclass
from io import StringIO

from py_interceptors import (
    Interceptor,
    Runtime,
    StreamInterceptor,
    chain,
    stream_chain,
)


@dataclass
class CsvText:
    text: str


@dataclass
class RawRow:
    line_number: int
    data: dict[str, str]


@dataclass
class RawRows:
    rows: list[RawRow]


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
class RowResult:
    accepted: AcceptedRow | None = None
    rejected: RejectedRow | None = None


@dataclass
class ImportSummary:
    accepted: list[AcceptedRow]
    rejected: list[RejectedRow]
    total_amount: int


class ParseCsv(Interceptor[CsvText, RawRows]):
    input_type = CsvText
    output_type = RawRows

    def enter(self, ctx: CsvText) -> RawRows:
        reader = csv.DictReader(StringIO(ctx.text))
        rows = [
            RawRow(
                line_number=index,
                data={key: value or "" for key, value in row.items()},
            )
            for index, row in enumerate(reader, start=2)
        ]
        return RawRows(rows=rows)


class SplitRows(StreamInterceptor[RawRows, RawRow, RowResult, ImportSummary]):
    input_type = RawRows
    emit_type = RawRow
    collect_type = RowResult
    output_type = ImportSummary

    def stream(self, ctx: RawRows) -> Iterable[RawRow]:
        return iter(ctx.rows)

    def collect(
        self,
        ctx: RawRows,
        items: Iterable[RowResult],
    ) -> ImportSummary:
        results = list(items)
        accepted = [item.accepted for item in results if item.accepted is not None]
        rejected = [item.rejected for item in results if item.rejected is not None]
        return ImportSummary(
            accepted=accepted,
            rejected=rejected,
            total_amount=sum(row.amount for row in accepted),
        )


class ValidateRow(Interceptor[RawRow, RowResult]):
    input_type = RawRow
    output_type = RowResult

    def enter(self, ctx: RawRow) -> RowResult:
        email = ctx.data.get("email", "").strip()
        amount_text = ctx.data.get("amount", "").strip()

        if "@" not in email:
            return RowResult(
                rejected=RejectedRow(ctx.line_number, "invalid email", ctx.data)
            )

        try:
            amount = int(amount_text)
        except ValueError:
            return RowResult(
                rejected=RejectedRow(ctx.line_number, "invalid amount", ctx.data)
            )

        if amount <= 0:
            return RowResult(
                rejected=RejectedRow(ctx.line_number, "amount must be positive", ctx.data)
            )

        return RowResult(accepted=AcceptedRow(email=email, amount=amount))


validate_rows = chain("validate row").use(ValidateRow).build()

import_stage = (
    stream_chain("import rows")
    .stream(SplitRows)
    .map(validate_rows)
    .build()
)

workflow = chain("csv import").use(ParseCsv).use(import_stage).build()


def example_context() -> CsvText:
    return CsvText(
        text=(
            "email,amount\n"
            "ada@example.com,10\n"
            "broken,20\n"
            "grace@example.com,15\n"
            "linus@example.com,-1\n"
        )
    )


def run_example() -> ImportSummary:
    runtime = Runtime()
    try:
        return runtime.run_sync(workflow, example_context())
    finally:
        runtime.shutdown()


def main() -> None:
    result = run_example()
    print(result)


if __name__ == "__main__":
    main()
