from collections.abc import Sequence
from enum import Enum
from typing import TYPE_CHECKING

from datachain.func import case, ifelse, isnone, or_
from datachain.lib.signal_schema import SignalSchema
from datachain.query.schema import Column

if TYPE_CHECKING:
    from datachain.lib.dc import DataChain

C = Column


STATUS_COL_NAME = "diff_7aeed3aa17ba4d50b8d1c368c76e16a6"
LEFT_DIFF_COL_NAME = "diff_95f95344064a4b819c8625cd1a5cfc2b"
RIGHT_DIFF_COL_NAME = "diff_5808838a49b54849aa461d7387376d34"


class CompareStatus(str, Enum):
    ADDED = "A"
    DELETED = "D"
    MODIFIED = "M"
    SAME = "S"


def _to_list(obj: str | Sequence[str] | None) -> list[str] | None:
    if obj is None:
        return None
    return [obj] if isinstance(obj, str) else list(obj)


def _validate_inputs(
    on, right_on, compare, right_compare, added, deleted, modified, same
):
    if not any([added, deleted, modified, same]):
        raise ValueError(
            "At least one of added, deleted, modified, same flags must be set"
        )
    if on is None:
        raise ValueError("'on' must be specified")
    if right_on and len(on) != len(right_on):
        raise ValueError("'on' and 'right_on' must be have the same length")
    if right_compare and not compare:
        raise ValueError("'compare' must be defined if 'right_compare' is defined")
    if compare and right_compare and len(compare) != len(right_compare):
        raise ValueError("'compare' and 'right_compare' must have the same length")


def _resolve_compare_cols(compare, right_compare, left, right, cols, right_cols, on):
    if compare:
        compare_ = compare
        compare = left.signals_schema.resolve(*compare).db_signals()
        right_compare = right.signals_schema.resolve(
            *(right_compare or compare_)
        ).db_signals()
    elif len(cols) != len(right_cols):
        compare = None
        right_compare = None
    else:
        compare = right_compare = [c for c in cols if c in right_cols and c not in on]
    return compare, right_compare


def _build_modified_cond(compare, right_compare, rname):
    if compare is None:
        return True
    if len(compare) == 0:
        return False
    return or_(
        *[
            C(c) != (C(f"{rname}{rc}") if c == rc else C(rc))
            for c, rc in zip(compare, right_compare, strict=False)
        ]
    )


def _normalize_lists(
    on, right_on, compare, right_compare, added, deleted, modified, same,
):
    on = _to_list(on)
    right_on = _to_list(right_on)
    compare = _to_list(compare)
    right_compare = _to_list(right_compare)
    _validate_inputs(
        on, right_on, compare, right_compare, added, deleted, modified, same
    )
    return on, right_on, compare, right_compare


def _resolve_schemas(on, right_on, compare, right_compare, left, right):
    cols = left.signals_schema.clone_without_sys_signals().db_signals()
    right_cols = right.signals_schema.clone_without_sys_signals().db_signals()
    cols_select = list(left.signals_schema.clone_without_sys_signals().values.keys())
    right_on = right.signals_schema.resolve(*(right_on or on)).db_signals()
    on = left.signals_schema.resolve(*on).db_signals()
    compare, right_compare = _resolve_compare_cols(
        compare, right_compare, left, right, cols, right_cols, on
    )
    return on, right_on, compare, right_compare, cols_select


def _build_diff_dataset(left, right, on, right_on, compare, right_compare, status_col):
    diff_col = status_col or STATUS_COL_NAME
    left = left.mutate(**{LEFT_DIFF_COL_NAME: 1})
    right = right.mutate(**{RIGHT_DIFF_COL_NAME: 1})

    dc_diff = (
        left.merge(right, on=on, right_on=right_on, rname="right_", full=True)
        .mutate(
            **{
                diff_col: case(
                    (isnone(LEFT_DIFF_COL_NAME), CompareStatus.DELETED),
                    (isnone(RIGHT_DIFF_COL_NAME), CompareStatus.ADDED),
                    (
                        _build_modified_cond(compare, right_compare, "right_"),
                        CompareStatus.MODIFIED,
                    ),
                    else_=CompareStatus.SAME,
                )
            }
        )
        .mutate(
            **{
                f"{l_on}": ifelse(
                    C(diff_col) == CompareStatus.DELETED,
                    C(f"{'right_' + l_on if on == right_on else r_on}"),
                    C(l_on),
                )
                for l_on, r_on in zip(on, right_on, strict=False)
            }
        )
        .select_except(LEFT_DIFF_COL_NAME, RIGHT_DIFF_COL_NAME)
    )
    return dc_diff, diff_col


def _apply_status_filters(dc_diff, diff_col, added, deleted, modified, same):
    for status, flag in (
        (CompareStatus.ADDED, added),
        (CompareStatus.MODIFIED, modified),
        (CompareStatus.SAME, same),
        (CompareStatus.DELETED, deleted),
    ):
        if not flag:
            dc_diff = dc_diff.filter(C(diff_col) != status)
    return dc_diff


def _finalize_schema(dc_diff, cols_select, diff_col, status_col, schema):
    if status_col:
        cols_select.append(diff_col)

    dc_diff = dc_diff.select(*cols_select)

    dc_diff.signals_schema = (
        schema if not status_col else SignalSchema({status_col: str}) | schema
    )

    return dc_diff


def _compare(
    left: "DataChain",
    right: "DataChain",
    on: str | Sequence[str],
    right_on: str | Sequence[str] | None = None,
    compare: str | Sequence[str] | None = None,
    right_compare: str | Sequence[str] | None = None,
    added: bool = True,
    deleted: bool = True,
    modified: bool = True,
    same: bool = True,
    status_col: str | None = None,
) -> "DataChain":
    schema = left.signals_schema

    on, right_on, compare, right_compare = _normalize_lists(
        on, right_on, compare, right_compare, added, deleted, modified, same,
    )
    on, right_on, compare, right_compare, cols_select = _resolve_schemas(
        on, right_on, compare, right_compare, left, right,
    )

    dc_diff, diff_col = _build_diff_dataset(
        left, right, on, right_on, compare, right_compare, status_col
    )

    dc_diff = _apply_status_filters(dc_diff, diff_col, added, deleted, modified, same)

    return _finalize_schema(dc_diff, cols_select, diff_col, status_col, schema)


def _to_status_dict(res, status_col, added, deleted, modified, same):
    chains = {}
    if added:
        chains[CompareStatus.ADDED.value] = (
            res.filter(C(status_col) == CompareStatus.ADDED).select_except(status_col)
        )
    if deleted:
        chains[CompareStatus.DELETED.value] = (
            res.filter(C(status_col) == CompareStatus.DELETED).select_except(status_col)
        )
    if modified:
        chains[CompareStatus.MODIFIED.value] = (
            res.filter(C(status_col) == CompareStatus.MODIFIED)
            .select_except(status_col)
        )
    if same:
        chains[CompareStatus.SAME.value] = (
            res.filter(C(status_col) == CompareStatus.SAME).select_except(status_col)
        )
    return chains


def compare_and_split(
    left: "DataChain",
    right: "DataChain",
    on: str | Sequence[str],
    right_on: str | Sequence[str] | None = None,
    compare: str | Sequence[str] | None = None,
    right_compare: str | Sequence[str] | None = None,
    added: bool = True,
    deleted: bool = True,
    modified: bool = True,
    same: bool = False,
) -> dict[str, "DataChain"]:
    """Comparing two chains and returning multiple chains, one for each of `added`,
    `deleted`, `modified` and `same` status. Result is returned in form of
    dictionary where each item represents one of the statuses and key values
    are `A`, `D`, `M`, `S` corresponding. Note that status column is not in the
    resulting chains.

    Parameters:
        left: Chain to calculate diff on.
        right: Chain to calculate diff from.
        on: Column or list of columns to match on. If both chains have the
            same columns then this column is enough for the match. Otherwise,
            `right_on` parameter has to specify the columns for the other chain.
            This value is used to find corresponding row in other dataset. If not
            found there, row is considered as added (or removed if vice versa), and
            if found then row can be either modified or same.
        right_on: Optional column or list of columns
            for the `other` to match.
        compare: Column or list of columns to compare on. If both chains have
            the same columns then this column is enough for the compare. Otherwise,
            `right_compare` parameter has to specify the columns for the other
            chain. This value is used to see if row is modified or same. If
            not set, all columns will be used for comparison
        right_compare: Optional column or list of columns
                for the `other` to compare to.
        added (bool): Whether to return chain containing only added rows.
        deleted (bool): Whether to return chain containing only deleted rows.
        modified (bool): Whether to return chain containing only modified rows.
        same (bool): Whether to return chain containing only same rows.

    Example:
        ```py
        chains = compare(
            persons,
            new_persons,
            on=["id"],
            right_on=["other_id"],
            compare=["name"],
            added=True,
            deleted=True,
            modified=True,
            same=True,
        )
        ```
    """
    status_col = STATUS_COL_NAME

    res = _compare(
        left,
        right,
        on,
        right_on=right_on,
        compare=compare,
        right_compare=right_compare,
        added=added,
        deleted=deleted,
        modified=modified,
        same=same,
        status_col=status_col,
    )

    return _to_status_dict(res, status_col, added, deleted, modified, same)
