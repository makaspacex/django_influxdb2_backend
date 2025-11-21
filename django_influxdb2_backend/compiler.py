from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Tuple

from django.db.models.expressions import Col
from django.db.models.sql.compiler import SQLCompiler
from django.db.models.sql.where import AND, OR, WhereNode


class FluxCompiler(SQLCompiler):
    def as_sql(self, with_limits=True, with_col_aliases=False) -> Tuple[str, tuple]:
        bucket = self.connection.settings_dict.get("NAME") or "default"
        measurement = self.query.model._meta.db_table
        start, stop, filters = self._build_filters(self.query.where)

        flux_parts: List[str] = [f'from(bucket: "{bucket}")']

        if start or stop:
            range_args = []
            if start:
                range_args.append(f"start: {start}")
            if stop:
                range_args.append(f"stop: {stop}")
            flux_parts.append(f"|> range({', '.join(range_args)})")
        else:
            flux_parts.append("|> range(start: -30d)")

        flux_parts.append(f'|> filter(fn: (r) => r["_measurement"] == "{measurement}")')
        for f in filters:
            flux_parts.append(f"|> filter(fn: (r) => {f})")

        selected = self._selected_columns()
        if selected:
            cols = ', '.join([f'"{col}"' for col in selected])
            flux_parts.append(f"|> keep(columns: [{cols}])")

        order_by = self._get_ordering()
        if order_by:
            cols, desc = zip(*order_by)
            desc_value = "true" if any(desc) else "false"
            col_list = ', '.join([f'"{c}"' for c in cols])
            flux_parts.append(f"|> sort(columns: [{col_list}], desc: {desc_value})")

        low_mark, high_mark = self.query.low_mark, self.query.high_mark
        if high_mark is not None:
            n_value = high_mark - (low_mark or 0)
            offset_value = f", offset: {low_mark}" if low_mark else ""
            flux_parts.append(f"|> limit(n: {n_value}{offset_value})")

        return "\n".join(flux_parts), ()

    # Helpers
    def _build_filters(self, where: WhereNode) -> Tuple[Optional[str], Optional[str], List[str]]:
        if not where or not where.children:
            return None, None, []
        start = None
        stop = None
        clauses: List[str] = []

        def _walk(node: WhereNode):
            nonlocal start, stop
            if isinstance(node, WhereNode):
                children = [ _walk(child) for child in node.children if child]
                children = [c for c in children if c]
                if not children:
                    return None
                connector = " and " if node.connector == AND else " or "
                return f"({connector.join(children)})"
            if hasattr(node, "rhs") and hasattr(node, "lhs"):
                field_name, lookup = self._field_and_lookup(node)
                value = node.rhs
                if field_name in {"_time", "time", "timestamp"} and lookup in {"gte", "gt", "lte", "lt"}:
                    rendered = self.connection.ops.adapt_datetimefield_value(value)
                    if lookup in {"gte", "gt"}:
                        start = rendered if start is None else start
                    if lookup in {"lte", "lt"}:
                        stop = rendered if stop is None else stop
                    return None
                rendered = self.connection.ops.format_lookup(field_name, lookup, value)
                return rendered
            return None

        for child in where.children:
            clause = _walk(child)
            if clause:
                clauses.append(clause)
        return start, stop, clauses

    def _field_and_lookup(self, node):
        lhs = node.lhs
        lookup = node.lookup_name
        if isinstance(lhs, Col):
            return lhs.target.column, lookup
        return str(lhs), lookup

    def _selected_columns(self) -> List[str]:
        if self.query.values_select:
            return list(self.query.values_select)
        if self.query.select:
            return [c.target.column for c in self.query.select if isinstance(c, Col)]
        return []

    def _get_ordering(self):
        ordering = []
        if not self.query.order_by:
            return ordering
        for expr in self.query.order_by:
            if isinstance(expr, str):
                descending = expr.startswith("-")
                ordering.append((expr.lstrip("-"), descending))
                continue
            descending = getattr(expr, "descending", False)
            col = getattr(expr, "expression", expr)
            if isinstance(col, Col):
                ordering.append((col.target.column, descending))
        return ordering
