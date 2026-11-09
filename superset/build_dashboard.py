"""Build or update the PayFlow Overview dashboard in Superset from the gold layer.

Runs inside the superset container and talks to its REST API. Safe to rerun: it reuses the
database connection and datasets. If the dashboard exists, its charts and filters are updated
in place, keeping the layout and any filter defaults changed in the UI; otherwise it is created.

    Get-Content superset/build_dashboard.py | docker exec -i -e CLICKHOUSE_USER -e CLICKHOUSE_PASSWORD superset python -

CLICKHOUSE_USER and CLICKHOUSE_PASSWORD must be set in the calling shell (values from .env).
"""
import json
import os
import uuid

import requests

BASE = "http://localhost:8088/api/v1"
DATABASE_NAME = "PayFlow ClickHouse"
DASHBOARD_TITLE = "PayFlow Overview"
DASHBOARD_SLUG = "payflow-overview"

CUSTOMER_PROFILE_SQL = """WITH Accounts AS (
  SELECT
    CustomerId,
    COUNT(*) AS AccountCount,
    SUM(BalanceUsd) AS BalanceUsd
  FROM
    gold.dim_accounts
  GROUP BY
    CustomerId
),
FailedPins AS (
  SELECT
    CustomerId,
    COUNT(*) AS FailedPinAttempts
  FROM
    gold.fact_app_events
  WHERE
    EventType = 'failed_pin_attempt'
  GROUP BY
    CustomerId
)
SELECT
  dc.CustomerId AS CustomerId,
  dc.CountryCode AS CountryCode,
  dc.Region AS Region,
  dc.ComplianceRiskLevel AS ComplianceRiskLevel,
  dc.KycStatus AS KycStatus,
  a.AccountCount AS AccountCount,
  a.BalanceUsd AS BalanceUsd,
  fp.FailedPinAttempts AS FailedPinAttempts
FROM
  gold.dim_customers dc
  LEFT JOIN Accounts a ON a.CustomerId = dc.CustomerId
  LEFT JOIN FailedPins fp ON fp.CustomerId = dc.CustomerId"""

FX_LATEST_SQL = """WITH LatestRates AS (
  SELECT
    CurrencyCode,
    MAX(RateDate) AS LatestRateDate,
    argMax(UnitsPerUsd, RateDate) AS UnitsPerUsd
  FROM
    silver.fx_rates
  WHERE
    CurrencyCode != 'USD'
  GROUP BY
    CurrencyCode
)
SELECT
  CurrencyCode,
  LatestRateDate AS RateDate,
  toFloat64(UnitsPerUsd) AS UnitsPerUsd,
  ROUND(1 / toFloat64(UnitsPerUsd), 6) AS UsdPerUnit
FROM
  LatestRates"""

# Transaction charts show amounts in the transaction's own currency. The Transaction currency
# filter is single-select and required, so amounts in different currencies are never summed.
TX_CHARTS = ("kpi_volume", "kpi_net", "kpi_tx", "daily_volume", "daily_net", "running_net")
FX_CHARTS = ("fx_latest", "fx_history")
NO_TIME_CHARTS = ("kpi_customers", "risk_pie", "risk_regions", "fx_latest")
LOCAL_NET_FLOW = "SUM(IF(IsInCashFlow, IF(Direction = 'in', Amount, -Amount), 0))"


def session():
    s = requests.Session()
    r = s.post(f"{BASE}/security/login", json={
        "username": os.environ["SUPERSET_ADMIN_USER"],
        "password": os.environ["SUPERSET_ADMIN_PASSWORD"],
        "provider": "db",
        "refresh": True,
    })
    r.raise_for_status()
    s.headers["Authorization"] = "Bearer " + r.json()["access_token"]
    s.headers["X-CSRFToken"] = s.get(f"{BASE}/security/csrf_token/").json()["result"]
    s.headers["Referer"] = "http://localhost:8088"
    return s


def call(s, method, path, **kwargs):
    r = s.request(method, f"{BASE}/{path}", **kwargs)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
    return r.json() if r.text else {}


def find(s, resource, column, value):
    q = f"(filters:!((col:{column},opr:eq,value:'{value}')),page_size:100)"
    return call(s, "GET", f"{resource}/", params={"q": q})["result"]


def ensure_database(s):
    uri = (
        f"clickhousedb://{os.environ['CLICKHOUSE_USER']}:{os.environ['CLICKHOUSE_PASSWORD']}"
        "@clickhouse:8123/gold"
    )
    call(s, "POST", "database/test_connection/", json={"database_name": DATABASE_NAME, "sqlalchemy_uri": uri})
    existing = find(s, "database", "database_name", DATABASE_NAME)
    if existing:
        return existing[0]["id"]
    return call(s, "POST", "database/", json={
        "database_name": DATABASE_NAME,
        "sqlalchemy_uri": uri,
        "expose_in_sqllab": True,
    })["id"]


def ensure_dataset(s, database_id, schema, name, sql=None):
    existing = [d for d in find(s, "dataset", "table_name", name) if d["schema"] == schema]
    if existing:
        dataset_id = existing[0]["id"]
        if sql:
            call(s, "PUT", f"dataset/{dataset_id}", json={"sql": sql})
    else:
        body = {"database": database_id, "schema": schema, "table_name": name}
        if sql:
            body["sql"] = sql
        dataset_id = call(s, "POST", "dataset/", json=body)["id"]
    call(s, "PUT", f"dataset/{dataset_id}/refresh")
    return dataset_id


def metric(sql, label):
    return {
        "expressionType": "SQL",
        "sqlExpression": sql,
        "label": label,
        "hasCustomLabel": True,
        "optionName": "metric_" + uuid.uuid4().hex[:12],
    }


def where(sql):
    return {"expressionType": "SQL", "sqlExpression": sql, "clause": "WHERE"}


def time_filter(column):
    return {
        "expressionType": "SIMPLE",
        "subject": column,
        "operator": "TEMPORAL_RANGE",
        "comparator": "No filter",
        "clause": "WHERE",
    }


def chart_specs(ds):
    completed = "Status = 'Completed'"
    volume = metric("SUM(Amount)", "Volume")
    net = metric(LOCAL_NET_FLOW, "Net cash flow")
    return [
        {
            "key": "kpi_volume", "name": "Completed volume", "ds": ds["tx"], "viz": "big_number_total",
            "description": "Completed transactions of every type, including Transfer, in the selected transaction currency.",
            "form": {"metric": volume, "adhoc_filters": [time_filter("TransactionDate"), where(completed)],
                     "y_axis_format": ",.0f", "subheader": "completed, in selected currency"},
            "metrics": [volume], "columns": [], "wheres": [completed],
        },
        {
            "key": "kpi_net", "name": "Net cash flow", "ds": ds["tx"], "viz": "big_number_total",
            "description": "Completed deposits minus withdrawals and card payments, in the selected transaction currency. Transfer is excluded: the source gives no direction.",
            "form": {"metric": net, "adhoc_filters": [time_filter("TransactionDate")],
                     "y_axis_format": ",.0f", "subheader": "in minus out, excluding Transfer"},
            "metrics": [net], "columns": [], "wheres": [],
        },
        {
            "key": "kpi_tx", "name": "Completed transactions", "ds": ds["tx"], "viz": "big_number_total",
            "description": "Number of completed transactions in the selected transaction currency.",
            "form": {"metric": metric("COUNT(*)", "Transactions"),
                     "adhoc_filters": [time_filter("TransactionDate"), where(completed)],
                     "y_axis_format": ",d", "subheader": "count"},
            "metrics": [metric("COUNT(*)", "Transactions")], "columns": [], "wheres": [completed],
        },
        {
            "key": "kpi_customers", "name": "Customers", "ds": ds["profile"], "viz": "big_number_total",
            "description": "Current customers.",
            "form": {"metric": metric("COUNT(*)", "Customers"), "adhoc_filters": [],
                     "y_axis_format": ",d", "subheader": "current"},
            "metrics": [metric("COUNT(*)", "Customers")], "columns": [], "wheres": [],
        },
        {
            "key": "daily_volume", "name": "Daily volume by type", "ds": ds["tx"], "viz": "echarts_timeseries_bar",
            "description": "Completed transactions per UTC day, stacked by type, in the selected transaction currency.",
            "form": {"x_axis": "TransactionDate", "time_grain_sqla": "P1D", "metrics": [volume],
                     "groupby": ["TransactionType"], "stack": "Stack", "show_legend": True,
                     "adhoc_filters": [time_filter("TransactionDate"), where(completed)],
                     "y_axis_format": ",.0f", "row_limit": 10000},
            "metrics": [volume], "columns": ["TransactionDate", "TransactionType"], "wheres": [completed],
        },
        {
            "key": "daily_net", "name": "Daily net cash flow", "ds": ds["tx"], "viz": "echarts_timeseries_bar",
            "description": "Net cash flow per UTC day in the selected transaction currency. Transfer and non-completed transactions are excluded.",
            "form": {"x_axis": "TransactionDate", "time_grain_sqla": "P1D", "metrics": [net], "groupby": [],
                     "adhoc_filters": [time_filter("TransactionDate")], "y_axis_format": ",.0f",
                     "show_legend": False, "row_limit": 10000},
            "metrics": [net], "columns": ["TransactionDate"], "wheres": [],
        },
        {
            "key": "running_net", "name": "Cumulative net cash flow", "ds": ds["tx"], "viz": "echarts_timeseries_line",
            "description": "Running total of net cash flow within the selected date range, in the selected transaction currency.",
            "form": {"x_axis": "TransactionDate", "time_grain_sqla": "P1D", "metrics": [net], "groupby": [],
                     "rolling_type": "cumsum", "adhoc_filters": [time_filter("TransactionDate")],
                     "y_axis_format": ",.0f", "show_legend": False, "row_limit": 10000},
            "metrics": [net], "columns": ["TransactionDate"], "wheres": [],
        },
        {
            "key": "risk_pie", "name": "Customers by risk level", "ds": ds["profile"], "viz": "pie",
            "description": "Compliance risk level of each customer's country.",
            "form": {"groupby": ["ComplianceRiskLevel"], "metric": metric("COUNT(*)", "Customers"),
                     "adhoc_filters": [], "show_labels": True, "label_type": "key_value_percent",
                     "show_legend": True, "row_limit": 100},
            "metrics": [metric("COUNT(*)", "Customers")], "columns": ["ComplianceRiskLevel"], "wheres": [],
        },
        {
            "key": "risk_regions", "name": "Risk profile by region", "ds": ds["profile"], "viz": "table",
            "description": "Customers, balances in USD and failed PIN attempts by region and risk level.",
            "form": {"query_mode": "aggregate", "groupby": ["Region", "ComplianceRiskLevel"],
                     "metrics": [metric("COUNT(*)", "Customers"), metric("SUM(BalanceUsd)", "Balance (USD)"),
                                 metric("SUM(FailedPinAttempts)", "Failed PIN attempts")],
                     "all_columns": [], "adhoc_filters": [], "row_limit": 1000, "order_desc": True,
                     "column_config": {"Balance (USD)": {"d3NumberFormat": "$,.0f"}}},
            "metrics": [metric("COUNT(*)", "Customers"), metric("SUM(BalanceUsd)", "Balance (USD)"),
                        metric("SUM(FailedPinAttempts)", "Failed PIN attempts")],
            "columns": ["Region", "ComplianceRiskLevel"], "wheres": [],
        },
        {
            "key": "fx_latest", "name": "FX converter: latest rates", "ds": ds["fx_latest"], "viz": "table",
            "description": "Latest rate per currency, both ways. Use the FX currency filter to pick currencies.",
            "form": {"query_mode": "raw", "all_columns": ["CurrencyCode", "RateDate", "UnitsPerUsd", "UsdPerUnit"],
                     "groupby": [], "metrics": [], "adhoc_filters": [], "row_limit": 100,
                     "column_config": {"UnitsPerUsd": {"d3NumberFormat": ",.6f"}, "UsdPerUnit": {"d3NumberFormat": ",.6f"}}},
            "metrics": [], "columns": ["CurrencyCode", "RateDate", "UnitsPerUsd", "UsdPerUnit"], "wheres": [],
            "raw": True,
        },
        {
            "key": "fx_history", "name": "FX rates over time (units per USD)", "ds": ds["fx_rates"], "viz": "echarts_timeseries_line",
            "description": "Daily rate per currency.",
            "form": {"x_axis": "RateDate", "time_grain_sqla": "P1D", "metrics": [metric("AVG(UnitsPerUsd)", "Units per USD")],
                     "groupby": ["CurrencyCode"], "adhoc_filters": [time_filter("RateDate"), where("CurrencyCode != 'USD'")],
                     "y_axis_format": ",.4f", "show_legend": True, "row_limit": 10000},
            "metrics": [metric("AVG(UnitsPerUsd)", "Units per USD")], "columns": ["RateDate", "CurrencyCode"],
            "wheres": ["CurrencyCode != 'USD'"],
        },
    ]


def query_context(spec, form_data, filters=None):
    query = {
        "columns": spec["columns"],
        "metrics": [] if spec.get("raw") else spec["metrics"],
        "filters": filters or [],
        "extras": {"where": " AND ".join(f"({w})" for w in spec["wheres"])},
        "orderby": [],
        "row_limit": 10000,
        "time_range": "No filter",
    }
    return {
        "datasource": {"id": spec["ds"], "type": "table"},
        "force": False,
        "queries": [query],
        "form_data": form_data,
        "result_format": "json",
        "result_type": "full",
    }


def native_filters(ds, ids):
    all_ids = list(ids.values())

    def scope(keys):
        in_scope = [ids[k] for k in keys]
        return {"rootPath": ["ROOT_ID"], "excluded": [i for i in all_ids if i not in in_scope]}, in_scope

    time_keys = [k for k in ids if k not in NO_TIME_CHARTS]
    date_scope, date_in = scope(time_keys)
    tx_scope, tx_in = scope(TX_CHARTS)
    fx_scope, fx_in = scope(FX_CHARTS)
    select_controls = {"enableEmptyFilter": False, "defaultToFirstItem": False,
                       "inverseSelection": False, "searchAllOptions": False}
    return [
        {
            "id": "NATIVE_FILTER-date-range", "name": "Date range", "filterType": "filter_time",
            "type": "NATIVE_FILTER", "targets": [{}], "controlValues": {"enableEmptyFilter": False},
            "cascadeParentIds": [], "scope": date_scope, "chartsInScope": date_in, "description": "",
            "defaultDataMask": {"filterState": {"value": "Last month"}, "extraFormData": {"time_range": "Last month"}},
        },
        {
            "id": "NATIVE_FILTER-transaction-currency", "name": "Transaction currency", "filterType": "filter_select",
            "type": "NATIVE_FILTER", "targets": [{"datasetId": ds["tx"], "column": {"name": "CurrencyCode"}}],
            "controlValues": {**select_controls, "multiSelect": False, "enableEmptyFilter": True},
            "cascadeParentIds": [], "scope": tx_scope, "chartsInScope": tx_in,
            "description": "One currency at a time, so amounts in different currencies are never added together.",
            "defaultDataMask": {"filterState": {"value": ["USD"], "label": "USD"},
                                "extraFormData": {"filters": [{"col": "CurrencyCode", "op": "IN", "val": ["USD"]}]}},
        },
        {
            "id": "NATIVE_FILTER-currency", "name": "FX currency", "filterType": "filter_select",
            "type": "NATIVE_FILTER", "targets": [{"datasetId": ds["fx_rates"], "column": {"name": "CurrencyCode"}}],
            "controlValues": {**select_controls, "multiSelect": True},
            "cascadeParentIds": [], "scope": fx_scope, "chartsInScope": fx_in, "description": "",
            "defaultDataMask": {"filterState": {}, "extraFormData": {}},
        },
    ]


def node(kind, node_id, parents, **meta):
    return {"type": kind, "id": node_id, "children": [], "parents": parents, "meta": meta}


def save_chart(s, spec, dashboard_id, chart_id=None):
    form = {"viz_type": spec["viz"], "datasource": f"{spec['ds']}__table", "time_range": "No filter", **spec["form"]}
    qc = query_context(spec, form)
    rows = call(s, "POST", "chart/data", json=qc)["result"][0]["rowcount"]
    body = {
        "slice_name": spec["name"], "viz_type": spec["viz"], "datasource_id": spec["ds"],
        "datasource_type": "table", "params": json.dumps(form), "query_context": json.dumps(qc),
        "description": spec["description"], "dashboards": [dashboard_id],
    }
    if chart_id:
        call(s, "PUT", f"chart/{chart_id}", json=body)
    else:
        chart_id = call(s, "POST", "chart/", json=body)["id"]
    print(f"chart {spec['name']}: id {chart_id}, {rows} rows")
    return chart_id


def build_layout(charts):
    layout = [
        ("Overview", [("kpi_volume", 3, 30), ("kpi_net", 3, 30), ("kpi_tx", 3, 30), ("kpi_customers", 3, 30)]),
        ("Transaction volume", [("daily_volume", 12, 60)]),
        ("Net cash flow", [("daily_net", 6, 55), ("running_net", 6, 55)]),
        ("Risk profiles", [("risk_pie", 4, 60), ("risk_regions", 8, 60)]),
        ("FX converter", [("fx_latest", 4, 50), ("fx_history", 8, 50)]),
    ]
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    for i, (title, row_charts) in enumerate(layout):
        header_id, row_id = f"HEADER-{i}", f"ROW-{i}"
        position[header_id] = node("HEADER", header_id, ["ROOT_ID", "GRID_ID"], text=title,
                                   headerSize="MEDIUM_HEADER", background="BACKGROUND_TRANSPARENT")
        position[row_id] = node("ROW", row_id, ["ROOT_ID", "GRID_ID"], background="BACKGROUND_TRANSPARENT")
        position["GRID_ID"]["children"] += [header_id, row_id]
        for key, width, height in row_charts:
            c = charts[key]
            position[f"CHART-{key}"] = node("CHART", f"CHART-{key}", ["ROOT_ID", "GRID_ID", row_id], chartId=c["id"],
                                            width=width, height=height, sliceName=c["name"], uuid=c["uuid"])
            position[row_id]["children"].append(f"CHART-{key}")
    return position


def main():
    s = session()
    database_id = ensure_database(s)
    ds = {
        "tx": ensure_dataset(s, database_id, "gold", "fact_transactions"),
        "fx_rates": ensure_dataset(s, database_id, "silver", "fx_rates"),
        "profile": ensure_dataset(s, database_id, "gold", "customer_profile", CUSTOMER_PROFILE_SQL),
        "fx_latest": ensure_dataset(s, database_id, "silver", "fx_latest", FX_LATEST_SQL),
    }
    print("datasets:", ds)
    specs = chart_specs(ds)

    existing = find(s, "dashboard", "dashboard_title", DASHBOARD_TITLE)
    if existing:
        dashboard = call(s, "GET", f"dashboard/{existing[0]['id']}")["result"]
        dashboard_id = dashboard["id"]
        position = json.loads(dashboard["position_json"] or "{}")
        metadata = json.loads(dashboard["json_metadata"] or "{}")
        chart_ids = {k[len("CHART-"):]: v["meta"]["chartId"] for k, v in position.items() if k.startswith("CHART-")}
        missing = [spec["key"] for spec in specs if spec["key"] not in chart_ids]
        if missing:
            raise RuntimeError(f"dashboard layout has no node for {missing}; delete the dashboard to rebuild it")
        for spec in specs:
            save_chart(s, spec, dashboard_id, chart_ids[spec["key"]])
            position[f"CHART-{spec['key']}"]["meta"]["sliceName"] = spec["name"]
        kept_defaults = {f["id"]: f.get("defaultDataMask") for f in metadata.get("native_filter_configuration", [])}
        filters = native_filters(ds, chart_ids)
        for f in filters:
            if f["id"] == "NATIVE_FILTER-date-range" and kept_defaults.get(f["id"]):
                f["defaultDataMask"] = kept_defaults[f["id"]]
    else:
        dashboard_id = call(s, "POST", "dashboard/", json={
            "dashboard_title": DASHBOARD_TITLE, "slug": DASHBOARD_SLUG, "published": True,
        })["id"]
        charts = {}
        for spec in specs:
            chart_id = save_chart(s, spec, dashboard_id)
            charts[spec["key"]] = {"id": chart_id, "name": spec["name"],
                                   "uuid": call(s, "GET", f"chart/{chart_id}")["result"].get("uuid")}
        position = build_layout(charts)
        chart_ids = {k: v["id"] for k, v in charts.items()}
        filters = native_filters(ds, chart_ids)
        metadata = {"chart_configuration": {}, "cross_filters_enabled": False, "refresh_frequency": 0, "color_scheme": ""}

    metadata["native_filter_configuration"] = filters
    metadata["global_chart_configuration"] = {"scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                                              "chartsInScope": list(chart_ids.values())}
    metadata["positions"] = position
    call(s, "PUT", f"dashboard/{dashboard_id}", json={
        "position_json": json.dumps(position), "json_metadata": json.dumps(metadata),
    })
    print(f"dashboard: http://localhost:8088/superset/dashboard/{DASHBOARD_SLUG}/")


main()
