from __future__ import annotations

import os
import json
import io
import zipfile
import uuid
import base64
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Contoso Control Room",
    page_icon="C",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    :root { --ink:#132238; --muted:#64748b; --line:#d9e2ec; --mint:#d9f4e8; --orange:#f59e0b; --red:#dc4c4c; }
    .stApp { background: #f4f7f9; color: var(--ink); }
    [data-testid="stSidebar"] { background: #102a43; }
    [data-testid="stSidebar"] * { color: #e8f1f7; }
    .hero { background: linear-gradient(120deg,#102a43 0%,#1f5268 62%,#2b7a78 100%); color:white; border-radius:14px; padding:28px 32px; margin:0 0 24px; }
    .hero h1 { font-size:2.2rem; margin:0 0 8px; letter-spacing:0; }
    .hero p { color:#d7e8ef; margin:0; font-size:1rem; }
    .section-label { color:#2b7a78; font-size:.75rem; font-weight:700; letter-spacing:.08em; text-transform:uppercase; margin:22px 0 8px; }
    div[data-testid="stMetric"] { background:white; border:1px solid var(--line); border-radius:10px; padding:14px 16px; box-shadow:0 2px 8px rgba(16,42,67,.04); }
    .status-ok { color:#18794e; font-weight:700; }
    .status-bad { color:#b42318; font-weight:700; }
    .caption { color:var(--muted); font-size:.86rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


QUERIES = {
    "deliveries": """
        SELECT delivery_id, source_system_id, delivery_date, delivery_status,
               expected_object_count, loaded_object_count, completed_at
        FROM {audit}.audit_delivery
        ORDER BY delivery_date DESC, source_system_id
        LIMIT 200
    """,
    "breaches": """
        SELECT 'DEAD_LETTER' AS breach_type, delivery_id,
               concat(layer, '.', entity_id, ': ', coalesce(last_error, '')) AS detail
        FROM {audit}.audit_work_item
        WHERE work_status = 'DEAD_LETTER'
        UNION ALL
        SELECT 'EXPIRED_WORK_LEASE', delivery_id,
               concat(layer, '.', entity_id, ': lease verlopen')
        FROM {audit}.audit_work_item
        WHERE work_status = 'RUNNING' AND lease_expires_at < current_timestamp()
        UNION ALL
        SELECT 'EXPIRED_GOLD_LEASE', NULL,
               concat('Publicatiegroep ', publication_group_id)
        FROM {audit}.audit_gold_publication_lease
        WHERE released_at IS NULL AND expires_at < current_timestamp()
        ORDER BY breach_type, delivery_id
    """,
    "runs": """
        SELECT delivery_id, batch_id, layer, entity_id, run_status,
               rows_read, rows_inserted, rows_rejected, started_at, ended_at,
               round(duration_seconds, 2) AS duration_seconds,
               databricks_job_run_id, error_message
        FROM {audit}.v_load_run_status
        WHERE started_at >= current_timestamp() - INTERVAL 7 DAYS
        ORDER BY started_at DESC
        LIMIT 250
    """,
    "gold": """
        SELECT g.publication_group_id, g.batch_id, g.delivery_id, g.published_at,
               p.gold_entity_id, p.physical_slot, p.row_count
        FROM {audit}.audit_gold_publication_group g
        JOIN {audit}.audit_gold_publication p ON p.batch_id = g.batch_id
        WHERE g.release_status = 'ACTIVE' AND p.publication_status = 'ACTIVE'
        ORDER BY g.published_at DESC, p.gold_entity_id
        LIMIT 200
    """,
    "work_items": """
        SELECT delivery_id, layer, entity_id, work_status, attempt_count,
               max_attempts, lease_expires_at, last_error, updated_at
        FROM {audit}.audit_work_item
        WHERE work_status IN ('DEAD_LETTER', 'RUNNING')
        ORDER BY updated_at DESC
        LIMIT 100
    """,
    "onboarding_drafts": """
        SELECT draft_id, source_system_id, source_object_id, onboarding_scope,
               draft_status, change_reference, created_by, created_at, updated_at
        FROM {audit}.audit_onboarding_draft
        ORDER BY updated_at DESC
        LIMIT 100
    """,
    "etl_solutions": """
        SELECT so.*, ss.source_system_name
        FROM {meta}.meta_source_object so
        LEFT JOIN {meta}.meta_source_system ss
          ON ss.source_system_id = so.source_system_id
        ORDER BY so.source_system_id, so.source_object_id
    """,
    "etl_connectors": """
        SELECT * FROM {meta}.meta_source_connector
        WHERE source_object_id = '{source_object_id}'
    """,
    "etl_mappings": """
        SELECT * FROM {meta}.meta_mapping
        WHERE source_object_id = '{source_object_id}'
        ORDER BY ordinal_position
    """,
    "etl_quality_rules": """
        SELECT * FROM {meta}.meta_quality_rule
        WHERE source_object_id = '{source_object_id}'
        ORDER BY execution_order
    """,
    "etl_dv_mappings": """
        SELECT m.* FROM {meta}.meta_dv_mapping m
        WHERE m.source_object_id = '{source_object_id}'
        ORDER BY m.ordinal_position
    """,
    "etl_dv_entities": """
        SELECT DISTINCT e.* FROM {meta}.meta_dv_entity e
        JOIN {meta}.meta_dv_mapping m ON m.dv_entity_id = e.dv_entity_id
        WHERE m.source_object_id = '{source_object_id}'
        ORDER BY e.load_order
    """,
    "etl_gold_entities": """
        SELECT * FROM {meta}.meta_gold_entity
        WHERE source_system_id = '{source_system_id}'
        ORDER BY load_order
    """,
}

SQL_SCRIPT_FILES = {
    "Bronze-tabellen": "sql/02_bronze/20_bronze_tables.sql",
    "Quality-tabellen": "sql/03_quality_reject/30_quality_tables.sql",
    "Reject-tabellen": "sql/03_quality_reject/31_reject_tables.sql",
    "Raw Data Vault": "sql/04_data_vault/40_raw_vault.sql",
    "Business Vault": "sql/04_data_vault/41_business_vault.sql",
    "Gold historisch": "sql/05_gold/50_gold_historical.sql",
    "Gold actueel": "sql/05_gold/51_gold_current.sql",
}


@st.cache_resource
def sql_connection():
    from databricks import sql
    from databricks.sdk import WorkspaceClient

    workspace = WorkspaceClient()
    host = os.environ.get("DATABRICKS_SERVER_HOSTNAME") or workspace.config.host.removeprefix("https://")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    if not http_path:
        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "06c37b8b16646405")
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        token = workspace.config.oauth_token().access_token
    return sql.connect(server_hostname=host, http_path=http_path, access_token=token)


@st.cache_data(ttl=30, show_spinner=False)
def query(name: str, env: str, **params: str) -> pd.DataFrame:
    catalog = f"contoso_meta_{env}"
    statement = QUERIES[name].format(
        audit=f"{catalog}.audit",
        meta=f"{catalog}.metadata",
        **params,
    )
    with sql_connection().cursor() as cursor:
        cursor.execute(statement)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def save_onboarding_draft(env: str, source_system_id: str, source_object_id: str, scope: str, components: dict) -> str:
    draft_id = str(uuid.uuid4())
    payload = json.dumps(components, separators=(",", ":")).replace("'", "''")
    with sql_connection().cursor() as cursor:
        cursor.execute(
            f"""
                INSERT INTO contoso_meta_{env}.audit.audit_onboarding_draft
            VALUES ('{draft_id}', '{source_system_id}', '{source_object_id}', '{scope}',
                    'DRAFT', NULL, '{payload}', current_user(), current_timestamp(), current_timestamp())
            """
        )
    return draft_id


def run_operator_job(action: str, params: dict[str, str]) -> int:
    from databricks.sdk import WorkspaceClient

    job_id = os.environ.get(f"CONTOSO_{action.upper()}_JOB_ID")
    if not job_id:
        raise RuntimeError(f"Geen job-ID geconfigureerd voor actie {action}.")
    run = WorkspaceClient().jobs.run_now(job_id=int(job_id), job_parameters=params)
    return int(run.run_id)


def github_api(method: str, path: str, payload: dict | None = None) -> dict:
    """Leest of schrijft repository-inhoud via GitHub; schrijft alleen met token."""
    repository = os.environ.get("GITHUB_REPOSITORY", "bobstoelinga/Contoso_lakehouse_v2")
    token = os.environ.get("GITHUB_TOKEN")
    url = f"https://api.github.com/repos/{repository}/{path}"
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "contoso-control-room"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {exc.code}: {detail[:500]}") from exc


def github_script(path: str, branch: str) -> tuple[str, str]:
    result = github_api("GET", f"contents/{path}?ref={urllib.parse.quote(branch)}")
    return base64.b64decode(result["content"]).decode("utf-8"), result["sha"]


def create_script_pull_request(path: str, content: str, source_object_id: str, description: str) -> str:
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN is niet geconfigureerd voor scriptwijzigingen.")
    base = os.environ.get("GITHUB_BASE_BRANCH", "main")
    branch = f"etl/{source_object_id.lower().replace('.', '-')}-{uuid.uuid4().hex[:8]}"
    base_ref = github_api("GET", f"git/ref/heads/{urllib.parse.quote(base)}")
    github_api("POST", "git/refs", {"ref": f"refs/heads/{branch}", "sha": base_ref["object"]["sha"]})
    current = github_api("GET", f"contents/{path}?ref={urllib.parse.quote(base)}")
    github_api(
        "PUT",
        f"contents/{path}",
        {
            "message": f"Update ETL SQL for {source_object_id}",
            "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
            "branch": branch,
            "sha": current["sha"],
        },
    )
    pull = github_api(
        "POST",
        "pulls",
        {
            "title": f"Update ETL SQL for {source_object_id}",
            "head": branch,
            "base": base,
            "body": description,
        },
    )
    return pull["html_url"]


def action_form(action: str, title: str, fields: tuple[str, ...]) -> None:
    with st.expander(title):
        params = {}
        for field in fields:
            params[field] = st.text_input(field.replace("_", " ").title(), key=f"{action}_{field}")
        reason = st.text_input("Reden", key=f"{action}_reason")
        approved_by = st.text_input("Uitgevoerd/goedgekeurd door", key=f"{action}_approved")
        reference = st.text_input("Change- of approvalreferentie", key=f"{action}_reference")
        if st.button("Actie starten", key=f"{action}_submit", type="primary"):
            if not all((reason.strip(), approved_by.strip(), reference.strip())):
                st.error("Reden, goedkeurder en referentie zijn verplicht.")
                return
            try:
                run_id = run_operator_job(action, {**params, "reason": reason, "approved_by": approved_by, "approval_reference": reference})
                st.success(f"{title} gestart. Run-ID: {run_id}")
            except Exception as exc:
                st.error(str(exc))


def selected_action_form(action: str, title: str, params: dict[str, str]) -> None:
    st.markdown(f"### {title}")
    reason = st.text_input("Reden", key=f"selected_{action}_reason")
    approved_by = st.text_input("Uitgevoerd/goedgekeurd door", key=f"selected_{action}_approved")
    reference = st.text_input("Change- of approvalreferentie", key=f"selected_{action}_reference")
    if st.button("Actie starten", key=f"selected_{action}_submit", type="primary"):
        if not all((reason.strip(), approved_by.strip(), reference.strip())):
            st.error("Reden, goedkeurder en referentie zijn verplicht.")
            return
        try:
            run_id = run_operator_job(action, {**params, "reason": reason, "approved_by": approved_by, "approval_reference": reference})
            st.success(f"{title} gestart. Run-ID: {run_id}")
        except Exception as exc:
            st.error(str(exc))


def process_card(title: str, state: str, owner: str, next_step: str, checks: list[str]) -> None:
        color = "#18794e" if state == "Gereed" else "#b45309" if state == "In behandeling" else "#b42318"
        checks_html = "".join(f"<li>{check}</li>" for check in checks)
        st.markdown(
                f"""
                <div style="background:white;border:1px solid #d9e2ec;border-left:5px solid {color};border-radius:10px;padding:18px 20px;margin-bottom:14px;">
                    <div style="display:flex;justify-content:space-between;gap:12px;align-items:center;">
                        <h3 style="margin:0;color:#132238">{title}</h3>
                        <strong style="color:{color}">{state}</strong>
                    </div>
                    <p style="color:#64748b;margin:8px 0">Eigenaar: {owner}</p>
                    <p style="margin:8px 0"><b>Volgende stap:</b> {next_step}</p>
                    <ul style="margin:8px 0 0 18px">{checks_html}</ul>
                </div>
                """,
                unsafe_allow_html=True,
        )


def flow_setup_wizard() -> None:
    st.subheader("Flow Setup")
    st.caption(
        "Maak een gevalideerde metadata-draft. De wizard schrijft niets naar productie; review en merge blijven verplicht."
    )
    with st.form("flow_setup"):
        left, right = st.columns(2)
        with left:
            source_system_id = st.text_input("Bronsysteem-ID", placeholder="CRM")
            source_system_name = st.text_input("Naam bronsysteem", placeholder="Customer CRM")
            object_id = st.text_input("Bronobject-ID", placeholder="CRM.CUSTOMERS")
            object_name = st.text_input("Objectnaam", placeholder="customers")
            file_format = st.selectbox("Bestandsformaat", ["parquet", "json", "csv"])
        with right:
            landing_path = st.text_input("Landing volume pad", placeholder="/Volumes/raw_${env}/crm/landing")
            file_pattern = st.text_input("Bestandspatroon", placeholder="customers*.parquet")
            strategy = st.selectbox(
                "Laadstrategie",
                ["INCREMENTAL_APPEND", "INCREMENTAL_MERGE", "SNAPSHOT_SCD2", "PARTIAL_SNAPSHOT", "FULL_OVERWRITE"],
            )
            business_keys = st.text_input("Business keys", placeholder="customer_id")
            route = st.selectbox("Processing route", ["RAW_VAULT", "REFERENCE_DATA"])
        submitted = st.form_submit_button("Valideer flow-draft", type="primary")

        if not submitted:
            return

        errors = []
        source_system_id = source_system_id.strip().upper()
        object_id = object_id.strip().upper()
        object_name = object_name.strip()
        keys = [key.strip() for key in business_keys.split(",") if key.strip()]
        if not source_system_id or " " in source_system_id:
            errors.append("Bronsysteem-ID is verplicht en mag geen spaties bevatten.")
        if not object_id or not object_id.startswith(f"{source_system_id}."):
            errors.append("Bronobject-ID moet beginnen met het gekozen bronsysteem.")
        if not object_name:
            errors.append("Objectnaam is verplicht.")
        if not landing_path.strip():
            errors.append("Landing volume pad is verplicht.")
        if not file_pattern.strip() or not file_pattern.endswith(f".{file_format}"):
            errors.append(f"Bestandspatroon moet eindigen op .{file_format}.")
        if not keys:
            errors.append("Minimaal één business key is verplicht.")
        if strategy == "FULL_OVERWRITE" and route == "REFERENCE_DATA":
            errors.append("FULL_OVERWRITE is niet toegestaan voor REFERENCE_DATA.")
        if strategy == "SNAPSHOT_SCD2" and route == "REFERENCE_DATA":
            errors.append("SCD2-reference flows moeten expliciet via review worden ingericht.")

        if errors:
            for error in errors:
                st.error(error)
            return

        source_record = {
            "source_system_id": source_system_id,
            "source_system_name": source_system_name.strip() or source_system_id,
            "landing_volume_path": landing_path.strip(),
            "is_active": False,
        }
        object_record = {
            "source_object_id": object_id,
            "source_system_id": source_system_id,
            "object_name": object_name,
            "file_pattern": file_pattern.strip(),
            "file_format": file_format,
            "reader_options": {},
            "load_strategy": strategy,
            "business_key_columns": keys,
            "is_mandatory_in_delivery": True,
            "processing_route": route,
            "is_active": False,
            "schema_drift_policy": "STRICT",
            "bronze_catalog": "contoso_bronze_${env}",
            "bronze_schema": source_system_id.lower(),
            "bronze_table": f"br_{object_name}",
        }
        st.success("Flow-draft gevalideerd. De flow staat bewust op is_active=false totdat review en merge zijn afgerond.")
        st.download_button(
            "Download bronsysteem-metadata",
            json.dumps([source_record], indent=2),
            file_name=f"meta_source_system_{source_system_id.lower()}.json",
            mime="application/json",
        )
        st.download_button(
            "Download bronobject-metadata",
            json.dumps([object_record], indent=2),
            file_name=f"meta_source_object_{object_name}.json",
            mime="application/json",
        )
        st.json({"meta_source_system": source_record, "meta_source_object": object_record})


def full_onboarding_wizard() -> None:
    st.subheader("Complete Source Onboarding")
    st.caption(
        "Genereert een reviewbaar onboardingpakket voor bron, connector, Quality, Data Vault, Gold en workflow."
    )
    with st.form("complete_source_onboarding"):
        source_id = st.text_input("Bronsysteem-ID", placeholder="CRM")
        source_name = st.text_input("Bronsysteemnaam", placeholder="Customer CRM")
        object_name = st.text_input("Bronobjectnaam", placeholder="customers")
        onboarding_scope = st.selectbox(
            "Onboardingscope",
            ["BRON_ONLY", "BRON_AND_VAULT", "END_TO_END_GOLD"],
            format_func=lambda value: {
                "BRON_ONLY": "Bron definiëren en testen",
                "BRON_AND_VAULT": "Bron + Data Vault",
                "END_TO_END_GOLD": "Volledige flow tot Gold",
            }[value],
        )
        file_format = st.selectbox("Bestandsformaat", ["parquet", "json", "csv"])
        connector_type = st.selectbox("Connector", ["FILE_DROP", "HTTP_JSON", "HTTP_CSV", "JDBC"])
        endpoint = st.text_input("Endpoint of landingpad", placeholder="/Volumes/raw_${env}/crm/landing")
        load_strategy = st.selectbox(
            "Laadstrategie", ["INCREMENTAL_APPEND", "INCREMENTAL_MERGE", "SNAPSHOT_SCD2", "PARTIAL_SNAPSHOT"]
        )
        columns_text = st.text_area(
            "Kolommen",
            placeholder="customer_id:string:key\ncustomer_name:string\nemail:string:nullable",
            help="Een kolom per regel: naam:type[:key|nullable]",
        )
        schema_sample = st.file_uploader("Optioneel schema-sample", type=["csv", "json"])
        gold_enabled = st.checkbox("Gold-entiteit opnemen", value=True)
        submit = st.form_submit_button("Genereer onboardingpakket", type="primary")

    if not submit:
        return

    source_id = source_id.strip().upper()
    object_name = object_name.strip().lower()
    object_id = f"{source_id}.{object_name.upper()}"
    errors = []
    if not columns_text.strip() and schema_sample is not None:
        try:
            if schema_sample.name.lower().endswith(".csv"):
                discovered = pd.read_csv(schema_sample, nrows=0).dtypes
            else:
                discovered = pd.read_json(schema_sample).dtypes
            type_map = {
                "object": "STRING", "string": "STRING", "int64": "INT",
                "int32": "INT", "float64": "DOUBLE", "bool": "BOOLEAN",
                "datetime64[ns]": "TIMESTAMP",
            }
            columns_text = "\n".join(
                f"{name}:{type_map.get(str(dtype), 'STRING')}:nullable"
                for name, dtype in discovered.items()
            )
            st.info(f"Schema automatisch ontdekt uit {schema_sample.name}.")
        except Exception as exc:
            errors.append(f"Schema-sample kon niet worden gelezen: {exc}")
    columns = []
    for line in columns_text.splitlines():
        parts = [part.strip() for part in line.split(":")]
        if len(parts) < 2 or not parts[0] or not parts[1]:
            errors.append(f"Ongeldige kolomdefinitie: {line}")
            continue
        columns.append({"name": parts[0], "type": parts[1].upper(), "flags": parts[2:]})
    keys = [column["name"] for column in columns if "key" in column["flags"]]
    if not source_id or " " in source_id:
        errors.append("Bronsysteem-ID is verplicht en mag geen spaties bevatten.")
    if not object_name:
        errors.append("Bronobjectnaam is verplicht.")
    if not endpoint.strip():
        errors.append("Endpoint of landingpad is verplicht.")
    if not columns:
        errors.append("Minimaal één kolom is verplicht.")
    if not keys:
        errors.append("Markeer minimaal één kolom als key.")
    if load_strategy == "SNAPSHOT_SCD2" and len(keys) > 1:
        errors.append("SCD2 met samengestelde keys vereist expliciete Data Vault review.")
    if errors:
        for error in errors:
            st.error(error)
        return

    def inactive(record: dict) -> dict:
        record["is_active"] = False
        return record

    source_system = inactive({
        "source_system_id": source_id,
        "source_system_name": source_name.strip() or source_id,
        "landing_volume_path": f"/Volumes/raw_${{env}}/{source_id.lower()}/landing",
        "delivery_folder_format": "yyyy-MM-dd",
        "delivery_frequency": "ON_DEMAND",
    })
    governance_policy = inactive({
        "source_system_id": source_id,
        "data_owner": "TBD",
        "data_steward": "TBD",
        "data_domain": "TBD",
        "pii_classification": "INTERNAL",
        "retention_days": 365,
        "cost_center": "TBD",
        "sla_tier": "STANDARD",
    })
    source_object = inactive({
        "source_object_id": object_id,
        "source_system_id": source_id,
        "object_name": object_name,
        "file_pattern": f"{object_name}*.{file_format}",
        "file_format": file_format,
        "reader_options": {},
        "load_strategy": load_strategy,
        "business_key_columns": keys,
        "is_mandatory_in_delivery": True,
        "processing_route": "RAW_VAULT",
        "schema_drift_policy": "STRICT",
        "schema_drift_approval_required": False,
        "delete_semantics": "NONE",
        "absence_means_delete": False,
        "schema_contract_version": "1.0",
        "late_arrival_window_days": 30,
        "freshness_sla_hours": 26,
        "backfill_strategy": "FULL_RELOAD",
        "owner_team": "TBD",
        "criticality": "MEDIUM",
        "bronze_partition_columns": ["_delivery_date"],
        "checkpoint_path": f"/Volumes/control_${{env}}/platform/checkpoints/{source_id.lower()}/bronze/br_{object_name}/_checkpoint",
        "schema_location_path": f"/Volumes/control_${{env}}/platform/checkpoints/{source_id.lower()}/bronze/br_{object_name}/_schema",
        "schema_evolution_mode": "addNewColumns",
        "max_files_per_trigger": 1000,
        "bronze_catalog": "contoso_bronze_${env}",
        "bronze_schema": source_id.lower(),
        "bronze_table": f"br_{object_name}",
        "quality_catalog": "contoso_quality_${env}",
        "quality_schema": source_id.lower(),
        "quality_table": f"qa_{object_name}",
        "reject_catalog": "contoso_reject_${env}",
        "reject_schema": source_id.lower(),
        "reject_table": f"rj_{object_name}",
        "load_order": 100,
    })
    connector = inactive({
        "source_object_id": object_id,
        "connector_type": connector_type,
        "endpoint_url": endpoint.strip(),
        "response_format": file_format.upper(),
        "records_key": "value" if connector_type == "HTTP_JSON" else None,
        "request_options": {"timeout_seconds": "60", "max_retries": "3"},
    })
    mappings = [inactive({
        "mapping_id": f"M-QA-{source_id}-{index:03d}",
        "source_object_id": object_id,
        "target_layer": "QUALITY",
        "target_entity": f"qa_{object_name}",
        "source_column": column["name"],
        "source_expression": f"cast({column['name']} as {column['type']})",
        "target_column": column["name"],
        "target_data_type": column["type"],
        "is_business_key": column["name"] in keys,
        "is_nullable": "nullable" in column["flags"] or column["name"] not in keys,
        "ordinal_position": index,
    }) for index, column in enumerate(columns, 1)]
    quality_rules = [inactive({
        "rule_id": f"DQ-{source_id}-{index:03d}",
        "source_object_id": object_id,
        "rule_name": f"{column['name']}_not_null" if column["name"] in keys else f"{column['name']}_quality_review",
        "rule_type": "NOT_NULL" if column["name"] in keys else "CUSTOM",
        "target_columns": [column["name"]],
        "rule_expression": f"{column['name']} IS NOT NULL" if column["name"] in keys else "true",
        "severity": "ERROR" if column["name"] in keys else "WARNING",
        "reject_reason_code": f"R{index:03d}",
        "reject_reason_text": f"Review kwaliteitsregel voor {column['name']}.",
        "execution_order": index * 10,
        "threshold_pct": 0.0 if column["name"] in keys else 10.0,
        "is_blocking": column["name"] in keys,
    }) for index, column in enumerate(columns, 1)]
    dv_entity = inactive({
        "dv_entity_id": f"HUB_{source_id}_{object_name.upper()}",
        "dv_entity_type": "HUB",
        "dv_zone": "RAW_VAULT",
        "target_table_fqn": f"{{vault_catalog}}.raw_vault.hub_{object_name}",
        "physical_table_fqn": f"{{vault_catalog}}.raw_vault.hub_{object_name}",
        "hash_key_column": f"hk_{object_name}",
        "parent_entity_ids": [],
        "business_key_columns": keys,
        "record_source_expr": f"'{object_id}'",
        "load_order": 10,
    })
    dv_mappings = [inactive({
        "dv_mapping_id": f"DVM-{source_id}-{index:03d}",
        "dv_entity_id": dv_entity["dv_entity_id"],
        "source_object_id": object_id,
        "source_expression": key,
        "target_column": key,
        "target_data_type": "STRING",
        "column_role": "BUSINESS_KEY",
        "is_in_hashdiff": False,
        "ordinal_position": index,
    }) for index, key in enumerate(keys, 1)]
    gold_entities = []
    publication_group_id = f"PG_{source_id}"
    gold_product = inactive({
        "publication_group_id": publication_group_id,
        "data_product_name": f"{source_id} {object_name} product",
        "data_owner": "TBD",
        "data_steward": "TBD",
        "consumer_group": "TBD",
        "refresh_sla_hours": 26,
        "compatibility_policy": "BACKWARD_COMPATIBLE",
    })
    if gold_enabled and onboarding_scope == "END_TO_END_GOLD":
        gold_entities.append(inactive({
            "gold_entity_id": f"GC_{source_id}_{object_name.upper()}",
            "gold_layer": "CURRENT",
            "entity_type": "DIMENSION",
            "target_catalog": "contoso_gold_${env}",
            "target_schema": "current",
            "target_table": f"dim_{object_name}",
            "select_sql": f"SELECT * FROM {{vault_catalog}}.raw_vault.hub_{object_name}",
            "business_key_columns": keys,
            "scd_type": "SCD2" if load_strategy == "SNAPSHOT_SCD2" else "NONE",
            "publish_mode": "ATOMIC_SWAP",
            "publication_group_id": publication_group_id,
            "pointer_table": f"vw_{object_name}",
            "staging_table": f"contoso_gold_${{env}}.current_internal.dim_{object_name}_v1",
            "publish_status": "READY",
            "depends_on_gold_entity_ids": [],
            "load_order": 10,
        }))
    workflow = f"""# Review-only draft. Activate after PR, setup and FULL preflight.
resources:
  jobs:
    {source_id.lower()}_{object_name}_load:
      name: "[${{var.env}}] {source_id} {object_name} load"
      tasks:
        - task_key: load_{object_name}
          run_job_task:
            job_id: ${{resources.jobs.contoso_lakehouse_pipeline.id}}
            job_parameters:
              source_system_id: {source_id}
              metadata_validation_mode: PREFLIGHT
"""
    files = {
        "meta_source_system.json": [source_system],
        "meta_data_governance_policy.json": [governance_policy],
        "meta_source_object.json": [source_object],
        "meta_source_connector.json": [connector],
        "meta_mapping.json": mappings,
        "meta_quality_rule.json": quality_rules,
        "ONBOARDING_CHECKLIST.md": f"""# Source onboarding checklist\n\nScope: {onboarding_scope}\n\n- Review generated source and connector metadata\n- Add secrets/connector credentials outside Git\n- Run setup job\n- Run FULL metadata preflight\n- Approve and activate source metadata\n- Run a controlled test delivery\n- Verify Bronze and Quality\n""",
    }
    st.session_state["onboarding_scope"] = onboarding_scope
    if onboarding_scope in {"BRON_AND_VAULT", "END_TO_END_GOLD"}:
        files["meta_dv_entity.json"] = [dv_entity]
        files["meta_dv_mapping.json"] = dv_mappings
        files["ONBOARDING_CHECKLIST.md"] += "- Verify Data Vault hubs, links and satellites\n"
    if onboarding_scope == "END_TO_END_GOLD":
        files["meta_gold_entity.json"] = gold_entities
        files["meta_gold_data_product.json"] = [gold_product]
        files["workflow.job.yml"] = workflow
        files["ONBOARDING_CHECKLIST.md"] += "- Review Gold contract and publication group\n- Verify Gold Historical and Gold Current\n"
    files["ONBOARDING_SCOPE.json"] = {
        "scope": onboarding_scope,
        "gold_included": onboarding_scope == "END_TO_END_GOLD",
        "vault_included": onboarding_scope in {"BRON_AND_VAULT", "END_TO_END_GOLD"},
        "source_is_independent_of_target": True,
    }
    st.session_state["onboarding_files"] = files
    st.session_state["onboarding_review"] = {
        "status": "DRAFT",
        "source_reviewed": False,
        "connector_reviewed": False,
        "quality_reviewed": False,
        "vault_reviewed": False,
        "gold_reviewed": False,
        "change_reference": "",
    }
    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
        for filename, content in files.items():
            payload = content if isinstance(content, str) else json.dumps(content, indent=2)
            bundle.writestr(filename, payload)
    st.success("Volledig onboardingpakket gevalideerd en aangemaakt als inactive draft.")
    st.download_button("Download complete onboarding package", archive.getvalue(), file_name=f"{source_id.lower()}_{object_name}_onboarding.zip")
    st.json({"source_system": source_system, "source_object": source_object, "connector": connector, "gold_entities": gold_entities})


def component_editor() -> None:
    files = st.session_state.get("onboarding_files")
    if not files:
        st.info("Genereer eerst een onboardingpakket. Daarna kun je elk onderdeel afzonderlijk aanpassen.")
        return
    review = st.session_state.setdefault("onboarding_review", {
        "status": "DRAFT",
        "source_reviewed": False,
        "connector_reviewed": False,
        "quality_reviewed": False,
        "vault_reviewed": False,
        "gold_reviewed": False,
        "change_reference": "",
    })
    st.subheader("Reviewstatus")
    review["source_reviewed"] = st.checkbox("Bron en object gecontroleerd", value=review["source_reviewed"])
    review["connector_reviewed"] = st.checkbox("Connector en secrets gecontroleerd", value=review["connector_reviewed"])
    review["quality_reviewed"] = st.checkbox("Mappings en Quality-regels gecontroleerd", value=review["quality_reviewed"])
    review["vault_reviewed"] = st.checkbox("Data Vault-entiteit en mappings gecontroleerd", value=review["vault_reviewed"])
    review["gold_reviewed"] = st.checkbox("Gold-contract en publicatiegroep gecontroleerd", value=review["gold_reviewed"])
    review["change_reference"] = st.text_input("Change-/PR-referentie", value=review["change_reference"])
    if all(review[key] for key in ("source_reviewed", "connector_reviewed", "quality_reviewed", "vault_reviewed", "gold_reviewed")) and review["change_reference"].strip():
        review["status"] = "READY_FOR_PR"
        st.success("Pakket is klaar voor PR-review en volledige metadata-preflight.")
    else:
        review["status"] = "DRAFT"
        st.warning("Pakket blijft DRAFT totdat alle controles en de change-referentie zijn ingevuld.")
    st.caption(f"Pakketstatus: {review['status']}")
    st.divider()
    st.subheader("Individuele componenten aanpassen")
    st.caption("Wijzig één onderdeel zonder de andere drafts te overschrijven. JSON-componenten worden gecontroleerd voordat ze worden opgeslagen.")
    editable = [name for name, content in files.items() if name.endswith(".json")]
    selected = st.selectbox("Onderdeel", editable)
    current = files[selected]
    text = json.dumps(current, indent=2) if not isinstance(current, str) else current
    edited = st.text_area("Draft JSON", text, height=300, key=f"editor_{selected}")
    col_save, col_download = st.columns(2)
    with col_save:
        if st.button("Bewaar onderdeel", type="primary"):
            try:
                files[selected] = json.loads(edited)
                st.session_state["onboarding_files"] = files
                st.success(f"{selected} afzonderlijk bijgewerkt.")
            except json.JSONDecodeError as exc:
                st.error(f"Ongeldige JSON: {exc}")
    with col_download:
        st.download_button("Download onderdeel", edited, file_name=selected, mime="application/json")

    updated_archive = io.BytesIO()
    with zipfile.ZipFile(updated_archive, "w", zipfile.ZIP_DEFLATED) as bundle:
        for filename, content in files.items():
            payload = content if isinstance(content, str) else json.dumps(content, indent=2)
            bundle.writestr(filename, payload)
    st.download_button(
        "Download bijgewerkt onboardingpakket",
        updated_archive.getvalue(),
        file_name="onboarding-package-reviewed.zip",
        mime="application/zip",
    )


def existing_etl_editor(env: str, solutions: pd.DataFrame) -> None:
    """Laat een bestaande bronflow aanpassen als reviewbare onboarding-draft."""
    st.subheader("Bestaande ETL-oplossingen")
    st.caption(
        "Bekijk actieve en inactieve bronflows. Wijzigingen worden als draft opgeslagen en raken de productiemetadata pas na review en merge."
    )
    if solutions.empty:
        st.info("Geen ETL-oplossingen gevonden in de metadata.")
        return None

    header = st.columns([2.2, 1.4, 1.5, 1.8, 0.8])
    for column, label in zip(header, ["Bronobject", "Bronsysteem", "Object", "Laadstrategie", ""]):
        column.markdown(f"**{label}**")
    for _, row in solutions.iterrows():
        columns = st.columns([2.2, 1.4, 1.5, 1.8, 0.8])
        columns[0].write(str(row.get("source_object_id", "")))
        columns[1].write(str(row.get("source_system_id", "")))
        columns[2].write(str(row.get("object_name", "")))
        columns[3].write(str(row.get("load_strategy", "")))
        with columns[4]:
            if st.button("Details", key=f"details_{row['source_object_id']}"):
                st.session_state["existing_etl_solution"] = str(row["source_object_id"])
                st.rerun()

    selected_id = st.selectbox(
        "Geselecteerd detailrecord",
        solutions["source_object_id"].tolist(),
        key="existing_etl_solution",
    )
    selected = solutions[solutions["source_object_id"] == selected_id].iloc[0].to_dict()

    def list_value(field: str) -> str:
        value = selected.get(field, [])
        if pd.api.types.is_list_like(value) and not isinstance(value, (str, bytes, dict)):
            return ", ".join(str(item) for item in value)
        if value is None or (not pd.api.types.is_list_like(value) and pd.isna(value)):
            return ""
        return str(value)

    widget_prefix = f"existing_etl_{selected_id}"
    with st.form(f"edit_etl_{selected_id}"):
        left, right = st.columns(2)
        with left:
            object_name = st.text_input("Objectnaam", value=str(selected.get("object_name") or ""), key=f"{widget_prefix}_object_name")
            file_pattern = st.text_input("Bestandspatroon", value=str(selected.get("file_pattern") or ""), key=f"{widget_prefix}_file_pattern")
            file_format = st.selectbox(
                "Bestandsformaat", ["parquet", "json", "csv"],
                index=["parquet", "json", "csv"].index(str(selected.get("file_format") or "parquet").lower()),
                key=f"{widget_prefix}_file_format",
            )
            load_strategy = st.selectbox(
                "Laadstrategie",
                ["INCREMENTAL_APPEND", "INCREMENTAL_MERGE", "SNAPSHOT_SCD2", "PARTIAL_SNAPSHOT", "FULL_OVERWRITE"],
                index=["INCREMENTAL_APPEND", "INCREMENTAL_MERGE", "SNAPSHOT_SCD2", "PARTIAL_SNAPSHOT", "FULL_OVERWRITE"].index(
                    str(selected.get("load_strategy") or "INCREMENTAL_APPEND")
                ),
                key=f"{widget_prefix}_load_strategy",
            )
            business_keys = st.text_input("Business keys", value=list_value("business_key_columns"), key=f"{widget_prefix}_business_keys")
            change_columns = st.text_input("Kolommen voor wijzigingsdetectie", value=list_value("change_tracking_columns"), key=f"{widget_prefix}_change_columns")
        with right:
            delete_semantics = st.selectbox(
                "Verwijdersemantiek", ["NONE", "SOFT_DELETE_FLAG", "HARD_DELETE"],
                index=["NONE", "SOFT_DELETE_FLAG", "HARD_DELETE"].index(str(selected.get("delete_semantics") or "NONE")),
                key=f"{widget_prefix}_delete_semantics",
            )
            schema_drift_policy = st.selectbox(
                "Schema-driftbeleid",
                ["STRICT", "ALLOW_NEW_COLUMNS_WITH_APPROVAL", "ALLOW_NEW_COLUMNS", "RESCUE"],
                index=(
                    ["STRICT", "ALLOW_NEW_COLUMNS_WITH_APPROVAL", "ALLOW_NEW_COLUMNS", "RESCUE"].index(
                        str(selected.get("schema_drift_policy") or "STRICT")
                    )
                    if str(selected.get("schema_drift_policy") or "STRICT")
                    in ["STRICT", "ALLOW_NEW_COLUMNS_WITH_APPROVAL", "ALLOW_NEW_COLUMNS", "RESCUE"]
                    else 0
                ),
                key=f"{widget_prefix}_schema_drift_policy",
            )
            owner_team = st.text_input("Verantwoordelijk team", value=str(selected.get("owner_team") or ""), key=f"{widget_prefix}_owner_team")
            criticality = st.selectbox(
                "Kritikaliteit", ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
                index=["LOW", "MEDIUM", "HIGH", "CRITICAL"].index(str(selected.get("criticality") or "MEDIUM")),
                key=f"{widget_prefix}_criticality",
            )
            freshness_sla = st.number_input("Freshness-SLA (uur)", min_value=1, value=int(selected.get("freshness_sla_hours") or 24), key=f"{widget_prefix}_freshness_sla")
            load_order = st.number_input("Laadvolgorde", min_value=1, value=int(selected.get("load_order") or 100), key=f"{widget_prefix}_load_order")
            is_active = st.checkbox("ETL-oplossing actief", value=bool(selected.get("is_active", False)), key=f"{widget_prefix}_is_active")
        submitted = st.form_submit_button("Bewaar wijziging als draft", type="primary")

    if submitted:
        try:
            edited = dict(selected)
            edited.update({
                "object_name": object_name.strip(),
                "file_pattern": file_pattern.strip(),
                "file_format": file_format,
                "load_strategy": load_strategy,
                "business_key_columns": [item.strip() for item in business_keys.split(",") if item.strip()],
                "change_tracking_columns": [item.strip() for item in change_columns.split(",") if item.strip()],
                "delete_semantics": delete_semantics,
                "schema_drift_policy": schema_drift_policy,
                "owner_team": owner_team.strip(),
                "criticality": criticality,
                "freshness_sla_hours": freshness_sla,
                "load_order": load_order,
                "is_active": is_active,
            })
            if not edited["object_name"] or not edited["file_pattern"] or not edited["business_key_columns"]:
                raise ValueError("Objectnaam, bestandspatroon en minimaal één business key zijn verplicht.")
            if not edited.get("source_system_id"):
                raise ValueError("Het bronsysteem van deze ETL-oplossing ontbreekt.")
            draft_id = save_onboarding_draft(
                env,
                str(edited["source_system_id"]),
                selected_id,
                "EXISTING_ETL_CHANGE",
                {"meta_source_object.json": [edited]},
            )
            st.success(f"Wijziging opgeslagen als draft: {draft_id}. Review en merge blijven verplicht.")
            st.info("De wijziging staat klaar voor review; de actieve metadata is nog niet aangepast.")
        except ValueError as exc:
            st.error(f"ETL-definitie kon niet worden opgeslagen: {exc}")
        except Exception as exc:
            st.error(f"ETL-draft kon niet worden opgeslagen: {exc}")
    return selected


def metadata_component_editor(
    env: str,
    source: dict,
    components: dict[str, pd.DataFrame],
) -> None:
    """Bewerk gerelateerde metadata via formulieren; nooit rechtstreeks in productie."""
    labels = {
        "etl_connectors": ("Connector", "source_object_id"),
        "etl_mappings": ("Kolommapping", "mapping_id"),
        "etl_quality_rules": ("Quality-regel", "rule_id"),
        "etl_dv_mappings": ("Data Vault-mapping", "dv_mapping_id"),
        "etl_dv_entities": ("Data Vault-entiteit", "dv_entity_id"),
        "etl_gold_entities": ("Gold-entiteit", "gold_entity_id"),
    }
    available = [name for name, (_, key) in labels.items() if not components[name].empty]
    if not available:
        st.info("Voor deze ETL-oplossing zijn geen gerelateerde componenten gevonden.")
        return

    component_name = st.selectbox(
        "Onderdeel beheren",
        available,
        format_func=lambda value: labels[value][0],
        key=f"component_type_{source['source_object_id']}",
    )
    frame = components[component_name]
    key_column = labels[component_name][1]
    options = frame[key_column].astype(str).tolist()
    selected_key = st.selectbox(
        labels[component_name][0], options,
        key=f"component_key_{component_name}_{source['source_object_id']}",
    )
    record = frame[frame[key_column].astype(str) == selected_key].iloc[0].to_dict()

    def is_collection(value: object) -> bool:
        return pd.api.types.is_list_like(value) and not isinstance(value, (str, bytes, dict))

    def text(field: str) -> str:
        value = record.get(field, "")
        if value is None or (not is_collection(value) and pd.isna(value)):
            return ""
        return str(value)

    def list_text(field: str) -> str:
        value = record.get(field, [])
        return ", ".join(str(item) for item in value) if is_collection(value) else text(field)

    def integer(field: str, default: int = 100) -> int:
        try:
            return int(record.get(field) or default)
        except (TypeError, ValueError):
            return default

    with st.form(f"component_form_{component_name}_{selected_key}"):
        edited = dict(record)
        if component_name == "etl_connectors":
            edited["connector_type"] = st.selectbox("Connectortype", ["FILE_DROP", "HTTP_JSON", "HTTP_CSV", "JDBC", "LAKEFLOW_CONNECT"], index=(["FILE_DROP", "HTTP_JSON", "HTTP_CSV", "JDBC", "LAKEFLOW_CONNECT"].index(text("connector_type")) if text("connector_type") in ["FILE_DROP", "HTTP_JSON", "HTTP_CSV", "JDBC", "LAKEFLOW_CONNECT"] else 0))
            edited["endpoint_url"] = st.text_input("Endpoint of landingpad", value=text("endpoint_url"))
            edited["response_format"] = st.selectbox("Responseformaat", ["JSON", "CSV", "PARQUET"], index=(["JSON", "CSV", "PARQUET"].index(text("response_format").upper()) if text("response_format").upper() in ["JSON", "CSV", "PARQUET"] else 0))
            edited["records_key"] = st.text_input("Records-sleutel", value=text("records_key"))
            edited["next_link_key"] = st.text_input("Volgende-pagina-sleutel", value=text("next_link_key"))
        elif component_name == "etl_mappings":
            edited["target_layer"] = st.selectbox("Doellaag", ["BRONZE", "QUALITY", "GOLD_HIST", "GOLD_CURR"], index=(["BRONZE", "QUALITY", "GOLD_HIST", "GOLD_CURR"].index(text("target_layer")) if text("target_layer") in ["BRONZE", "QUALITY", "GOLD_HIST", "GOLD_CURR"] else 0))
            edited["target_entity"] = st.text_input("Doelentiteit", value=text("target_entity"))
            edited["source_column"] = st.text_input("Bronkolom", value=text("source_column"))
            edited["source_expression"] = st.text_input("Bronexpressie", value=text("source_expression"))
            edited["target_column"] = st.text_input("Doelkolom", value=text("target_column"))
            edited["target_data_type"] = st.text_input("Datatype", value=text("target_data_type"))
            edited["is_business_key"] = st.checkbox("Business key", value=bool(record.get("is_business_key", False)))
            edited["is_nullable"] = st.checkbox("Nullable", value=bool(record.get("is_nullable", True)))
            edited["default_value"] = st.text_input("Standaardwaarde", value=text("default_value"))
            edited["ordinal_position"] = st.number_input("Volgorde", min_value=1, value=integer("ordinal_position"))
        elif component_name == "etl_quality_rules":
            edited["rule_name"] = st.text_input("Regelnaam", value=text("rule_name"))
            edited["rule_type"] = st.selectbox("Regeltype", ["NOT_NULL", "UNIQUE", "RANGE", "REGEX", "ALLOWED_VALUES", "REFERENTIAL", "CUSTOM_SQL", "DATA_TYPE"], index=(["NOT_NULL", "UNIQUE", "RANGE", "REGEX", "ALLOWED_VALUES", "REFERENTIAL", "CUSTOM_SQL", "DATA_TYPE"].index(text("rule_type")) if text("rule_type") in ["NOT_NULL", "UNIQUE", "RANGE", "REGEX", "ALLOWED_VALUES", "REFERENTIAL", "CUSTOM_SQL", "DATA_TYPE"] else 0))
            edited["target_columns"] = [item.strip() for item in st.text_input("Doelkolommen", value=list_text("target_columns")).split(",") if item.strip()]
            edited["rule_expression"] = st.text_input("Regel-expressie", value=text("rule_expression"))
            edited["severity"] = st.selectbox("Ernst", ["ERROR", "WARNING"], index=0 if text("severity") != "WARNING" else 1)
            edited["reject_reason_code"] = st.text_input("Reject-code", value=text("reject_reason_code"))
            edited["reject_reason_text"] = st.text_input("Reject-omschrijving", value=text("reject_reason_text"))
            edited["execution_order"] = st.number_input("Uitvoervolgorde", min_value=1, value=integer("execution_order"))
            edited["is_blocking"] = st.checkbox("Blokkerend", value=bool(record.get("is_blocking", True)))
        elif component_name == "etl_dv_mappings":
            edited["source_expression"] = st.text_input("Bronexpressie", value=text("source_expression"))
            edited["target_column"] = st.text_input("Doelkolom", value=text("target_column"))
            edited["target_data_type"] = st.text_input("Datatype", value=text("target_data_type"))
            edited["column_role"] = st.selectbox("Kolomrol", ["HASH_KEY", "BUSINESS_KEY", "HASHDIFF", "DESCRIPTIVE", "DEGENERATE", "DRIVING_KEY", "LOAD_DATE", "RECORD_SOURCE"], index=0)
            edited["is_in_hashdiff"] = st.checkbox("Opnemen in hashdiff", value=bool(record.get("is_in_hashdiff", False)))
            edited["ordinal_position"] = st.number_input("Volgorde", min_value=1, value=integer("ordinal_position"))
        elif component_name == "etl_dv_entities":
            edited["target_table"] = st.text_input("Doeltabel", value=text("target_table"))
            edited["hash_key_column"] = st.text_input("Hash-keykolom", value=text("hash_key_column"))
            edited["business_key_columns"] = [item.strip() for item in st.text_input("Business keys", value=list_text("business_key_columns")).split(",") if item.strip()]
            edited["parent_entity_ids"] = [item.strip() for item in st.text_input("Bovenliggende entiteiten", value=list_text("parent_entity_ids")).split(",") if item.strip()]
            edited["load_order"] = st.number_input("Laadvolgorde", min_value=1, value=integer("load_order"))
        else:
            edited["target_table"] = st.text_input("Doeltabel", value=text("target_table"))
            edited["business_key_columns"] = [item.strip() for item in st.text_input("Business keys", value=list_text("business_key_columns")).split(",") if item.strip()]
            edited["select_sql"] = st.text_area("Gold-selectie", value=text("select_sql"), height=120)
            edited["scd_type"] = st.selectbox("SCD-type", ["NONE", "SCD1", "SCD2", "SNAPSHOT"], index=(["NONE", "SCD1", "SCD2", "SNAPSHOT"].index(text("scd_type")) if text("scd_type") in ["NONE", "SCD1", "SCD2", "SNAPSHOT"] else 0))
            edited["publish_mode"] = st.selectbox("Publicatiemodus", ["ATOMIC_SWAP", "MERGE", "OVERWRITE"], index=(["ATOMIC_SWAP", "MERGE", "OVERWRITE"].index(text("publish_mode")) if text("publish_mode") in ["ATOMIC_SWAP", "MERGE", "OVERWRITE"] else 0))
            edited["publication_group_id"] = st.text_input("Publicatiegroep", value=text("publication_group_id"))
            edited["load_order"] = st.number_input("Laadvolgorde", min_value=1, value=integer("load_order"))
        save = st.form_submit_button("Bewaar onderdeel als draft", type="primary")

    deactivate = st.button("Deactiveer onderdeel", key=f"deactivate_{component_name}_{selected_key}")
    if save or deactivate:
        if deactivate:
            edited["is_active"] = False
        draft_id = save_onboarding_draft(
            env,
            str(source["source_system_id"]),
            str(source["source_object_id"]),
            "EXISTING_ETL_COMPONENT_CHANGE",
            {"meta_source_connector.json" if component_name == "etl_connectors" else
             "meta_mapping.json" if component_name == "etl_mappings" else
             "meta_quality_rule.json" if component_name == "etl_quality_rules" else
             "meta_dv_mapping.json" if component_name == "etl_dv_mappings" else
             "meta_dv_entity.json" if component_name == "etl_dv_entities" else
             "meta_gold_entity.json": [edited]},
        )
        action = "deactivatie" if deactivate else "wijziging"
        st.success(f"{action.capitalize()} opgeslagen als draft: {draft_id}. Review en merge blijven verplicht.")


def sql_script_editor(source_object_id: str) -> None:
    """Toont repository-SQL en maakt een gecontroleerde Pull Request mogelijk."""
    st.subheader("SQL-logica van bron naar output")
    st.caption(
        "SQL wordt uit GitHub geladen. Een wijziging maakt een branch en Pull Request; main en Databricks worden niet rechtstreeks vanuit dit scherm gewijzigd."
    )
    layer = st.selectbox("SQL-laag", list(SQL_SCRIPT_FILES), key=f"sql_layer_{source_object_id}")
    path = SQL_SCRIPT_FILES[layer]
    branch = os.environ.get("GITHUB_BASE_BRANCH", "main")
    try:
        content, file_sha = github_script(path, branch)
    except Exception as exc:
        st.error(f"SQL-script kon niet uit GitHub worden geladen: {exc}")
        return

    st.caption(f"Bestand: `{path}` | basisbranch: `{branch}` | versie: `{file_sha[:10]}`")
    edited = st.text_area("SQL-script", value=content, height=460, key=f"sql_content_{source_object_id}_{path}")
    description = st.text_input(
        "Beschrijving van de wijziging",
        key=f"sql_description_{source_object_id}_{path}",
        placeholder="Waarom moet deze SQL worden aangepast?",
    )
    has_token = bool(os.environ.get("GITHUB_TOKEN"))
    if not has_token:
        st.info("SQL is leesbaar, maar GITHUB_TOKEN ontbreekt. Configureer die als Databricks App-secret om een Pull Request te maken.")
    if st.button(
        "Maak Pull Request voor SQL-wijziging",
        type="primary",
        key=f"sql_submit_{source_object_id}_{path}",
        disabled=not has_token,
    ):
        if not edited.strip() or not description.strip():
            st.error("SQL en een beschrijving van de wijziging zijn verplicht.")
            return
        try:
            pull_url = create_script_pull_request(path, edited, source_object_id, description)
            st.success(f"Pull Request aangemaakt: {pull_url}")
        except Exception as exc:
            st.error(f"Pull Request kon niet worden aangemaakt: {exc}")


st.sidebar.markdown("## Control Room")
env = st.sidebar.selectbox("Omgeving", ["dev", "tst", "prd"], index=0)
if st.sidebar.button("Ververs data"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.markdown('<p class="caption">Data ververst maximaal elke 30 seconden.</p>', unsafe_allow_html=True)

st.markdown(
    '<div class="hero"><h1>Contoso Control Room</h1><p>Operationele gezondheid van deliveries, quality, Vault en Gold.</p></div>',
    unsafe_allow_html=True,
)

try:
    deliveries = query("deliveries", env)
    breaches = query("breaches", env)
    runs = query("runs", env)
    gold = query("gold", env)
    work_items = query("work_items", env)
    onboarding_drafts = query("onboarding_drafts", env)
    etl_solutions = query("etl_solutions", env)
except Exception as exc:
    st.error(f"Databricks SQL is niet bereikbaar: {exc}")
    st.info("Configureer een Databricks CLI-profiel of DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH en DATABRICKS_TOKEN.")
    st.stop()

completed = int((deliveries["delivery_status"] == "COMPLETE").sum()) if not deliveries.empty else 0
failed_runs = int((runs["run_status"] == "FAILED").sum()) if not runs.empty else 0
active_gold = int(gold["gold_entity_id"].nunique()) if not gold.empty else 0
open_actions = len(work_items)

columns = st.columns(4)
columns[0].metric("Deliveries compleet", completed)
columns[1].metric("Actie vereist", open_actions, delta="control plane", delta_color="inverse" if open_actions else "normal")
columns[2].metric("Mislukte runs / 7 dagen", failed_runs, delta_color="inverse")
columns[3].metric("Actieve Gold-entiteiten", active_gold)

st.markdown('<div class="section-label">Operations</div>', unsafe_allow_html=True)
overview, delivery_tab, run_tab, process_tab, flow_tab, etl_tab, action_tab = st.tabs(
    ["Overzicht", "Deliveries", "Runs & Gold", "Processen", "Flow Setup", "ETL-oplossingen", "Operatoracties"]
)

with overview:
    left, right = st.columns([1.25, 1])
    with left:
        st.subheader("Actie vereist")
        if breaches.empty:
            st.success("Geen actieve SLO-breaches, verlopen leases of dead letters.")
        else:
            st.dataframe(breaches, use_container_width=True, hide_index=True)
    with right:
        st.subheader("Laatste deliveries")
        st.dataframe(deliveries.head(12), use_container_width=True, hide_index=True)

with delivery_tab:
    st.subheader("Delivery control plane")
    selected = st.selectbox("Delivery", deliveries["delivery_id"].tolist() if not deliveries.empty else [])
    if selected:
        st.dataframe(runs[runs["delivery_id"] == selected], use_container_width=True, hide_index=True)
        st.caption("Detailqueries kunnen hier rechtstreeks op v_load_run_status en audit_delivery_object worden uitgebreid.")

with run_tab:
    st.subheader("Pipeline runs")
    st.dataframe(runs, use_container_width=True, hide_index=True)
    st.subheader("Actieve Gold-publicaties")
    st.dataframe(gold, use_container_width=True, hide_index=True)

with process_tab:
    st.subheader("Procesregie")
    st.caption("De processtatus komt uit de bestaande audit/control plane. De app voert geen directe audit-writes uit.")
    incident_state = "In behandeling" if not breaches.empty or failed_runs else "Gereed"
    delivery_state = "In behandeling" if not deliveries.empty and (deliveries["delivery_status"] == "IN_PROGRESS").any() else "Gereed"
    change_state = "Gereed" if active_gold else "In behandeling"
    maintenance_state = "In behandeling" if open_actions else "Gereed"
    left, right = st.columns(2)
    with left:
        process_card(
            "Incident & remediation", incident_state, "Data Engineering",
            "Review actieve breaches en herplan alleen met approval.",
            [f"{failed_runs} mislukte runs in zeven dagen", f"{len(breaches)} actieve breaches"],
        )
        process_card(
            "Delivery lifecycle", delivery_state, "Data Operations",
            "Volg delivery van manifest via gate naar Gold-publicatie.",
            [f"{completed} deliveries compleet", f"{len(deliveries)} deliveries zichtbaar"],
        )
    with right:
        process_card(
            "Change & release", change_state, "Platform Engineering",
            "Valideer metadatafingerprint en publiceer gecontroleerd via Gold pointers.",
            [f"{active_gold} actieve Gold-entiteiten", "Preflight vereist dezelfde metadatarelease"],
        )
        process_card(
            "Maintenance", maintenance_state, "Data Platform",
            "Voer onderhoud alleen als dry-run of geautoriseerde job uit.",
            [f"{open_actions} openstaande control-plane-items", "Bestaande maintenance-job als uitvoerpunt"],
        )

with flow_tab:
    full_onboarding_wizard()
    component_editor()
    if "onboarding_files" in st.session_state:
        if st.button("Bewaar onboardingdossier", type="primary"):
            try:
                scope = st.session_state.get("onboarding_scope", "BRON_ONLY")
                source = st.session_state["onboarding_files"]["meta_source_system.json"][0]
                obj = st.session_state["onboarding_files"]["meta_source_object.json"][0]
                draft_id = save_onboarding_draft(
                    env,
                    source["source_system_id"], obj["source_object_id"], scope,
                    st.session_state["onboarding_files"],
                )
                st.success(f"Onboardingdossier opgeslagen: {draft_id}")
            except Exception as exc:
                st.error(f"Onboardingdossier kon niet worden opgeslagen: {exc}")

with etl_tab:
    existing_etl_editor(env, etl_solutions)
    selected_id = st.session_state.get("existing_etl_solution")
    if selected_id:
        selected_source = etl_solutions[etl_solutions["source_object_id"] == selected_id].iloc[0].to_dict()
        try:
            related_components = {
                name: query(
                    name,
                    env,
                    source_object_id=str(selected_source["source_object_id"]),
                    source_system_id=str(selected_source["source_system_id"]),
                )
                for name in ("etl_connectors", "etl_mappings", "etl_quality_rules", "etl_dv_mappings", "etl_dv_entities", "etl_gold_entities")
            }
            st.divider()
            st.subheader("Gerelateerde ETL-componenten")
            metadata_component_editor(env, selected_source, related_components)
            st.divider()
            sql_script_editor(str(selected_source["source_object_id"]))
        except Exception as exc:
            st.error(f"Gerelateerde ETL-componenten konden niet worden geladen: {exc}")

with action_tab:
    st.subheader("Gecontroleerde operatoracties")
    st.caption("Acties roepen bestaande Databricks Jobs aan en schrijven niet rechtstreeks naar audit-tabellen.")
    action_search = st.text_input("Zoek delivery, object of foutmelding", key="operator_action_search")
    search_text = action_search.strip().lower()
    matching_work = work_items[
        work_items.apply(lambda row: search_text in " ".join(str(value).lower() for value in row), axis=1)
    ] if search_text else work_items
    if matching_work.empty:
        st.info("Geen openstaande work-items gevonden.")
    else:
        work_options = matching_work.apply(
            lambda row: f"{row.delivery_id} | {row.layer}.{row.entity_id} | {row.work_status}", axis=1
        ).tolist()
        selected_work = st.selectbox("Work-item", work_options, key="selected_work_item")
        work_row = matching_work.iloc[work_options.index(selected_work)]
        selected_action_form(
            "requeue_dead_letter_work_item",
            "Geselecteerd work-item herplannen",
            {
                "delivery_id": str(work_row.delivery_id),
                "layer": str(work_row.layer),
                "entity_id": str(work_row.entity_id),
            },
        )

    quarantined = deliveries[deliveries["delivery_status"] == "QUARANTINED"] if not deliveries.empty else deliveries
    if not quarantined.empty:
        selected_quarantine = st.selectbox("Quarantined delivery", quarantined["delivery_id"].tolist())
        selected_action_form(
            "release_quarantined_delivery",
            "Quarantined delivery vrijgeven",
            {"delivery_id": selected_quarantine},
        )
    else:
        st.caption("Geen quarantined deliveries beschikbaar.")

    supersede_candidates = deliveries[~deliveries["delivery_status"].isin(["SUPERSEDED", "COMPLETE"])] if not deliveries.empty else deliveries
    if not supersede_candidates.empty:
        selected_supersede = st.selectbox("Delivery voor supersede", supersede_candidates["delivery_id"].tolist())
        selected_action_form(
            "supersede_delivery",
            "Delivery superseden",
            {"delivery_id": selected_supersede},
        )
    st.divider()
    action_form("lakehouse_maintenance", "Maintenance starten", ("dry_run",))

st.caption(f"Laatste applicatie-refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")