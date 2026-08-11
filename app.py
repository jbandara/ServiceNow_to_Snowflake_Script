import requests
import pandas as pd
import re
import os
# import psycopg2
# import psycopg2.extras
import json
from datetime import datetime, timezone, timedelta
from cryptography.hazmat.backends import default_backend
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
# from utils import get_secret

requests.packages.urllib3.disable_warnings()
 
BASE_URL = "https://kmartaus.service-now.com"
 
TAGS_API = f"{BASE_URL}/api/kmw/get_inc_tag/getTag"
INCIDENT_API = f"{BASE_URL}/api/now/table/incident"
WORKNOTES_API = f"{BASE_URL}/api/now/table/sys_journal_field"


USERNAME = "SVCKRISPRT"
PASSWORD = ",oA!pH__?g_Vhi)T8lH_SynW4t0)Iy4=:)}jFsA4"


ist = timezone(timedelta(hours=5, minutes=30))
def get_timestamp():
    return datetime.now(ist).strftime("%Y-%m-%d_%H-%M-%S")


def chunk_items(items, chunk_size):
    for index in range(0, len(items), chunk_size):
        yield items[index:index + chunk_size]

# STEP 1 — Extract Incidents
def fetch_and_process_incidents(session, incident_fields, group_filter, last_run_time):
    all_incidents = []
    offset = 0
    limit = 3000

    params_inc = {
        "sysparm_fields": incident_fields,
        # "sysparm_limit": limit,
        # "sysparm_offset": offset,
        "sysparm_query": f"assignment_group.nameIN{group_filter}^sys_updated_on>={last_run_time}"
        # "sysparm_query": f"assignment_group.nameIN{group_filter}"
    }

    print('getting incidents data from service now.....')
    response = session.get(INCIDENT_API, params=params_inc, timeout=60)
    print(f'got response from service now. status={response.status_code}')

    if response.status_code != 200:
        print(f"Failed to fetch incidents: {response.status_code}")
        print(response.text[:1000])
        return []

    try:
        data = response.json()
    except ValueError:
        print("Response is not valid JSON. First 1000 chars:")
        print(response.text[:1000])
        return []

    result_records = data.get("result", [])
    if not result_records:
        print("No incidents")
    else:
        print(f"data length: {len(result_records)}")
        # print("records:")
        # print(json.dumps(result_records, indent=2))

    batch = result_records
    print(f"received {len(batch)} incidents in current batch")

    if not batch:
        print("No more incidents. Stopping pagination.")

    all_incidents.extend(batch)
    print(f"Total incidents fetched: {len(all_incidents)}")
    return all_incidents

# STEP 2 — Tags
def fetch_tags_for_incidents(session, assignment_groups):
    tags_dict = {}
    all_tag_records = []
    for group in assignment_groups:
        params_tags = {"name": group}
        tag_response = session.get(TAGS_API, params=params_tags)
        try:
            tag_records = tag_response.json().get("result", {}).get("data", [])
        except:
            tag_records = []
 
        all_tag_records.extend(tag_records)
 
    tags_df = pd.DataFrame(all_tag_records)
    if tags_df.empty:
        print("No tags")
    else:
        print(f"tags count: {len(tags_df)}")
        # print(f"first 10 tags length: {len(tags_df.head(10))}")
        # print("first 10 tags records:")
        # print(
        #     tags_df[['incident_number', 'tags']]
        #     .head(10)
        #     .to_json(orient="records", indent=2)
        # )
    return tags_df

# STEP 3 — Work Notes
def fetch_work_notes_for_incidents(session, incident_df):
    if incident_df.empty or "sys_id" not in incident_df.columns:
        print("No incidents available for work notes")
        return []

    notes_list = []
    sys_id_list = incident_df["sys_id"].dropna().astype(str).tolist()

    for batch_number, sys_id_batch in enumerate(chunk_items(sys_id_list, 100), start=1):
        sys_ids = ",".join(sys_id_batch)
        params_notes = {
            "sysparm_query": f"elementINwork_notes,comments^element_idIN{sys_ids}",
            "sysparm_fields": "sys_created_on,element_id,value,sys_created_by,element",
            "sysparm_limit": "10000"
        }

        try:
            response_notes = session.get(WORKNOTES_API, params=params_notes, timeout=60)
        except requests.exceptions.RequestException as error:
            print(f"Work notes request failed for batch {batch_number}: {error}")
            continue

        if response_notes.status_code != 200:
            print(f"Failed to fetch work notes for batch {batch_number}: {response_notes.status_code}")
            print(response_notes.text[:1000])
            continue

        try:
            notes = response_notes.json().get("result", [])
            incident_map = dict(zip(incident_df["sys_id"], incident_df["number"]))
            for note in notes:
 
                sys_id = note["element_id"]
                incident_number = incident_map.get(sys_id)
            
                text = note["value"]
                text = re.split("_{10,}", text)[0].strip()
            
                formatted = f"{note['sys_created_on']} — {note['sys_created_by']}: {text}"
            
                notes_list.append({
                    "number": incident_number,
                    "formatted_note": formatted
                })
            
            notes_df = pd.DataFrame(notes_list)

        except ValueError:
            print(f"Work notes response is not valid JSON for batch {batch_number}. First 1000 chars:")
            print(response_notes.text[:1000])
            continue

        print(f"work notes batch {batch_number} count: {len(notes_df)}")
        # all_notes.extend(notes)

    if notes_df.empty:
        print("No work notes data")
        return []

    print(f"work notes count: {len(notes_df)}")
    print("first 10 work notes records:")
    # print(json.dumps(notes_df.head(10).to_dict(orient="records"), indent=2))
    return notes_df

# STEP 4 — Jira extraction
def jira_extraction(notes_df):
    jira_pattern = re.compile(r"\b(?:DISCO|RANGE|BP)\s*-\s*\d+\b", re.IGNORECASE)
    print(f"work notes count:---- {len(notes_df)}")
    if not notes_df.empty:
    
        notes_df = notes_df.groupby("number")["formatted_note"] \
            .apply(lambda x: "\n\n".join(x)) \
            .reset_index()
    
        notes_df["JIRA_CARD"] = notes_df["formatted_note"].str.findall(jira_pattern)
    
        notes_df["JIRA_CARD"] = notes_df["JIRA_CARD"].apply(
            lambda x: ",".join(re.sub(r"\s+", "", match).upper() for match in x) if isinstance(x, list) else ""
        )

        print(f"jira_extraction length: {len(notes_df)}")
        print("jira_extraction first 10 JIRA_CARD values:")
        print(notes_df["JIRA_CARD"].head(10).tolist())
        return notes_df

    print("jira_extraction length: 0")
    # print("jira_extraction first 10 JIRA_CARD values:")
    print([])
    return notes_df



def lambda_handler(event, context):
    print('Lambda function has started')
    SNOWFLAKE_TOKEN = "5cjAEsZ47LERssUdg"
    # SNOWFLAKE_TOKEN = os.environ['SNOWFLAKE_TOKEN']

    # secret_response = get_secret()
    # secrets = json.loads(secret_response)
    # print(f"secrets: {secrets}")
    
    private_key_password = SNOWFLAKE_TOKEN.encode("utf-8") if SNOWFLAKE_TOKEN else None
    target_database = "KSFTA"
    target_schema = "DDRPF"


    PRIVATE_KEY_PATH = os.path.join(os.path.dirname(__file__),"rsa_key.p8")
    print('rsa key file path =', PRIVATE_KEY_PATH)

    with open(PRIVATE_KEY_PATH, "rb") as key:
    # with open("./rsa_key.p8", "rb") as key:
        p_key= serialization.load_pem_private_key(
        key.read(),
        password=private_key_password,
        # password=SNOWFLAKE_TOKEN.encode(),
        backend=default_backend()
    )
    pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())


    snowflake.connector.paramstyle = 'numeric'

    snowflake_conn = snowflake.connector.connect(
    user='SVCDISCO_DDRPF',
    account='kmartau.ap-southeast-2',
    private_key=pkb,
    warehouse='KSF_DISCO_WH',
    database=target_database,
    schema=target_schema,
    role ='KSF_DISCO'
    )

    cursor = snowflake_conn.cursor()
    cursor.execute(f"USE DATABASE {target_database}")
    try:
        cursor.execute(f"USE SCHEMA {target_schema}")
        print(f"Using preferred schema: {target_schema}")
    except snowflake.connector.errors.ProgrammingError:
        print(f"Schema unavailable. Falling back to: {target_schema}")
    print('Snowflake connected successfully')

    # with snowflake_conn.cursor() as source_cursor:
    #     source_cursor.execute('select * from "KSFTA"."DDRPF"."TPR" limit 10')
    #     elc_costing_data = source_cursor.fetchall()
    #     print(f"Sample data from Snowflake: {elc_costing_data}")



    session = requests.Session()
    session.auth = (USERNAME, PASSWORD)
    session.headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "Connection": "keep-alive"
    })

    query = """
    SELECT COALESCE(MAX(sys_updated_on),'2020-01-01 00:00:00')
    FROM KSFTA.DDRPF.SERVICENOW_INCIDENTS 
    """
    cursor.execute(query)
    
    last_run_time = cursor.fetchone()[0]
    print(f"Last run time: {last_run_time}")
    # return

    assignment_groups = [
        "KMART IT MERCH – KRIS",    
        "KMART IT MERCH - RANGING TOOL",
        "KMART IT MERCH - BUYPLAN"
    ]
 
    group_filter = ",".join(assignment_groups)
    print('group--', group_filter)
    incident_fields = ",".join([
    "number","short_description","assignment_group.name","description","u_solution","sys_created_on",
    "severity","state","sys_updated_on","u_classification","lessons_learned",
    "sys_created_by","knowledge","u_resolved","u_incident_type","impact","active",
    "u_servicenow_platform_name","u_choice_1","made_sla","closed_at","sys_id",
    "urgency","sys_tags","u_resolution_code","priority","business_duration",
    "u_servicenow_platform_group","u_customer_acceptance","u_jira_ref_number",
    "task_effective_number","sys_updated_by","opened_at"
    ])

    all_incidents = fetch_and_process_incidents(session, incident_fields, group_filter, last_run_time)
    incident_df = pd.DataFrame(all_incidents)
    # return
    print('Tags call started....')
    tags_df = fetch_tags_for_incidents(session, assignment_groups)
    print('Tags call end....')

    print('Work notes call started....')
    all_work_notes = fetch_work_notes_for_incidents(session, incident_df)
    print('Work notes call end....')

    print('Jira extraction started....')
    notes_df = jira_extraction(all_work_notes)
    # print('Jira extraction end....', all_work_notes.head(10).to_dict(orient="records"))
    print('Jira extraction end....')

    # merge dataframes and write to snowflake
    merged = incident_df.copy()
 
    if not tags_df.empty and "sys_id" in tags_df.columns:
        merged = merged.merge(tags_df[["sys_id","tags"]], on="sys_id", how="left")
    
    merged = merged.merge(notes_df, on="number", how="left")

    # merged.columns = [str(col).strip().replace(" ", "_").upper() for col in merged.columns]

    
    merged.columns = [
        str(col).strip()
        .replace(" ", "_")
        .replace(".", "_")     # <<< IMPORTANT: convert ASSIGNMENT_GROUP.NAME -> ASSIGNMENT_GROUP_NAME
        .upper()
        for col in merged.columns
    ]


    # with snowflake_conn.cursor() as source_cursor:
    #     source_cursor.execute(
    #         f'DESCRIBE TABLE "{target_database}"."{target_schema}"."SERVICENOW_INCIDENTS"'
    #     )
    #     table_columns = [row[0].upper() for row in source_cursor.fetchall()]

    # incoming_columns = merged.columns.tolist()
    # matched_columns = [col for col in incoming_columns if col in table_columns]
    # dropped_columns = [col for col in incoming_columns if col not in table_columns]

    # if dropped_columns:
    #     print(f"Dropping non-target columns: {dropped_columns}")

    # if not matched_columns:
    #     raise ValueError("No matching columns found between merged data and SERVICENOW_INCIDENTS table")

    # merged = merged[matched_columns]

    target_columns = [
        "NUMBER",
        "SHORT_DESCRIPTION",
        "DESCRIPTION",
        "U_SOLUTION",
        "JIRA_CARD",
        "SYS_CREATED_ON",
        "SEVERITY",
        "STATE",
        "SYS_UPDATED_ON",
        "SYS_CREATED_BY",
        "IMPACT",
        "PRIORITY",
        "SYS_ID",
        "OPENED_AT",
        "TAGS",
        "FORMATTED_NOTE",
        "ASSIGNMENT_GROUP_NAME"
    ]

    missing_columns = [col for col in target_columns if col not in merged.columns]
    for col in missing_columns:
        merged[col] = None

    merged = merged[target_columns]

    print(f"merged length: {len(merged)}")
    # print("merged first 10 records:")
    # print(merged.head(10).to_dict(orient="records"))
    print(f"[{get_timestamp()}] Data consolidation complete")


    # # STEP 6 — Load into Snowflake
    # success, nchunks, nrows, _ = write_pandas(
    #     snowflake_conn,
    #     merged,
    #     "SERVICENOW_INCIDENTS",
    #     database=target_database,
    #     schema=target_schema
    #     # auto_create_table=True
    # )

    # print(f"[{get_timestamp()}] Snowflake Load Completed")
    # print(f"Rows Inserted: {nrows}")

    # STEP 6 — Upsert into Snowflake (update existing + insert new on NUMBER)
    stage_table = f"SERVICENOW_INCIDENTS_STAGE_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    with snowflake_conn.cursor() as source_cursor:
        source_cursor.execute(
            f'CREATE TEMP TABLE "{target_database}"."{target_schema}"."{stage_table}" '
            f'LIKE "{target_database}"."{target_schema}"."SERVICENOW_INCIDENTS"'
        )

    success, nchunks, nrows, _ = write_pandas(
        snowflake_conn,
        merged,
        stage_table,
        database=target_database,
        schema=target_schema
    )

    if not success:
        raise RuntimeError("Failed to stage data in Snowflake before MERGE")
    merge_columns = [f'"{col}"' for col in target_columns]
    update_columns = [col for col in target_columns if col != "NUMBER"]
    update_clause = ", ".join([f'tgt."{col}" = src."{col}"' for col in update_columns])
    insert_columns = ", ".join(merge_columns)
    insert_values = ", ".join([f'src.{col}' for col in merge_columns])

    merge_sql = f'''
        MERGE INTO "{target_database}"."{target_schema}"."SERVICENOW_INCIDENTS" AS tgt
        USING "{target_database}"."{target_schema}"."{stage_table}" AS src
        ON tgt."NUMBER" = src."NUMBER"
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
    '''

    with snowflake_conn.cursor() as source_cursor:
        source_cursor.execute(merge_sql)

    print(f"[{get_timestamp()}] Snowflake Load Completed")
    print(f"Rows Inserted: {nrows}")
    cursor.close()
    snowflake_conn.close()
    print("Snowflake connection is closed")
    #extra spaces removed

lambda_handler({}, {}) 