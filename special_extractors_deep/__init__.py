# __init__.py
# Master registry for all deep extractors

from .alteryx import extract_alteryx
from .amazon import extract_amazon
from .anomalo import extract_anomalo
from .ataccama import extract_ataccama
from .atlan import extract_atlan
from .bigeye import extract_bigeye
from .cloudera import extract_cloudera
from .collibra import extract_collibra
from .couchbase import extract_couchbase
from .dataworld import extract_dataworld
from .databricks import extract_bricks
from .datadog import extract_datadog
from .decube import extract_decube
from .exasol import extract_exasol
from .firebolt import extract_firebolt
from .ibm import extract_ibm
from .informatica import extract_informatica
from .influxdata import extract_influxdata
from .montecarlo import extract_montecarlo
from .oracle import extract_oracle
from .pentaho import extract_pentaho
from .precisely import extract_precisely
from .qlik import extract_qlik    
from .sifflet import extract_sifflet
from .solidatus import extract_solidatus
from .syniti import extract_syniti
from .teradata import extract_teradata
from .vertica import extract_vertica
from .yellowbrick import extract_yellowbrick
from .sap import extract_sap
from .salesforce import extract_salesforce
from .snowflake import extract_snowflake


# Master registry
SPECIAL_EXTRACTORS_DEEP = {
    "Alteryx": extract_alteryx,
    "Amazon": extract_amazon,
    "Anomalo": extract_anomalo,
    "Ataccama": extract_ataccama,
    "Atlan": extract_atlan,
    "BigEye": extract_bigeye,
    "Cloudera": extract_cloudera,
    "Collibra": extract_collibra,
    "Couchbase": extract_couchbase,
    "Data.World": extract_dataworld,
    "Datadog": extract_datadog,
    "Decube": extract_decube,
    "Exasol": extract_exasol,
    "Firebolt": extract_firebolt,
    "IBM": extract_ibm,
    "Informatica": extract_informatica,
    "InfluxData": extract_influxdata,
    "Monte Carlo": extract_montecarlo,
    "Oracle": extract_oracle,
    "Pentaho": extract_pentaho,
    "Precisely": extract_precisely,
    "Qlik": extract_qlik,
    "Sifflet": extract_sifflet,
    "Solidatus": extract_solidatus,
    "Syniti": extract_syniti,
    "Teradata": extract_teradata,
    "Vertica": extract_vertica,
    "Yellowbrick": extract_yellowbrick,
    "SAP": extract_sap,
    "Salesforce": extract_salesforce,
    "Snowflake": extract_snowflake,
}
