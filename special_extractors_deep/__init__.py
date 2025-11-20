# __init__.py
# Master registry for all 27 deep extractors

from .extractor_alation import extract_alation
from .extractor_amazon import extract_amazon
from .extractor_ataccama import extract_ataccama
from .extractor_atlan import extract_atlan
from .extractor_bigeye import extract_bigeye
from .extractor_cloudera import extract_cloudera
from .extractor_collibra import extract_collibra
from .extractor_couchbase import extract_couchbase
from .extractor_dataworld import extract_dataworld
from .extractor_datadog import extract_datadog
from .extractor_decube import extract_decube
from .extractor_exasol import extract_exasol
from .extractor_firebolt import extract_firebolt
from .extractor_ibm import extract_ibm
from .extractor_informatica import extract_informatica
from .extractor_influxdata import extract_influxdata
from .extractor_matillion import extract_matillion
from .extractor_mongodb import extract_mongodb
from .extractor_oracle import extract_oracle
from .extractor_pentaho import extract_pentaho
from .extractor_qlik import extract_qlik
from .extractor_sifflet import extract_sifflet
from .extractor_solidatus import extract_solidatus
from .extractor_syniti import extract_syniti
from .extractor_teradata import extract_teradata
from .extractor_vertica import extract_vertica
from .extractor_yellowbrick import extract_yellowbrick
from .extractor_sap import extract_sap
from .extractor_salesforce import extract_salesforce

# Master registry
SPECIAL_EXTRACTORS_DEEP = {
    "Alation": extract_alation,
    "Amazon": extract_amazon,
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
    "Matillion": extract_matillion,
    "MongoDB": extract_mongodb,
    "Oracle": extract_oracle,
    "Pentaho": extract_pentaho,
    "Qlik": extract_qlik,
    "Sifflet": extract_sifflet,
    "Solidatus": extract_solidatus,
    "Syniti": extract_syniti,
    "Teradata": extract_teradata,
    "Vertica": extract_vertica,
    "Yellowbrick": extract_yellowbrick,
    "SAP": extract_sap,
    "Salesforce": extract_salesforce,
}

