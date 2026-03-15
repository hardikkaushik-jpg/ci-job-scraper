# special_extractors_deep/__init__.py — v3.0
# Master registry. All extractors return list of 5-tuples:
#   (job_link, title, description, location, posting_date)
# Shorter tuples (2 or 3) are accepted but 5-tuples give richer enrichment.
#
# ATS coverage:
#   Greenhouse API  : Databricks, Collibra, Fivetran, Datadog, MongoDB, Boomi, Anomalo
#   Lever API       : Matillion
#   Ashby API       : Atlan, Anomalo, Monte Carlo
#   Workday cxs API : Alteryx, Teradata
#   Playwright DOM  : Snowflake, Salesforce, IBM, Oracle, Sifflet + all others

from .alteryx     import extract_alteryx
from .amazon      import extract_amazon
from .anomalo     import extract_anomalo
from .ataccama    import extract_ataccama
from .atlan       import extract_atlan
from .bigeye      import extract_bigeye
from .boomi       import extract_boomi
from .cloudera    import extract_cloudera
from .collibra    import extract_collibra
from .couchbase   import extract_couchbase
from .dataworld   import extract_dataworld
from .databricks  import extract_databricks
from .datadog     import extract_datadog
from .decube      import extract_decube
from .exasol      import extract_exasol
from .firebolt    import extract_firebolt
from .fivetran    import extract_fivetran
from .ibm         import extract_ibm
from .informatica import extract_informatica
from .influxdata  import extract_influxdata
from .matillion   import extract_matillion
from .mongodb     import extract_mongodb
from .montecarlo  import extract_montecarlo
from .oracle      import extract_oracle
from .pentaho     import extract_pentaho
from .pinecone    import extract_pinecone           # NEW — vector DB
from .precisely   import extract_precisely
from .qdrant      import extract_qdrant             # NEW — vector DB
from .qlik        import extract_qlik
from .sifflet     import extract_sifflet
from .solidatus   import extract_solidatus
from .syniti      import extract_syniti
from .teradata    import extract_teradata
from .vertica     import extract_vertica
from .weaviate    import extract_weaviate           # NEW — vector DB
from .yellowbrick import extract_yellowbrick
from .zilliz      import extract_zilliz             # NEW — Milvus/Zilliz
from .sap         import extract_sap
from .salesforce  import extract_salesforce
from .snowflake   import extract_snowflake

SPECIAL_EXTRACTORS_DEEP = {
    "Alteryx":     extract_alteryx,
    "Amazon":      extract_amazon,
    "Anomalo":     extract_anomalo,
    "Ataccama":    extract_ataccama,
    "Atlan":       extract_atlan,
    "BigEye":      extract_bigeye,
    "Boomi":       extract_boomi,
    "Cloudera":    extract_cloudera,
    "Collibra":    extract_collibra,
    "Couchbase":   extract_couchbase,
    "Data.World":  extract_dataworld,
    "Databricks":  extract_databricks,
    "Datadog":     extract_datadog,
    "Decube":      extract_decube,
    "Exasol":      extract_exasol,
    "Firebolt":    extract_firebolt,
    "Fivetran":    extract_fivetran,
    "IBM":         extract_ibm,
    "Informatica": extract_informatica,
    "InfluxData":  extract_influxdata,
    "Matillion":   extract_matillion,
    "MongoDB":     extract_mongodb,
    "Monte Carlo": extract_montecarlo,
    "Oracle":      extract_oracle,
    "Pentaho":     extract_pentaho,
    "Pinecone":    extract_pinecone,                # NEW
    "Precisely":   extract_precisely,
    "Qdrant":      extract_qdrant,                  # NEW
    "Qlik":        extract_qlik,
    "Sifflet":     extract_sifflet,
    "Solidatus":   extract_solidatus,
    "Syniti":      extract_syniti,
    "Teradata":    extract_teradata,
    "Vertica":     extract_vertica,
    "Weaviate":    extract_weaviate,                # NEW
    "Yellowbrick": extract_yellowbrick,
    "Zilliz":      extract_zilliz,                  # NEW — covers Milvus
    "SAP":         extract_sap,
    "Salesforce":  extract_salesforce,
    "Snowflake":   extract_snowflake,
}
