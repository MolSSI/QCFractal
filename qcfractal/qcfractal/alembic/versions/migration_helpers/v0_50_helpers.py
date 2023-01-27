import json

import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "bf4b379a6ce4"


def get_empty_keywords_id(conn):
    res = conn.execute(sa.text("SELECT id FROM keywords WHERE hash_index = 'bf21a9e8fbc5a3846fb05b4fa0859e0917b2202f'"))
    return res.scalar()


def add_qc_spec(conn, program, driver, method, basis, keywords_id, protocols):
    if basis is None:
        basis = ""
    if keywords_id is None:
        keywords_id = get_empty_keywords_id(conn)

    # Remove protocol defaults
    if protocols.get("wavefunction", None) in [None, "none"]:
        protocols.pop("wavefunction", None)
    if protocols.get("stdout", True) is True:
        protocols.pop("stdout", None)

    e_corr = protocols.pop("error_correction", None)

    if e_corr is not None:
        if e_corr.get("default_policy", True) is True:
            e_corr.pop("default_policy", None)
        if e_corr.get("policies", {}) in [None, {}]:
            e_corr.pop("policies", None)

    if e_corr not in [None, {}]:
        protocols["error_correction"] = e_corr

    protocols_str = json.dumps(protocols)

    conn.execute(
        sa.text(
            """
               INSERT INTO qc_specification (program, driver, method, basis, keywords_id, protocols)
               VALUES (:program, :driver, :method, :basis, :keywords_id, (:protocols)::jsonb)
               ON CONFLICT DO NOTHING
               """
        ),
        parameters=dict(
            program=program, driver=driver, method=method, basis=basis, keywords_id=keywords_id, protocols=protocols_str
        ),
    )

    res = conn.execute(
        sa.text(
            """SELECT id FROM qc_specification
                   WHERE program = :program
                   AND driver = :driver
                   AND method = :method
                   AND basis = :basis
                   AND keywords_id = :keywords_id
                   AND protocols = (:protocols)::jsonb
                """
        ),
        parameters=dict(
            program=program, driver=driver, method=method, basis=basis, keywords_id=keywords_id, protocols=protocols_str
        ),
    )
    return res.scalar()


def add_opt_spec(conn, qc_specification_id, program, keywords, protocols):
    keywords_str = json.dumps(keywords)

    # Remove protocol defaults
    if protocols.get("trajectory", "all") in [None, "all"]:
        protocols.pop("trajectory", None)

    protocols_str = json.dumps(protocols)

    conn.execute(
        sa.text(
            """
           INSERT INTO optimization_specification (program, keywords, protocols, qc_specification_id)
           VALUES (:program, (:keywords)::jsonb, (:protocols)::jsonb, :qc_specification_id)
           ON CONFLICT DO NOTHING
           """
        ),
        parameters=dict(
            program=program, keywords=keywords_str, protocols=protocols_str, qc_specification_id=qc_specification_id
        ),
    )

    res = conn.execute(
        sa.text(
            """
                SELECT id FROM optimization_specification
                WHERE program = :program
                AND keywords = (:keywords)::jsonb
                AND protocols = (:protocols)::jsonb
                AND qc_specification_id = :qc_specification_id
                """
        ),
        parameters=dict(
            program=program, keywords=keywords_str, protocols=protocols_str, qc_specification_id=qc_specification_id
        ),
    )

    return res.scalar()
