import os

import requests

from qcfractal.testing_helpers import QCATestingSnowflake


def test_homepage_redirect(temporary_database):
    db_config = temporary_database.config

    extra_config = {"homepage_redirect_url": "https://example.com"}
    snowflake = QCATestingSnowflake(db_config, encoding="application/json", extra_config=extra_config)

    # The URI doesn't contain any path or anything
    r = requests.get(snowflake.get_uri(), allow_redirects=False)
    assert r.status_code == 302
    assert r.headers["Location"] == "https://example.com"


def test_homepage_serve_dir(temporary_database, tmp_path_factory):
    homepage_path = str(tmp_path_factory.mktemp("homepage"))
    index_file = os.path.join(homepage_path, "index.html")
    other_file = os.path.join(homepage_path, "other_file.html")

    with open(index_file, "w") as f:
        f.write("HOMEPAGE")
    with open(other_file, "w") as f:
        f.write("OTHER FILE")

    db_config = temporary_database.config
    extra_config = {"homepage_directory": homepage_path}
    snowflake = QCATestingSnowflake(db_config, encoding="application/json", extra_config=extra_config)

    # The URI doesn't contain any path or anything
    r = requests.get(snowflake.get_uri(), headers={"Accept": "text/html"})
    assert r.status_code == 200
    assert r.text == "HOMEPAGE"

    r = requests.get(snowflake.get_uri() + "/other_file.html", headers={"Accept": "text/html"})
    assert r.status_code == 200
    assert r.text == "OTHER FILE"

    r = requests.get(snowflake.get_uri() + "/missing.html", headers={"Accept": "text/html"})
    assert r.status_code == 404
