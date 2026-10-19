"""
Work with Claude Code (CC) to complete this prefect flow.

The best way to do this is to open CC in the root dir of this
project, and your prompt can reference @05_prefect/07_github_api_pagination.py
to ask for a plan for how to complete the steps. the instructors
inline comments already add to the prompt - i.e. to paginate over that
API endpoint for a certain number of pages, and to store the output
in a single duckdb table. Ask CC to enumerate the steps you will need
to do this, and REQUIRE it to work with you on each task, one by one.

Read what CC generates/suggests with each step, and ask yourself if you
understand the python it has generated. IF YOU DO NOT UNDERSTAND IT WELL,
this is your chance to learn a bit more python! Ask CC to explain the specific
lines that are unclear to you, and have a back and forth with CC until you
get a sense for what is going on. You know about loops, and you know about
fetching remote web content, and so the growth here is understanding how
to cycle through a series of related links to build up all 1000 records.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import httpx
import pyarrow as pa
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner


# commit history for the git/git repository
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/repos/git/git/commits"
PER_PAGE = 100
PAGES = 10
DB_FILE = "actions.duckdb"                               # local duckdb file, created if missing
LOG_FILE = Path(__file__).parent / "github_ingest.log"   # sits next to this script

# copy everything Prefect logs (including print() from log_prints tasks) into a local file
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
logging.getLogger("prefect").addHandler(file_handler)


# ------ only one task ------
# create a single prefect task that loops through 10 pages
# of git/git commits and stores each one into a duckdb table.
# add more tasks and more data if you like, and do more with it.
# if successful you should have 1000 records in your db.

def flatten(c):
    """Pull the fields we care about out of one nested commit object into a flat dict."""
    gh_author = c.get("author") or {}   # null when the commit email isn't linked to a GitHub account
    return {
        "sha": c["sha"],
        "author_name": c["commit"]["author"]["name"],
        "author_email": c["commit"]["author"]["email"],
        "author_login": gh_author.get("login"),
        "authored_at": c["commit"]["author"]["date"],
        "committer_name": c["commit"]["committer"]["name"],
        "committed_at": c["commit"]["committer"]["date"],
        "message": c["commit"]["message"],
        "html_url": c["html_url"],
    }


@task(name="ingest", log_prints=True, retries=3, retry_delay_seconds=5)
def ingest_data():
    # work with claude to fetch 100 records for each page
    # and pull 10 pages total. This could be done in series,
    # or if you want to look at previous prefect .map examples
    # you could pull some/all pages concurrently.
    #
    # for help with github api pagination see
    # https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2026-03-10
    #
    # httpx parses it into response.links. The "next" URL already includes 
    # per_page and page, so params are only needed on the first request. 
    # The loop ends when there is no "next" link (last page).

    # this task already comes with a name, retry settings, and anything
    # you print() is written to the log.
    # visit https://sds-prefect.pods.uvarc.io/v2/runs to review your flows

    # if you need to see a working example of this, see this file in this repo
    # 04_apis/05_pagination.py

    logger = get_run_logger()   # like print(), but lets us log at ERROR level

    # follow the "next" link until we've pulled PAGES pages (or run out of pages)
    url = URL
    params = {"per_page": PER_PAGE}   # only needed on the first request
    page = 1
    all_commits = []
    try:
        while url and page <= PAGES:
            response = httpx.get(url, headers=HEADERS, params=params, timeout=30)
            response.raise_for_status()   # 4xx/5xx -> exception, instead of parsing an error body as commits
            commits = response.json()
            all_commits.extend(flatten(c) for c in commits)   # flatten each commit, add to running list
            print(f"page {page}: {len(commits)} commits (total {len(all_commits)})")

            url = response.links.get("next", {}).get("url")   # None on the last page
            params = None                                     # "next" url already has per_page/page
            page += 1
    except httpx.HTTPStatusError as e:
        # GitHub answered with an error, e.g. 401 bad token or 403 rate limited
        logger.error(f"GitHub returned {e.response.status_code} on page {page}: {e.response.text[:200]}")
        raise   # re-raise so the task fails and Prefect's retries kick in
    except httpx.RequestError as e:
        # never got an answer: timeout, DNS failure, dropped connection
        logger.error(f"request for page {page} failed: {e!r}")
        raise

    try:
        # "with" closes the connection when the block ends, even if an error is raised inside it
        with duckdb.connect(DB_FILE) as conn:
            # CREATE OR REPLACE so re-running the flow rebuilds the table instead of duplicating rows
            conn.execute("""
                CREATE OR REPLACE TABLE commits (
                    sha            VARCHAR PRIMARY KEY,
                    author_name    VARCHAR,
                    author_email   VARCHAR,
                    author_login   VARCHAR,
                    authored_at    TIMESTAMPTZ,
                    committer_name VARCHAR,
                    committed_at   TIMESTAMPTZ,
                    message        VARCHAR,
                    html_url       VARCHAR
                )
            """)

            # build one in-memory arrow table from the list of dicts and insert it in a
            # single statement (executemany ran one INSERT per row: ~14s vs well under 1s)
            conn.register("new_rows", pa.Table.from_pylist(all_commits))
            conn.execute("INSERT INTO commits BY NAME SELECT * FROM new_rows")

            count = conn.execute("SELECT count(*) FROM commits").fetchone()[0]
    except duckdb.Error as e:
        # e.g. file locked by another process, duplicate sha, bad timestamp
        logger.error(f"writing to {DB_FILE} failed: {e}")
        raise

    print(f"commits table now has {count} rows")
    return count


@flow(name="azt6gn-flow")
def main_flow():
    ingest_data()

if __name__ == "__main__":
  main_flow()
