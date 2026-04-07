"""
Decision Maker Scraper — Web API
"""

import asyncio
import uuid
import os
import csv
import json
import time
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from app.scraper_engine import run_scraper_job, load_domains_from_file

# ── Config ──
JOBS_DIR = Path(os.getenv("JOBS_DIR", "./jobs"))
JOBS_DIR.mkdir(exist_ok=True)

app = FastAPI(title="Decision Maker Scraper", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job tracking (survives within process lifetime)
# For Railway with single instance this is fine. For multi-instance, use Redis.
jobs = {}


def load_persisted_jobs():
    """Reload job metadata from disk on startup."""
    for job_dir in JOBS_DIR.iterdir():
        if job_dir.is_dir():
            meta_path = job_dir / "meta.json"
            if meta_path.exists():
                with open(meta_path) as f:
                    jobs[job_dir.name] = json.load(f)


@app.on_event("startup")
async def startup():
    load_persisted_jobs()


def save_job_meta(job_id: str):
    meta_path = JOBS_DIR / job_id / "meta.json"
    with open(meta_path, "w") as f:
        json.dump(jobs[job_id], f)


# ═══════════════════════════════════════════════════════════════
#  ENDPOINTS
# ═══════════════════════════════════════════════════════════════

@app.post("/api/jobs")
async def create_job(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    column: Optional[str] = Form(None),
    workers: int = Form(15),
    timeout: int = Form(10),
    enable_dork: bool = Form(False),
    enable_verify: bool = Form(False),
):
    """Upload a CSV and start a scrape job."""
    job_id = str(uuid.uuid4())[:8]
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(exist_ok=True)

    # Save uploaded file
    input_path = job_dir / "input.csv"
    content = await file.read()
    with open(input_path, "wb") as f:
        f.write(content)

    # Parse domains
    try:
        domains, domain_meta = load_domains_from_file(str(input_path), column)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not domains:
        raise HTTPException(status_code=400, detail="No valid domains found in CSV.")

    output_path = str(job_dir / "results.csv")

    jobs[job_id] = {
        "id": job_id,
        "status": "queued",
        "created_at": datetime.utcnow().isoformat(),
        "total_domains": len(domains),
        "completed": 0,
        "emails_found": 0,
        "people_found": 0,
        "inferred_emails": 0,
        "domains_with_data": 0,
        "workers": workers,
        "timeout": timeout,
        "enable_dork": enable_dork,
        "enable_verify": enable_verify,
        "error": None,
        "output_file": output_path,
    }
    save_job_meta(job_id)

    # Launch background scrape
    background_tasks.add_task(
        _run_job, job_id, domains, domain_meta, output_path,
        workers, timeout, enable_dork, enable_verify
    )

    return {"job_id": job_id, "total_domains": len(domains)}


async def _run_job(job_id, domains, domain_meta, output_path,
                   workers, timeout, enable_dork, enable_verify):
    """Background task that runs the scraper and updates job state."""
    jobs[job_id]["status"] = "running"
    save_job_meta(job_id)

    def progress_callback(completed, emails, people, inferred, domains_with_data):
        jobs[job_id]["completed"] = completed
        jobs[job_id]["emails_found"] = emails
        jobs[job_id]["people_found"] = people
        jobs[job_id]["inferred_emails"] = inferred
        jobs[job_id]["domains_with_data"] = domains_with_data
        # Persist periodically (every 10 domains)
        if completed % 10 == 0:
            save_job_meta(job_id)

    try:
        await run_scraper_job(
            domains, domain_meta, output_path,
            workers, timeout, enable_dork, enable_verify,
            progress_callback
        )
        jobs[job_id]["status"] = "completed"
    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)

    save_job_meta(job_id)


@app.get("/api/jobs")
async def list_jobs():
    """List all jobs."""
    sorted_jobs = sorted(jobs.values(), key=lambda j: j["created_at"], reverse=True)
    return sorted_jobs


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get job status and progress."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]


@app.get("/api/jobs/{job_id}/results")
async def get_results(job_id: str, limit: int = 100, offset: int = 0, min_score: int = 0):
    """Get job results as JSON (paginated, filterable by score)."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job status: {job['status']}")

    output_path = job["output_file"]
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Results file not found")

    rows = []
    with open(output_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            score = int(row.get("Score", 0) or 0)
            if score >= min_score:
                rows.append(row)

    total = len(rows)
    page = rows[offset:offset + limit]
    return {"total": total, "offset": offset, "limit": limit, "rows": page}


@app.get("/api/jobs/{job_id}/download")
async def download_results(job_id: str):
    """Download results CSV."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = jobs[job_id]
    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"Job status: {job['status']}")

    output_path = job["output_file"]
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="Results file not found")

    return FileResponse(
        output_path,
        media_type="text/csv",
        filename=f"decision_makers_{job_id}.csv"
    )


@app.delete("/api/jobs/{job_id}")
async def delete_job(job_id: str):
    """Delete a job and its files."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    import shutil
    job_dir = JOBS_DIR / job_id
    if job_dir.exists():
        shutil.rmtree(job_dir)

    del jobs[job_id]
    return {"deleted": job_id}


@app.post("/api/quick-scrape")
async def quick_scrape(
    background_tasks: BackgroundTasks,
    domains: str = Form(...),
    workers: int = Form(10),
    timeout: int = Form(10),
):
    """Quick scrape: paste domains directly (newline-separated)."""
    domain_list = [d.strip() for d in domains.strip().split("\n") if d.strip()]
    from app.scraper_engine import normalize_domain
    cleaned = []
    domain_meta = {}
    for d in domain_list:
        nd = normalize_domain(d)
        if nd and "." in nd:
            cleaned.append(nd)
            domain_meta[nd] = {"company_name": "", "category": ""}

    if not cleaned:
        raise HTTPException(status_code=400, detail="No valid domains provided.")

    cleaned = sorted(set(cleaned))
    job_id = str(uuid.uuid4())[:8]
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(exist_ok=True)
    output_path = str(job_dir / "results.csv")

    jobs[job_id] = {
        "id": job_id,
        "status": "queued",
        "created_at": datetime.utcnow().isoformat(),
        "total_domains": len(cleaned),
        "completed": 0,
        "emails_found": 0,
        "people_found": 0,
        "inferred_emails": 0,
        "domains_with_data": 0,
        "workers": workers,
        "timeout": timeout,
        "enable_dork": False,
        "enable_verify": False,
        "error": None,
        "output_file": output_path,
    }
    save_job_meta(job_id)

    background_tasks.add_task(
        _run_job, job_id, cleaned, domain_meta, output_path,
        workers, timeout, False, False
    )

    return {"job_id": job_id, "total_domains": len(cleaned)}


# ── Serve frontend ──
static_dir = Path(__file__).parent.parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
