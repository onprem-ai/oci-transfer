"""FastAPI integration using one process-scoped OCI copy manager."""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, TypedDict

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel

from opai_oci_transfer import (
    AsyncOCIClient,
    CopyManager,
    JobConflictError,
    JobNotFoundError,
    RegistryCredentials,
    StaticCredentialProvider,
)


class AppState(TypedDict):
    copies: CopyManager


class CopyRequest(BaseModel):
    source: str
    destination: str
    platforms: str | tuple[str, ...] = "all"
    overwrite: bool = False


def optional_credentials(prefix: str) -> StaticCredentialProvider | None:
    username = os.environ.get(f"{prefix}_USERNAME")
    password = os.environ.get(f"{prefix}_PASSWORD")
    if not username and not password:
        return None
    if not username or not password:
        raise RuntimeError(f"{prefix}_USERNAME and {prefix}_PASSWORD must be set together")
    return StaticCredentialProvider(prefix.lower(), RegistryCredentials(username, password))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[AppState]:
    client = AsyncOCIClient()
    manager = CopyManager(
        database_path=Path("/var/lib/app/oci-copies.sqlite"),
        client=client,
        workers=1,
        source_credentials=optional_credentials("SOURCE_REGISTRY"),
        destination_credentials=optional_credentials("DESTINATION_REGISTRY"),
    )
    await manager.start()
    try:
        yield {"copies": manager}
    finally:
        await manager.close()
        await client.aclose()


app = FastAPI(lifespan=lifespan)


def get_copy_manager(request: Request) -> CopyManager:
    return request.state.copies


Copies = Annotated[CopyManager, Depends(get_copy_manager)]


@app.post("/copies", status_code=202)
async def create_copy(body: CopyRequest, copies: Copies) -> dict[str, object]:
    try:
        job = await copies.enqueue(
            body.source,
            body.destination,
            platforms=body.platforms,
            replacement_policy="overwrite" if body.overwrite else "no_clobber",
        )
        return job.to_dict()
    except JobConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from None


@app.get("/copies/{job_id}")
async def get_copy(job_id: str, copies: Copies) -> dict[str, object]:
    try:
        return (await copies.get(job_id)).to_dict()
    except JobNotFoundError:
        raise HTTPException(status_code=404, detail="copy not found") from None


@app.delete("/copies/{job_id}")
async def cancel_copy(job_id: str, copies: Copies) -> dict[str, object]:
    try:
        return (await copies.cancel(job_id)).to_dict()
    except JobNotFoundError:
        raise HTTPException(status_code=404, detail="copy not found") from None
