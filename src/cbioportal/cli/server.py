def run(port: int = 8000, host: str = "127.0.0.1", workers: int = 1):
    import uvicorn

    if workers > 1:
        # Multi-worker mode requires an import string — each worker process
        # imports the app factory independently, so they can't share an object.
        uvicorn.run(
            "cbioportal.web.app:create_app",
            host=host,
            port=port,
            workers=workers,
            factory=True,
        )
    else:
        from cbioportal.web.app import create_app
        uvicorn.run(create_app(), host=host, port=port)
