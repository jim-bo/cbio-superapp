def run(port: int = 8000, host: str = "127.0.0.1"):
    import uvicorn
    from cbioportal.web.app import create_app
    
    app = create_app()
    uvicorn.run(app, host=host, port=port)
