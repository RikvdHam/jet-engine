from fastapi.middleware.trustedhost import TrustedHostMiddleware


def add_trusted_hosts(app, hosts: list[str]):
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=hosts,
    )
