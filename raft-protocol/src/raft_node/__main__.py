import uvicorn

from raft_node.api import create_app
from raft_node.config import Settings


def main() -> None:
    settings = Settings()

    uvicorn.run(
        create_app(settings=settings),
        host=settings.bind_host,
        port=settings.bind_port,
        log_level=settings.log_level.lower()
    )


if __name__ == "__main__":
    main()
