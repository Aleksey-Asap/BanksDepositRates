import os


def is_docker_env():
    if os.path.exists('/.dockerenv') or os.getenv('DOCKER_ENV') is not None:
        return True