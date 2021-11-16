import logging

from pyramid.config import Configurator


logger = logging.getLogger(__name__)


def main(global_config, **settings):
    """This function returns a Pyramid WSGI application."""
    logger.info("Preparing configuration and routes")
    with Configurator(settings=settings) as config:
        config.include("webapp.routes")
        config.scan()
    logger.info("Accepting connections now")
    return config.make_wsgi_app()
