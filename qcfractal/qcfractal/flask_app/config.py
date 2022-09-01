import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:

    # must be set to false to avoid restarting
    DEBUG = False

    # Never propagate exceptions. This uses the default error pages
    # which are HTML, but we are using json...
    PROPAGATE_EXCEPTIONS = False

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(Config):
    pass


class TestingConfig(Config):
    # Testing=True disables error catching during request handling, so that you get better error
    # reports when performing test requests against the application.
    TESTING = True
    DEBUG = False
    JWT_ACCESS_TOKEN_EXPIRES = 2  # seconds


class ProductionConfig(Config):
    DEBUG = False

    @classmethod
    def init_app(cls, app):
        Config.init_app(app)
        # do more production specific


class SnowflakeConfig(ProductionConfig):
    pass


class DockerConfig(ProductionConfig):
    @classmethod
    def init_app(cls, app):
        ProductionConfig.init_app(app)

        # log to stderr
        import logging
        from logging import StreamHandler

        file_handler = StreamHandler()
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)


class UnixConfig(ProductionConfig):
    @classmethod
    def init_app(cls, app):
        ProductionConfig.init_app(app)

        # log to syslog
        import logging
        from logging.handlers import SysLogHandler

        syslog_handler = SysLogHandler()
        syslog_handler.setLevel(logging.INFO)
        app.logger.addHandler(syslog_handler)


config = {
    "development": DevelopmentConfig,
    "testing": TestingConfig,
    "production": ProductionConfig,
    "docker": DockerConfig,
    "unix": UnixConfig,
    "default": DevelopmentConfig,
    "snowflake": SnowflakeConfig,
}
