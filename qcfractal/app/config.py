import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:

    JWT_SECRET_KEY = "super-secret"
    JWT_ACCESS_TOKEN_EXPIRES = 60 * 60 * 24 * 7
    JWT_REFRESH_TOKEN_EXPIRES = 60 * 60 * 24 * 30

    SECRET_KEY = os.environ.get("SECRET_KEY") or "hard to guess string"

    # TODO, not used
    DB_LOGGING = True

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(Config):
    DEBUG = True


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
}
