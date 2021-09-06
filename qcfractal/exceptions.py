class UserReportableError(RuntimeError):
    """
    An error reportable to the end user

    Exceptions of this class or derived classes are able to be reported to the end user.
    Many exceptions should only be viewable by an administrator as they may leak implementation
    details or other sensitive information. Errors of this class are safe to report to all kinds
    of non-admin users.
    """

    pass


class UserManagementError(UserReportableError):
    pass


class AuthenticationFailure(UserReportableError):
    pass


class ValidationError(UserReportableError):
    pass


class MissingDataError(UserReportableError):
    pass
