"""Authentication and authorization for the API. Needs the `api` extra.

roles.py       roles → permissions
principal.py   who is calling, and what their ACL lets them touch
tokens.py      JWT issue and verify
passwords.py   Argon2id
users.py       accounts
"""

from pgtaskqueue.auth.principal import Principal
from pgtaskqueue.auth.roles import ROLE_PERMISSIONS, Permission, Role, permissions_for
from pgtaskqueue.auth.tokens import AuthConfigError, TokenError, TokenService
from pgtaskqueue.auth.users import UserError, UserService

__all__ = [
    "ROLE_PERMISSIONS",
    "AuthConfigError",
    "Permission",
    "Principal",
    "Role",
    "TokenError",
    "TokenService",
    "UserError",
    "UserService",
    "permissions_for",
]
