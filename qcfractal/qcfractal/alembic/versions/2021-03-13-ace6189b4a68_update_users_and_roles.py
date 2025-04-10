"""update users and roles

Revision ID: ace6189b4a68
Revises: 038ffd952a00
Create Date: 2021-03-13 15:53:00.374469

"""

import logging

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import table, column

from migration_helpers.default_roles import default_roles

# revision identifiers, used by Alembic.
revision = "ace6189b4a68"
down_revision = "038ffd952a00"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "role",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("rolename", sa.String(), nullable=False),
        sa.Column("permissions", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("rolename", name="ux_role_rolename"),
    )

    # Create some temporary tables for updating
    user_table = table(
        "user",
        column("username", sa.String),
        column("permissions", postgresql.JSON(astext_type=sa.Text)),
        column("role_id", sa.Integer),
    )

    role_table = table(
        "role",
        column("id", sa.Integer),
        column("rolename", sa.String),
        column("permissions", sa.JSON),
    )

    bind = op.get_bind()
    session = Session(bind=bind)

    # Populate the role table with with the default roles
    for rolename, permissions in default_roles.items():
        session.execute(role_table.insert().values({"rolename": rolename, "permissions": permissions}))
    session.flush()

    # Now get the ids of the just-added roles
    role_query = session.query(role_table).all()
    role_map = {r.rolename: r.id for r in role_query}

    # Now modify the user table
    op.add_column(
        "user",
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="True"),
    )
    op.add_column("user", sa.Column("email", sa.String(), nullable=False, server_default=""))
    op.add_column("user", sa.Column("fullname", sa.String(), nullable=False, server_default=""))
    op.add_column(
        "user",
        sa.Column("organization", sa.String(), nullable=False, server_default=""),
    )

    # Set as nullable first, then change after it is populated
    op.add_column("user", sa.Column("role_id", sa.Integer(), nullable=True))

    op.drop_constraint("user_username_key", "user", type_="unique")
    op.create_unique_constraint("ux_user_username", "user", ["username"])
    op.create_foreign_key(None, "user", "role", ["role_id"], ["id"])

    # For all users, determine what the new role should be
    users = session.query(user_table).all()
    for user in users:
        permissions = set(user.permissions)
        if "admin" in permissions:
            role_id = role_map["admin"]
        elif permissions == {"read"}:
            role_id = role_map["read"]
        elif permissions <= {"write", "compute"}:
            role_id = role_map["submit"]
        elif permissions == {"queue"}:
            role_id = role_map["compute"]
        else:
            logging.getLogger().warning(
                f"!!!! User {user.username} does not have permissions that map to a single role. YOU MUST MANUALLY FIX THIS. Setting to 'read' for now"
            )
            role_id = role_map["read"]

        session.execute(user_table.update().where(user_table.c.username == user.username).values({"role_id": role_id}))

    session.flush()

    # Now we can drop the old permissions column and make the role_id column not nullable
    op.drop_column("user", "permissions")
    op.alter_column("user", "role_id", existing_type=sa.Integer(), nullable=False)

    session.commit()
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "user",
        sa.Column(
            "permissions",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=True,
        ),
    )
    op.drop_constraint(None, "user", type_="foreignkey")
    op.drop_index(op.f("ix_user_username"), table_name="user")
    op.create_unique_constraint("user_username_key", "user", ["username"])
    op.drop_column("user", "role_id")
    op.drop_column("user", "organization")
    op.drop_column("user", "fullname")
    op.drop_column("user", "enabled")
    op.drop_column("user", "email")
    op.drop_table("role")

    logging.getLogger().warning(f"!!!! All user permissions have been set to null. You must change this manually")

    # ### end Alembic commands ###
