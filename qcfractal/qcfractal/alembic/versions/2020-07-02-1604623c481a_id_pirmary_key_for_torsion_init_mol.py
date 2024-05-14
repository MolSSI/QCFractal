"""id(pirmary key) for torsion_init_mol

Revision ID: 1604623c481a
Revises: fb5bd88ae2f3
Create Date: 2020-07-02 18:42:17.267792

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "1604623c481a"
down_revision = "fb5bd88ae2f3"
branch_labels = None
depends_on = None


def upgrade():
    # Removes (harmless) duplicate rows
    op.execute(
        "DELETE FROM torsion_init_mol_association a USING \
    (SELECT MIN(ctid) as ctid, torsion_id, molecule_id \
        FROM torsion_init_mol_association \
        GROUP BY torsion_id, molecule_id HAVING COUNT(*) > 1 \
      ) b \
      WHERE a.torsion_id = b.torsion_id and a.molecule_id = b.molecule_id \
      AND a.ctid <> b.ctid"
    )

    op.execute("alter table torsion_init_mol_association add primary key (torsion_id, molecule_id)")


def downgrade():
    op.execute("alter table torsion_init_mol_association drop constraint torsion_init_mol_association_pkey")
