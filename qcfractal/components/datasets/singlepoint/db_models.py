from sqlalchemy import Column, Integer, ForeignKey, String, JSON
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from qcfractal.components.datasets.db_models import CollectionORM, DatasetMixin, ContributedValuesORM
from qcfractal.storage_sockets.models import Base


class DatasetEntryORM(Base):
    """Association table for many to many"""

    __tablename__ = "dataset_entry"

    dataset_id = Column(Integer, ForeignKey("dataset.id", ondelete="cascade"), primary_key=True)
    # TODO: check the cascase_delete with molecule
    molecule_id = Column(Integer, ForeignKey("molecule.id"), nullable=False)

    name = Column(String, nullable=False, primary_key=True)
    comment = Column(String)
    local_results = Column(JSON)


class DatasetORM(CollectionORM, DatasetMixin):
    """
    The Dataset class for homogeneous computations on many molecules.
    """

    __tablename__ = "dataset"

    id = Column(Integer, ForeignKey("collection.id", ondelete="CASCADE"), primary_key=True)

    contributed_values_obj = relationship(ContributedValuesORM, lazy="selectin", cascade="all, delete-orphan")

    records_obj = relationship(
        DatasetEntryORM, lazy="selectin", cascade="all, delete-orphan", backref="dataset"  # lazy='noload',
    )

    @hybrid_property
    def contributed_values(self):
        return self._contributed_values(self.contributed_values_obj)

    @staticmethod
    def _contributed_values(contributed_values_obj):
        if not contributed_values_obj:
            return {}

        if not isinstance(contributed_values_obj, list):
            contributed_values_obj = [contributed_values_obj]
        ret = {}
        try:
            for obj in contributed_values_obj:
                ret[obj.name.lower()] = obj.to_dict(exclude=["collection_id"])
        except Exception as err:
            pass

        return ret

    @contributed_values.setter
    def contributed_values(self, dict_values):
        return dict_values

    @hybrid_property
    def records(self):
        """calculated property when accessed, not saved in the DB
        A view of the many to many relation"""

        return self._records(self.records_obj)

    @staticmethod
    def _records(records_obj):

        if not records_obj:
            return []

        if not isinstance(records_obj, list):
            records_obj = [records_obj]

        ret = []
        try:
            for rec in records_obj:
                ret.append(rec.to_dict(exclude=["dataset_id"]))
        except Exception as err:
            # raises exception of first access!!
            pass

        return ret

    @records.setter
    def records(self, dict_values):
        return dict_values

    def update_relations(self, records=None, contributed_values=None, **kwarg):

        self.records_obj = []
        records = [] if not records else records
        for rec_dict in records:
            rec = DatasetEntryORM(dataset_id=int(self.id), **rec_dict)
            self.records_obj.append(rec)

        self.contributed_values_obj = []
        contributed_values = {} if not contributed_values else contributed_values
        for key, rec_dict in contributed_values.items():
            rec = ContributedValuesORM(collection_id=int(self.id), **rec_dict)
            self.contributed_values_obj.append(rec)

    __table_args__ = (
        # Index('ix_results_molecule', 'molecule'),  # b-tree index
        # UniqueConstraint("program", "driver", "method", "basis", "keywords", "molecule", name='uix_results_keys'),
    )

    __mapper_args__ = {
        "polymorphic_identity": "dataset",
        # to have separate select when querying CollectionORM
        "polymorphic_load": "selectin",
    }
