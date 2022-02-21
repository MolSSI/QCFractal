from sqlalchemy import Column, Integer, ForeignKey, JSON, String
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from qcfractal.components.datasets.db_models import CollectionORM, DatasetMixin, ContributedValuesORM
from qcfractal.components.datasets.singlepoint.db_models import SinglepointDatasetORM
from qcfractal.db_socket import BaseORM


class ReactionDatasetEntryORM(BaseORM):
    """Association table for many to many"""

    __tablename__ = "reaction_dataset_entry"

    reaction_dataset_id = Column(Integer, ForeignKey("reaction_dataset.id", ondelete="cascade"), primary_key=True)

    attributes = Column(JSON)
    name = Column(String, nullable=False, primary_key=True)
    reaction_results = Column(JSON)
    stoichiometry = Column(JSON)
    extras = Column(JSON)


class ReactionDatasetORM(CollectionORM, DatasetMixin):
    """
    Reaction Dataset
    """

    __tablename__ = "reaction_dataset"

    id = Column(Integer, ForeignKey("collection.id", ondelete="CASCADE"), primary_key=True)

    ds_type = Column(String)

    records_obj = relationship(
        ReactionDatasetEntryORM, lazy="selectin", cascade="all, delete-orphan", backref="reaction_dataset"
    )

    contributed_values_obj = relationship(ContributedValuesORM, lazy="selectin", cascade="all, delete-orphan")

    @hybrid_property
    def contributed_values(self):
        return self._contributed_values(self.contributed_values_obj)

    @staticmethod
    def _contributed_values(contributed_values_obj):
        return SinglepointDatasetORM._contributed_values(contributed_values_obj)

    @contributed_values.setter
    def contributed_values(self, dict_values):
        return dict_values

    def update_relations(self, records=None, contributed_values=None, **kwarg):

        self.records_obj = []
        records = records or []
        for rec_dict in records:
            rec = ReactionDatasetEntryORM(reaction_dataset_id=int(self.id), **rec_dict)
            self.records_obj.append(rec)

        self.contributed_values_obj = []
        contributed_values = {} if not contributed_values else contributed_values
        for key, rec_dict in contributed_values.items():
            rec = ContributedValuesORM(collection_id=int(self.id), **rec_dict)
            self.contributed_values_obj.append(rec)

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
                ret.append(rec.to_dict(exclude=["reaction_dataset_id"]))
        except Exception as err:
            # raises exception of first access!!
            pass
        return ret

    @records.setter
    def records(self, dict_values):
        return dict_values

    __table_args__ = (
        # Index('ix_results_molecule', 'molecule'),  # b-tree index
        # UniqueConstraint("program", "driver", "method", "basis", "keywords", "molecule", name='uix_results_keys'),
    )

    __mapper_args__ = {
        "polymorphic_identity": "reactiondataset",
        # to have separate select when querying CollectionORM
        "polymorphic_load": "selectin",
    }
