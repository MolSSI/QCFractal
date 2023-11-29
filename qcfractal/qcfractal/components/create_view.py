import os

from sqlalchemy import select, create_engine, Column, String, ForeignKey, LargeBinary
from sqlalchemy.orm import selectinload, sessionmaker, declarative_base

from qcfractal.db_socket.socket import SQLAlchemySocket
from qcportal.compression import compress, CompressionEnum
from qcportal.serialization import serialize

ViewBaseORM = declarative_base()


class DatasetViewEntry(ViewBaseORM):
    __tablename__ = "dataset_entry"
    name = Column(String, primary_key=True)
    data = Column(LargeBinary, nullable=False)


class DatasetViewSpecification(ViewBaseORM):
    __tablename__ = "dataset_specification"
    name = Column(String, primary_key=True)
    data = Column(LargeBinary, nullable=False)


class DatasetViewRecord(ViewBaseORM):
    __tablename__ = "dataset_record"
    entry_name = Column(String, ForeignKey(DatasetViewEntry.name), primary_key=True)
    specification_name = Column(String, ForeignKey(DatasetViewSpecification.name), primary_key=True)
    data = Column(LargeBinary, nullable=False)


class DatasetViewMetadata(ViewBaseORM):
    __tablename__ = "dataset_metadata"

    key = Column(String, ForeignKey(DatasetViewEntry.name), primary_key=True)
    value = Column(LargeBinary, nullable=False)


def _serialize_orm(orm, exclude=None):
    s_data = serialize(orm.model_dict(exclude=exclude), "application/msgpack")
    c_data, _, _ = compress(s_data, CompressionEnum.zstd, 7)
    return c_data


def create_dataset_view(dataset_id: int, socket: SQLAlchemySocket, view_file_path: str):
    if os.path.exists(view_file_path):
        raise RuntimeError(f"File {view_file_path} exists - will not overwrite")

    if os.path.isdir(view_file_path):
        raise RuntimeError(f"{view_file_path} is a directory")

    uri = "sqlite:///" + view_file_path
    engine = create_engine(uri)
    ViewSession = sessionmaker(bind=engine)

    ViewBaseORM.metadata.create_all(engine)

    view_session = ViewSession()

    with socket.session_scope(True) as fractal_session:
        ds_type = socket.datasets.lookup_type(dataset_id)
        ds_socket = socket.datasets.get_socket(ds_type)

        dataset_orm = ds_socket.dataset_orm
        entry_orm = ds_socket.entry_orm
        specification_orm = ds_socket.specification_orm
        record_item_orm = ds_socket.record_item_orm

        stmt = select(dataset_orm).where(dataset_orm.id == dataset_id)
        stmt = stmt.options(selectinload("*"))
        ds_orm = fractal_session.execute(stmt).scalar_one()

        # Metadata
        metadata_bytes = _serialize_orm(ds_orm)
        metadata_orm = DatasetViewMetadata(key="raw_data", value=metadata_bytes)
        view_session.add(metadata_orm)
        view_session.commit()

        # Entries
        stmt = select(entry_orm)
        stmt = stmt.options(selectinload("*"))
        stmt = stmt.where(entry_orm.dataset_id == dataset_id)
        entries = fractal_session.execute(stmt).scalars().all()

        for entry in entries:
            entry_bytes = _serialize_orm(entry)
            entry_orm = DatasetViewEntry(name=entry.name, data=entry_bytes)
            view_session.add(entry_orm)

        view_session.commit()

        # Specifications
        stmt = select(specification_orm)
        stmt = stmt.options(selectinload("*"))
        stmt = stmt.where(specification_orm.dataset_id == dataset_id)
        specs = fractal_session.execute(stmt).scalars().all()

        for spec in specs:
            spec_bytes = _serialize_orm(spec)
            specification_orm = DatasetViewSpecification(name=spec.name, data=spec_bytes)
            view_session.add(specification_orm)

        view_session.commit()

        base_stmt = select(record_item_orm).where(record_item_orm.dataset_id == dataset_id)
        base_stmt = base_stmt.options(selectinload("*"))
        base_stmt = base_stmt.order_by(record_item_orm.record_id.asc())

        skip = 0
        while True:
            stmt = base_stmt.offset(skip).limit(10)
            batch = fractal_session.execute(stmt).scalars()

            count = 0
            for item_orm in batch:
                item_bytes = _serialize_orm(item_orm)

                view_record_orm = DatasetViewRecord(
                    entry_name=item_orm.entry_name,
                    specification_name=item_orm.specification_name,
                    data=item_bytes,
                )

                view_session.add(view_record_orm)
                count += 1

            view_session.commit()

            if count == 0:
                break

            skip += count
