from qcfractal.storage_sockets.models.sql_base import Base, MsgpackExt
from qcfractal.interface.models.records import DriverEnum
from qcfractal.storage_sockets.models import OptimizationProcedureORM, KeywordsORM
from sqlalchemy.orm import relationship, Session
from sqlalchemy import (
    JSON,
    Column,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    UniqueConstraint,
    func,
    select,
    create_engine
)

class QCSpecORM(Base):

    __tablename__ = 'qc_spec'

    id = Column('id', Integer, primary_key=True)
    program = Column('program', String(100), nullable=False)
    basis = Column('basis', String(100))
    method = Column('method', String(100), nullable=False)
    driver = Column('driver', String(100), Enum(DriverEnum), nullable=False)
    keywords = Column('keywords', ForeignKey("keyword.id", lazy='select'))
    keywords_obj = relationship('keywords.id', lazy='select', cascade='all')

db_name = 'molecule_tests'
db_uri = 'postgres://apple@localhost/' + db_name
engine = create_engine(db_uri)

session = Session(bind=engine)
optim_class = OptimizationProcedureORM
qc_spec_records = session.query(optim_class.qc_spec, optim_class.program, optim_class.keywords).distinct()


keyword_ids = [record.keyword for record in qc_spec_records]
found_ids = session.query(KeywordsORM).filter_by(KeywordsORM.id == keyword_ids).all()
non_existent_ids = set(keyword_ids) - set(found_ids)
# what to do with non_existent ids?

qc_spec_records = []
for row in qc_spec_records:
    qc_json = row.qc_spec
    qc_program, qc_keywords = row.program, row.keywords
    qc_spec_records.append(QCSpecORM(program = qc_program, basis = qc_json['basis'],
                           method=qc_json['method'], driver=qc_json['driver'], keywords=qc_keywords))


session.add_all(qc_spec_records)
session.commit()

# getting the ids of qc_specs
qc_spec_ids = [qc_spec.id for qc_spec in qc_spec_records]
# Adding the reference to the new qc_spec table
for spec in qc_spec_records:
    obj_id, basis, method, driver, program, keywords = spec.id, spec.basis, spec.method, spec.driver, spec.program, spec.keywords
    session.query(optim_class).filter(optim_class.qc_spec['basis'] == basis, optim_class.qc_spec['method'] == method,
                                      optim_class.qc_spec['driver'] == driver, optim_class.program == program,
                                      optim_class.keywords == keywords).update({optim_class.qc_spec : obj_id})

