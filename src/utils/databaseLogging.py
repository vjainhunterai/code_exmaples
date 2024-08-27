from datetime import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime
from sqlalchemy.orm import sessionmaker
#import logging

# Enable SQLAlchemy logging
#logging.basicConfig()
#logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


class databaseHandler:
    def __init__(self, dbUrl):
        self.engine = create_engine(dbUrl)
        self.connection = self.engine.connect()
        self.metadata = MetaData()

        print("Creating tables if they do not exist...")
        self.logTable = Table('etl_log_final', self.metadata,
                              Column('id', Integer, primary_key=True, autoincrement=True),
                              Column('exec_id', String(19)),
                              Column('job_name', String(255)),
                              Column('file_name', String(500)),
                              Column('start_time', DateTime),
                              Column('end_time', DateTime),
                              Column('src_cnt', Integer),
                              Column('tgt_cnt', Integer),
                              Column('lkp_cnt', Integer),
                              Column('rej_cnt', Integer),
                              Column('job_status', String(255)),
                              Column('crt_dttm', DateTime, default=datetime.now))

        self.auditTable = Table('etl_audit_final', self.metadata,
                                Column('exec_id', String(10), primary_key=True),
                                Column('job_name', String(255)),
                                Column('system_id', String(255)),
                                Column('file_name', String(255)),
                                Column('file_date', DateTime),
                                Column('total_rec_cnt', Integer),
                                Column('processed_cnt', Integer),
                                Column('job_status', String(255)),
                                Column('rejection_cnt', Integer),
                                Column('rejection_rsn', String(255)),
                                Column('s3_last_modified_date', DateTime))

        self.metadata.create_all(self.engine)  # Create tables

        print("Initializing session...")
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def insertLogEntry(self, logEntry):
        try:
            print(f"Inserting log entry: {logEntry}")
            ins = self.logTable.insert().values(logEntry)
            self.session.execute(ins)
            self.session.commit()
            print("Log entry committed.")
        except Exception as e:
            print(f"Error inserting log record: {e}")
            self.session.rollback()

    def insertAuditEntry(self, auditEntry):
        try:
            print(f"Inserting audit entry: {auditEntry}")
            ins = self.auditTable.insert().values(auditEntry)
            self.session.execute(ins)
            self.session.commit()
            print("Audit entry committed.")
        except Exception as e:
            print(f"Error inserting audit record: {e}")
            self.session.rollback()

    #added audit update logic
    def updateAuditEntry(self, exec_id, updated_values):
        try:
            print(f"Updating audit entry for exec_id: {exec_id} with values: {updated_values}")
            upd = self.auditTable.update().where(self.auditTable.c.exec_id == exec_id).values(updated_values)
            self.session.execute(upd)
            self.session.commit()
            print("Audit entry updated.")
        except Exception as e:
            print(f"Error updating audit record: {e}")
            self.session.rollback()


    def close(self):
        print("Closing the session and connection...")
        self.session.close()
        self.connection.close()


class stepLogger:
    def __init__(self, dbHandler):
        self.dbHandler = dbHandler

    def logStep(self, execId, jobName, fileName, startTime, endTime, srcCnt, tgtCnt, lkpCnt, rejCnt, jobStatus):
        logEntry = {
            'exec_id': execId,
            'job_name': jobName,
            'file_name': fileName,
            'start_time': startTime,
            'end_time': endTime,
            'src_cnt': srcCnt,
            'tgt_cnt': tgtCnt,
            'lkp_cnt': lkpCnt,
            'rej_cnt': rejCnt,
            'job_status': jobStatus,
            'crt_dttm': datetime.now()
        }
        self.dbHandler.insertLogEntry(logEntry)


class auditLogger:
    def __init__(self, dbHandler):
        self.dbHandler = dbHandler

    def logAudit(self, auditEntry):
        self.dbHandler.insertAuditEntry(auditEntry)

    #added function for audit update
    def updateAudit(self, exec_id, updated_values):
        self.dbHandler.updateAuditEntry(exec_id, updated_values)