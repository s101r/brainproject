import sqlalchemy
from sqlalchemy import create_engine, BIGINT, Float, text, Column, Integer, String, Date, delete
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import logging
import platform
import FinanceDataReader as fdr
from sqlalchemy.dialects import mysql
# Import for specific dialect types if needed, though not strictly used in this example.


class UpdateCompany:
    # Database connection configuration
    DATABASE_URL = "mysql+pymysql://stockmanager:oxford@192.168.0.3:3306/brain?charset=utf8mb4"
    ENGINE = create_engine(DATABASE_URL,
                           pool_size=50,
                           max_overflow=50,
                           pool_recycle=3600)
    Base = declarative_base()

    # Logger configuration
    LOG_PATH = ''
    if platform.system() == 'Windows':
        LOG_PATH = f"C:/Users/Administrator/PycharmProjects/brain/log/company_log_file_{datetime.today().strftime('%Y-%m-%d')}.txt"
    else:
        LOG_PATH = f"/home/kiwon/brain/log/company_log_file_{datetime.today().strftime('%Y-%m-%d')}.txt"

    logging.basicConfig(filename=LOG_PATH,
                        level=logging.INFO,  # Changed to INFO for more detailed operational logging
                        format="[ %(asctime)s | %(levelname)s ] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger(__name__)  # Use __name__ for logger name

    class CompanyInfo(Base):
        __tablename__ = 'company_info'
        code = Column(String(20), primary_key=True)
        isu_cd = Column(String(30))
        name = Column(String(50))
        market = Column(String(20))
        dept = Column(String(30))
        close = Column(BIGINT)
        changecode = Column(String(5))
        changes = Column(Integer)
        chagesratio = Column(Float)
        open = Column(BIGINT)
        high = Column(BIGINT)
        low = Column(BIGINT)
        volume = Column(BIGINT)
        amount = Column(BIGINT)
        marcap = Column(BIGINT)
        stocks = Column(BIGINT)
        marketid = Column(String(10))
        last_update = Column(Date)

    def __init__(self):
        # Create tables if they don't exist
        UpdateCompany.Base.metadata.create_all(self.ENGINE)
        self.SessionLocal = sessionmaker(bind=self.ENGINE)

    def _get_max_last_update_date(self):
        """Fetches the maximum last_update date from the company_info table."""
        with self.ENGINE.connect() as connection:
            result = connection.execute(text("SELECT MAX(last_update) FROM company_info")).fetchone()
            return result[0]

    def _get_existing_company_codes(self, session):
        """Fetches all existing company codes from the database."""
        result = session.query(UpdateCompany.CompanyInfo.code).all()
        return {code for (code,) in result}

    def read_finance_reader(self):
        """Reads stock listing data from FinanceDataReader."""
        try:
            df_krx = fdr.StockListing('KRX')
            self.logger.info("Successfully fetched stock listing from FinanceDataReader.")
            return df_krx
        except Exception as e:
            self.logger.error(f"Failed to fetch stock listing from FinanceDataReader: {e}")
            return pd.DataFrame()  # Return empty DataFrame on error

    def update_comp(self):
        """
        Updates the company information in the database.
        It fetches the latest KRX stock listing, determines which records to update/insert,
        and performs bulk operations for efficiency.
        """
        max_last_update_db = self._get_max_last_update_date()
        today = datetime.today().date()  # Get today's date

        if max_last_update_db and max_last_update_db >= today:
            self.logger.info(f"Company info is already up-to-date as of {today}. No update needed.")
            return

        self.logger.info("Starting company information update...")
        krx_data = self.read_finance_reader()

        if krx_data.empty:
            self.logger.warning("No KRX data to process. Aborting update.")
            return

        company_data_to_upsert = []  # List to hold objects for bulk upsert
        updated_count = 0
        inserted_count = 0

        with self.SessionLocal() as session:
            existing_codes = self._get_existing_company_codes(session)
            self.logger.info(f"Found {len(existing_codes)} existing company codes in the database.")

            for _, row in krx_data.iterrows():  # Iterate using iterrows for better readability
                company_code = row['Code']

                # Create a CompanyInfo object
                company_info_obj = UpdateCompany.CompanyInfo(
                    code=company_code,
                    name=row['Name'],
                    isu_cd=row['ISU_CD'],
                    market=row['Market'],
                    dept=row['Dept'],
                    close=row['Close'],
                    changecode=row['ChangeCode'],
                    changes=row['Changes'],
                    chagesratio=row['ChagesRatio'],
                    open=row['Open'],
                    high=row['High'],
                    low=row['Low'],
                    volume=row['Volume'],
                    amount=row['Amount'],
                    marcap=row['Marcap'],
                    stocks=row['Stocks'],
                    marketid=row['MarketId'],
                    last_update=today
                )
                company_data_to_upsert.append(company_info_obj)

            try:
                # Use bulk_insert_mappings for better performance if just inserting,
                # or merge for upsert operations which is more fitting here.
                # Since `merge` handles both inserts and updates based on primary key,
                # it's the most appropriate here.
                for company_obj in company_data_to_upsert:
                    if company_obj.code in existing_codes:
                        session.merge(company_obj)  # Updates if exists, inserts if not
                        updated_count += 1
                    else:
                        session.add(company_obj)
                        inserted_count += 1

                session.commit()
                self.logger.info(
                    f"Company information update complete: {inserted_count} new companies inserted, {updated_count} existing companies updated.")
            except sqlalchemy.exc.SQLAlchemyError as e:
                session.rollback()
                self.logger.error(f"Database error during company info update: {e}")
            except Exception as e:
                session.rollback()
                self.logger.error(f"An unexpected error occurred during company info update: {e}")


# Entry point for the script
if __name__ == '__main__':
    updater = UpdateCompany()
    updater.update_comp()