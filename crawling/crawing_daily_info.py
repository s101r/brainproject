import logging
import requests
import sqlalchemy
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text, Column, String, Date, BigInteger
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import declarative_base, sessionmaker
import json
import io
import platform

class UpdateDailyInfo:
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
        LOG_PATH = f"C:/Users/Administrator/PycharmProjects/brain/log/daily_price_log_file_{datetime.today().strftime('%Y-%m-%d')}.log"
    else:
        LOG_PATH = f"/home/kiwon/brain/log/daily_price_log_file_{datetime.today().strftime('%Y-%m-%d')}.log"


    logging.basicConfig(filename=LOG_PATH,
                        level=logging.INFO, # Changed to INFO for better traceability
                        format="[ %(asctime)s | %(levelname)s ] %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")
    logger = logging.getLogger(__name__) # Use __name__ for logger name

    class DailyPrice(Base):
        __tablename__ = 'daily_price'
        code = Column(String(20), primary_key=True)
        date = Column(Date, primary_key=True)
        open = Column(BigInteger)
        high = Column(BigInteger)
        low = Column(BigInteger)
        close = Column(BigInteger)
        diff = Column(BigInteger)
        volume = Column(BigInteger)

    def __init__(self):
        # Create tables if they don't exist
        UpdateDailyInfo.Base.metadata.create_all(self.ENGINE)
        self.SessionLocal = sessionmaker(bind=self.ENGINE)
        self.company_codes = {} # Renamed 'codes' for clarity

    def _get_pages_to_fetch_config(self):
        """Loads or creates the pages_to_fetch configuration."""
        config_file_path = 'config.json'
        try:
            with open(config_file_path, 'r') as in_file:
                config = json.load(in_file)
                pages_to_fetch = config.get('pages_to_fetch', 100) # Default to 100 if not found
                self.logger.info(f"Loaded pages_to_fetch from config.json: {pages_to_fetch}")
        except FileNotFoundError:
            self.logger.warning(f"config.json not found. Creating with default pages_to_fetch=100.")
            pages_to_fetch = 100
            config = {'pages_to_fetch': pages_to_fetch}
            try:
                with open(config_file_path, 'w') as out_file:
                    json.dump(config, out_file, indent=4)
            except IOError as e:
                self.logger.error(f"Failed to write config.json: {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding config.json: {e}. Using default pages_to_fetch=100.")
            pages_to_fetch = 100
        return pages_to_fetch

    def _read_naver_stock_data(self, code, company_name, pages_to_fetch):
        """
        Reads stock price data from Naver Finance for a given code.
        Returns a pandas DataFrame or None on failure.
        """
        base_url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
        all_df = pd.DataFrame()

        try:
            # Fetch the first page to determine total pages
            response = requests.get(base_url, headers={'User-agent': 'Mozilla/5.0'})
            response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
            html = BeautifulSoup(response.text, "lxml")
            pgrr = html.find("td", class_="pgRR")

            last_page = 1
            if pgrr and pgrr.a and "href" in pgrr.a.attrs:
                last_page_href = str(pgrr.a["href"])
                try:
                    last_page = int(last_page_href.split('=')[-1])
                except ValueError:
                    self.logger.warning(f"Could not parse last page for {company_name} ({code}). Assuming 1.")
                    last_page = 1
            else:
                # If pgRR is not found, it might be a single page or an error page
                # We'll try to read the first page anyway
                self.logger.info(f"No 'pgRR' element found for {company_name} ({code}). Proceeding with first page only.")


            pages_to_process = min(last_page, pages_to_fetch)
            self.logger.info(f"Fetching {pages_to_process} pages for {company_name} ({code}). Total pages available: {last_page}")

            for page in range(1, pages_to_process + 1):
                pg_url = f'{base_url}&page={page}'
                page_response = requests.get(pg_url, headers={'User-agent': 'Mozilla/5.0'})
                page_response.raise_for_status()

                # pd.read_html can take a string directly
                page_df = pd.read_html(io.StringIO(page_response.text))[0]
                all_df = pd.concat([all_df, page_df], ignore_index=True)

            # Data cleaning and type conversion
            all_df = all_df.rename(columns={
                '날짜': 'date', '종가': 'close', '전일비': 'diff',
                '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'
            })
            all_df['date'] = pd.to_datetime(all_df['date'].str.replace('.', '-'), errors='coerce')
            all_df['diff'] = all_df['diff'].str.replace(',', '').str.extract(r'(\d+)').astype(float) # Extract digits and convert to float first
            all_df = all_df.dropna(subset=['date', 'close', 'diff', 'open', 'high', 'low', 'volume']) # Drop rows with NaNs after parsing

            # Convert to appropriate integer types, handle potential non-numeric values
            for col in ['close', 'diff', 'open', 'high', 'low', 'volume']:
                #all_df[col] = pd.to_numeric(all_df[col], errors='coerce').fillna(0).astype(BigInteger)
                all_df[col] = pd.to_numeric(all_df[col], errors='coerce').fillna(0).astype('int64')
            all_df = all_df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
            return all_df

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching data for {company_name} ({code}): {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error processing Naver data for {company_name} ({code}): {e}")
            return None

    def _upsert_daily_prices(self, daily_prices_df, company_code, company_name):
        """
        Performs a bulk upsert (insert or update) of daily prices into the database.
        """
        if daily_prices_df.empty:
            self.logger.warning(f"No daily price data to upsert for {company_name} ({company_code}).")
            return

        daily_price_objects = []
        for _, row in daily_prices_df.iterrows():
            daily_price_objects.append(
                UpdateDailyInfo.DailyPrice(
                    code=company_code,
                    date=row['date'].date(), # Store as date object
                    open=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    diff=row['diff'],
                    volume=row['volume']
                )
            )

        with self.SessionLocal() as session:
            try:
                # Iterate and merge for each object. This will perform an UPSERT (UPDATE if exists, INSERT if not)
                # For very large datasets, consider `bulk_insert_mappings` after checking for existence
                # or a more complex `ON DUPLICATE KEY UPDATE` approach if directly executing raw SQL.
                # However, for a few hundred records per company, `merge` is acceptable and safer.
                for obj in daily_price_objects:
                    session.merge(obj) # merge handles both inserts and updates based on primary key

                session.commit()
                self.logger.info(f"Successfully upserted {len(daily_prices_df)} daily prices for {company_name} ({company_code}).")
            except sqlalchemy.exc.SQLAlchemyError as e:
                session.rollback()
                self.logger.error(f"Database error during upsert for {company_name} ({company_code}): {e}")
            except Exception as e:
                session.rollback()
                self.logger.error(f"An unexpected error occurred during upsert for {company_name} ({company_code}): {e}")

    def update_all_daily_prices(self):
        """
        Main method to orchestrate fetching company codes and updating their daily prices.
        """
        self.logger.info("Starting daily price update process...")

        # 1. Load company codes
        try:
            # Using pd.read_sql is convenient for reading the whole table
            df_companies = pd.read_sql(text("SELECT code, name FROM company_info"), self.ENGINE) # Assuming 'name' is the company name column
            if df_companies.empty:
                self.logger.warning("No company information found in 'company_info' table. Aborting daily price update.")
                return

            self.company_codes = df_companies.set_index('code')['name'].to_dict()
            self.logger.info(f"Loaded {len(self.company_codes)} company codes from 'company_info'.")
        except sqlalchemy.exc.SQLAlchemyError as e:
            self.logger.error(f"Failed to load company info from DB: {e}")
            return
        except Exception as e:
            self.logger.error(f"An unexpected error occurred while loading company info: {e}")
            return

        # 2. Get pages to fetch configuration
        pages_to_fetch = self._get_pages_to_fetch_config()

        # 3. Update daily prices for each company
        for i, (code, company_name) in enumerate(self.company_codes.items()):
            self.logger.info(f"Processing ({i+1}/{len(self.company_codes)}): {company_name} ({code})")
            daily_df = self._read_naver_stock_data(code, company_name, pages_to_fetch)
            if daily_df is not None and not daily_df.empty:
                self._upsert_daily_prices(daily_df, code, company_name)
            else:
                self.logger.warning(f"Skipping daily price update for {company_name} ({code}) due to no data or error.")

        self.logger.info("Daily price update process completed.")


if __name__ == '__main__':
    updater = UpdateDailyInfo()
    updater.update_all_daily_prices()