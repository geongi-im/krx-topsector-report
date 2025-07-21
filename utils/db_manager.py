import pymysql
import os
from dotenv import load_dotenv
from utils.logger_util import LoggerUtil

# 로거 설정
logger = LoggerUtil().get_logger()

# .env 파일에서 환경 변수 로드
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT", 3306))

def get_db_connection():
    """DB 연결을 생성하고 반환합니다."""
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            port=DB_PORT,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info("DB에 성공적으로 연결되었습니다.")
        return conn
    except pymysql.MySQLError as e:
        logger.error(f"DB 연결 오류: {e}")
        return None
# 테이블 생성 SQL
CREATE_KRX_STOCK_TABLE = """
CREATE TABLE IF NOT EXISTS krx_stock (
    idx INT AUTO_INCREMENT PRIMARY KEY COMMENT '내부 고유 ID (Auto Increment)',
    stock_code VARCHAR(10) NOT NULL COMMENT '종목코드 (예: 005930)',
    stock_name VARCHAR(100) NOT NULL COMMENT '종목명 (예: 삼성전자)',
    market_type VARCHAR(10) NOT NULL COMMENT '시장구분 (예: KOSPI, KOSDAQ)',
    industry VARCHAR(100) COMMENT '업종명 (예: 반도체, 화학)',
    trade_date DATE NOT NULL COMMENT '거래일 (yyyy-mm-dd)',
    close_price FLOAT NOT NULL COMMENT '종가',
    change_amount FLOAT COMMENT '대비 (전일 대비 금액)',
    change_rate FLOAT COMMENT '등락률 (%)',
    market_cap BIGINT COMMENT '시가총액',
    reg_date DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '등록일시 (yyyy-mm-dd hh:mm:ss)',
    
    UNIQUE KEY uq_stock_trade_date (stock_code, trade_date),
    KEY idx_stock_code (stock_code),
    KEY idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='KRX 일별 주가 데이터';
"""

CREATE_KRX_SECTOR_RSI_TABLE = """
CREATE TABLE IF NOT EXISTS krx_sector_rsi (
    idx INT AUTO_INCREMENT PRIMARY KEY COMMENT '내부 고유 ID (Auto Increment)',
    trade_date DATE NOT NULL COMMENT 'RSI 계산 기준일',
    market_type VARCHAR(10) NOT NULL COMMENT '시장구분 (KOSPI, KOSDAQ)',
    industry VARCHAR(100) NOT NULL COMMENT '업종명',
    rsi_d FLOAT COMMENT '일간 RSI',
    rsi_w FLOAT COMMENT '주간 RSI',
    rsi_m FLOAT COMMENT '월간 RSI',
    reg_date DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '등록일시',
    UNIQUE KEY uq_trade_date_market_industry (trade_date, market_type, industry),
    KEY idx_trade_date (trade_date),
    KEY idx_industry (industry)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='업종별 RSI 일일 요약';
"""

CREATE_KRX_SECTOR_LEADERS_TABLE = """
CREATE TABLE IF NOT EXISTS krx_sector_leaders (
    idx INT AUTO_INCREMENT PRIMARY KEY COMMENT '내부 고유 ID (Auto Increment)',
    market_type VARCHAR(10) NOT NULL COMMENT '시장구분 (KOSPI, KOSDAQ)',
    industry VARCHAR(100) NOT NULL COMMENT '업종명',
    rank_position TINYINT NOT NULL COMMENT '1위 또는 2위',
    stock_code VARCHAR(10) NOT NULL COMMENT '종목코드',
    stock_name VARCHAR(100) NOT NULL COMMENT '종목명',
    market_cap BIGINT NOT NULL COMMENT '시가총액',
    consecutive_days INT DEFAULT 1 COMMENT '연속 유지 일수',
    reg_date DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '최초 등록일시',
    update_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '최종 업데이트일시',
    
    UNIQUE KEY uq_market_industry_rank (market_type, industry, rank_position),
    KEY idx_market_industry_rank (market_type, industry, rank_position),
    KEY idx_stock_code (stock_code),
    KEY idx_update_date (update_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='업종별 대장주 추적';
"""

def create_tables_if_not_exists(conn):
    """필요한 테이블이 없으면 생성합니다."""
    with conn.cursor() as cursor:
        try:
            cursor.execute(CREATE_KRX_STOCK_TABLE)
            logger.info("'krx_stock' 테이블이 준비되었습니다.")

            # krx_sector_rsi 테이블 재생성
            try:
                cursor.execute("DROP TABLE IF EXISTS krx_sector_rsi")
                logger.info("기존 'krx_sector_rsi' 테이블을 삭제했습니다.")
            except pymysql.MySQLError as e:
                logger.warning(f"'krx_sector_rsi' 테이블 삭제 실패 (무시): {e}")
            cursor.execute(CREATE_KRX_SECTOR_RSI_TABLE)
            logger.info("'krx_sector_rsi' 테이블이 새로운 구조로 생성되었습니다.")

            # krx_sector_leaders 테이블 생성
            cursor.execute(CREATE_KRX_SECTOR_LEADERS_TABLE)
            logger.info("'krx_sector_leaders' 테이블이 준비되었습니다.")
            
            conn.commit()
        except pymysql.MySQLError as e:
            logger.error(f"테이블 생성 오류: {e}")
            conn.rollback()
            raise

def delete_old_stock_data(conn, days=100):
    """지정된 일수보다 오래된 krx_stock 데이터를 삭제합니다."""
    with conn.cursor() as cursor:
        try:
            sql = "DELETE FROM krx_stock WHERE trade_date < DATE_SUB(CURDATE(), INTERVAL %s DAY)"
            cursor.execute(sql, (days,))
            deleted_count = cursor.rowcount
            conn.commit()
            return deleted_count
        except pymysql.MySQLError as e:
            logger.error(f"오래된 데이터 삭제 오류: {e}")
            conn.rollback()
            raise

def insert_stock_data(conn, stock_data_list):
    """krx_stock 테이블에 주식 데이터를 일괄 삽입합니다."""
    if not stock_data_list:
        return 0
    
    with conn.cursor() as cursor:
        try:
            sql = """
            INSERT INTO krx_stock 
            (stock_code, stock_name, market_type, industry, trade_date, close_price, 
             change_amount, change_rate, market_cap)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            close_price = VALUES(close_price),
            change_amount = VALUES(change_amount),
            change_rate = VALUES(change_rate),
            market_cap = VALUES(market_cap),
            reg_date = CURRENT_TIMESTAMP
            """
            
            values_to_insert = []
            for stock_data in stock_data_list:
                values_to_insert.append((
                    stock_data['stock_code'],
                    stock_data['stock_name'],
                    stock_data['market_type'],
                    stock_data['industry'],
                    stock_data['trade_date'],
                    stock_data['close_price'],
                    stock_data.get('change_amount'),
                    stock_data.get('change_rate'),
                    stock_data.get('market_cap')
                ))
            
            cursor.executemany(sql, values_to_insert)
            inserted_count = cursor.rowcount
            conn.commit()
            return inserted_count
        except pymysql.MySQLError as e:
            logger.error(f"주식 데이터 삽입 오류: {e}")
            conn.rollback()
            raise

def get_stock_data_for_rsi(conn, stock_code, days=30):
    """특정 종목의 RSI 계산을 위한 과거 데이터를 조회합니다."""
    with conn.cursor() as cursor:
        try:
            sql = """
            SELECT trade_date, close_price 
            FROM krx_stock 
            WHERE stock_code = %s 
            ORDER BY trade_date DESC 
            LIMIT %s
            """
            cursor.execute(sql, (stock_code, days))
            result = cursor.fetchall()
            return result
        except pymysql.MySQLError as e:
            logger.error(f"RSI 계산용 데이터 조회 오류 (종목: {stock_code}): {e}")
            raise

def insert_sector_rsi(conn, sector_rsi_list):
    """krx_sector_rsi 테이블에 업종별 RSI 데이터를 삽입합니다."""
    if not sector_rsi_list:
        return 0
    
    with conn.cursor() as cursor:
        try:
            sql = """
            INSERT INTO krx_sector_rsi 
            (trade_date, market_type, industry, rsi_d, rsi_w, rsi_m)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            rsi_d = VALUES(rsi_d),
            rsi_w = VALUES(rsi_w),
            rsi_m = VALUES(rsi_m),
            reg_date = CURRENT_TIMESTAMP
            """
            
            values_to_insert = []
            for rsi_data in sector_rsi_list:
                values_to_insert.append((
                    rsi_data['trade_date'],
                    rsi_data['market_type'],
                    rsi_data['industry'],
                    rsi_data.get('rsi_d'),
                    rsi_data.get('rsi_w'),
                    rsi_data.get('rsi_m')
                ))
            
            cursor.executemany(sql, values_to_insert)
            inserted_count = cursor.rowcount
            conn.commit()
            return inserted_count
        except pymysql.MySQLError as e:
            logger.error(f"섹터 RSI 데이터 삽입 오류: {e}")
            conn.rollback()
            raise

def get_latest_sector_rsi(conn, trade_date=None):
    """최신 섹터 RSI 데이터를 조회합니다."""
    with conn.cursor() as cursor:
        try:
            if trade_date:
                sql = """
                SELECT industry, rsi_d, rsi_w, rsi_m, market_type
                FROM krx_sector_rsi 
                WHERE trade_date = %s
                ORDER BY industry
                """
                cursor.execute(sql, (trade_date,))
            else:
                sql = """
                SELECT industry, rsi_d, rsi_w, rsi_m, market_type
                FROM krx_sector_rsi 
                WHERE trade_date = (SELECT MAX(trade_date) FROM krx_sector_rsi)
                ORDER BY industry
                """
                cursor.execute(sql)
            
            result = cursor.fetchall()
            return result
        except pymysql.MySQLError as e:
            logger.error(f"섹터 RSI 데이터 조회 오류: {e}")
            raise

def insert_sector_leaders(conn, sector_leaders_list):
    """krx_sector_leaders 테이블에 업종별 대장주 데이터를 삽입합니다."""
    if not sector_leaders_list:
        return 0
    
    with conn.cursor() as cursor:
        try:
            sql = """
            INSERT INTO krx_sector_leaders 
            (market_type, industry, rank_position, stock_code, stock_name, market_cap, consecutive_days)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            stock_code = VALUES(stock_code),
            stock_name = VALUES(stock_name),
            market_cap = VALUES(market_cap),
            consecutive_days = VALUES(consecutive_days),
            update_date = CURRENT_TIMESTAMP
            """
            
            values_to_insert = []
            for leader_data in sector_leaders_list:
                values_to_insert.append((
                    leader_data['market_type'],
                    leader_data['industry'],
                    leader_data['rank_position'],
                    leader_data['stock_code'],
                    leader_data['stock_name'],
                    leader_data['market_cap'],
                    leader_data.get('consecutive_days', 1)
                ))
            
            cursor.executemany(sql, values_to_insert)
            inserted_count = cursor.rowcount
            conn.commit()
            return inserted_count
        except pymysql.MySQLError as e:
            logger.error(f"섹터 대장주 데이터 삽입 오류: {e}")
            conn.rollback()
            raise

def get_sector_leaders(conn):
    """섹터별 대장주 데이터를 조회합니다. (trade_date 파라미터 제거 - 항상 최신 데이터)"""
    with conn.cursor() as cursor:
        try:
            sql = """
            SELECT industry, rank_position, stock_code, stock_name,
                   market_cap, consecutive_days, reg_date, update_date
            FROM krx_sector_leaders 
            ORDER BY industry, rank_position
            """
            cursor.execute(sql)
            
            result = cursor.fetchall()
            return result
        except pymysql.MySQLError as e:
            logger.error(f"섹터 대장주 데이터 조회 오류: {e}")
            raise

# 이 파일이 직접 실행될 때 테이블 생성 로직을 실행 (테스트용)
if __name__ == '__main__':
    db_conn = get_db_connection()
    if db_conn:
        try:
            # 모듈 테스트
            print("DB 환경변수:")
            print(f"- DB_HOST: {DB_HOST}")
            print(f"- DB_PORT: {DB_PORT}")
            print(f"- DB_USER: {DB_USER}")
            print(f"- DB_NAME: {DB_NAME}")
            
            # 테이블 생성 테스트
            create_tables_if_not_exists(db_conn)
            print("테이블 생성 테스트 완료")
        finally:
            db_conn.close()
            print("DB 연결이 종료되었습니다.")
    else:
        print("DB 연결에 실패하여 테이블 생성 및 테스트를 진행할 수 없습니다.") 