import os
import sys
from datetime import datetime, timedelta

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_collector import KRXDataCollector
from rsi_calculator import RSICalculator
from sector_leader_tracker import SectorLeaderTracker
from table_report_generator import TableReportGenerator
from utils.db_manager import (
    get_db_connection, 
    create_tables_if_not_exists,
    delete_old_stock_data,
    insert_stock_data,
    insert_sector_rsi,
    get_latest_sector_rsi
)
from utils.logger_util import LoggerUtil
from utils.telegram_util import TelegramUtil

class KRXReportService:
    def __init__(self):
        self.logger = LoggerUtil().get_logger()
        self.collector = KRXDataCollector()
        self.rsi_calculator = RSICalculator()
        self.leader_tracker = SectorLeaderTracker()
        self.table_generator = TableReportGenerator()
        self.telegram = TelegramUtil()
        self._last_rsi_date = None
        
    def initialize_database(self):
        """데이터베이스 초기화 및 테이블 생성"""
        try:
            conn = get_db_connection()
            if not conn:
                raise Exception("데이터베이스 연결 실패")
            
            create_tables_if_not_exists(conn)
            self.logger.info("데이터베이스 초기화 완료")
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"데이터베이스 초기화 오류: {e}")
            return False
    
    def collect_initial_data(self, days=100):
        """초기 데이터 수집 (과거 N일치 거래일만)"""
        try:
            self.logger.info(f"초기 데이터 수집 시작 - 최근 {days}일간 거래일")
            
            conn = get_db_connection()
            if not conn:
                raise Exception("데이터베이스 연결 실패")
            
            try:
                # 1단계: 과거 거래일들을 찾아서 리스트로 수집 (최신 -> 과거 순으로 찾기)
                trading_days_list = []
                current_date = datetime.now().date()
                
                # 충분한 기간을 확보하기 위해 days * 1.5 일정도 뒤로 가면서 거래일 찾기
                max_search_days = int(days * 1.5)
                search_count = 0
                
                while len(trading_days_list) < days and search_count < max_search_days:
                    search_count += 1
                    
                    # 거래일인지 확인
                    if self.collector.is_trading_day(current_date):
                        trading_days_list.append(current_date)
                    
                    current_date -= timedelta(days=1)
                
                # 2단계: 거래일 리스트를 과거순으로 정렬 (과거 -> 최신)
                trading_days_list.sort()
                
                # 3단계: 과거부터 최신 순으로 데이터 수집 및 저장
                trading_days_collected = 0
                total_inserted = 0
                
                for trade_date in trading_days_list:
                    date_str_yyyymmdd = trade_date.strftime('%Y%m%d')
                    date_str_sql = trade_date.strftime('%Y-%m-%d')
                    
                    # 이미 해당 날짜 데이터가 있는지 확인
                    if self._check_data_exists(conn, date_str_sql):
                        trading_days_collected += 1
                        continue
                    
                    
                    # 해당 날짜의 데이터 수집 (KOSPI + KOSDAQ)
                    day_data = []
                    for market in ['STK', 'KSQ']:
                        try:
                            market_data = self.collector.fetch_stock_data(date_str_yyyymmdd, market)
                            if market_data:
                                day_data.extend(market_data)
                        except Exception as e:
                            self.logger.warning(f"시장 데이터 수집 실패 - 날짜: {date_str_yyyymmdd}, 시장: {market}, 오류: {e}")
                            continue
                    
                    # 해당 날짜 데이터를 DB에 저장
                    if day_data:
                        try:
                            inserted_count = insert_stock_data(conn, day_data)
                            total_inserted += inserted_count
                            trading_days_collected += 1
                        except Exception as e:
                            self.logger.error(f"데이터 저장 실패 - 날짜: {date_str_yyyymmdd}, 오류: {e}")
                    else:
                        self.logger.warning(f"수집된 데이터 없음 - 날짜: {date_str_yyyymmdd}")
                        trading_days_collected += 1  # 거래일이지만 데이터가 없는 경우도 카운트
                
                self.logger.info(f"초기 데이터 수집 완료 - {trading_days_collected}개 거래일, 총 {total_inserted}개 레코드 저장")
                return trading_days_collected > 0
                
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"초기 데이터 수집 오류: {e}")
            return False
    
    def _check_data_exists(self, conn, date_str):
        """특정 날짜의 데이터가 이미 존재하는지 확인"""
        try:
            with conn.cursor() as cursor:
                sql = "SELECT COUNT(*) as count FROM krx_stock WHERE trade_date = %s"
                cursor.execute(sql, (date_str,))
                result = cursor.fetchone()
                return result['count'] > 0
        except Exception as e:
            self.logger.error(f"데이터 존재 여부 확인 오류: {e}")
            return False
    
    def daily_data_collection(self, target_date=None):
        """일일 데이터 수집 및 처리"""
        try:
            # 기준일 설정 (미지정시 오늘 날짜)
            if target_date is None:
                target_date = datetime.now().strftime('%Y%m%d')
            
            if isinstance(target_date, str) and len(target_date) == 8:
                # YYYYMMDD -> YYYY-MM-DD 변환
                formatted_date = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
            else:
                formatted_date = target_date
            
            self.logger.info(f"일일 데이터 수집 시작 - 기준일: {target_date}")
            
            conn = get_db_connection()
            if not conn:
                raise Exception("데이터베이스 연결 실패")
            
            try:
                # 1. 오래된 데이터 삭제 (매일 실행) - RSI 90 계산을 위해 데이터 보존
                delete_old_stock_data(conn, 365)
                
                # 2. 오늘 날짜 데이터가 이미 존재하는지 확인
                if self._check_data_exists(conn, formatted_date):
                    self.logger.info(f"오늘 날짜 데이터가 이미 존재합니다. 데이터 수집을 건너뛰고 다음 단계로 진행합니다.")
                    # RSI 계산은 진행
                    self._last_rsi_date = formatted_date
                else:
                    # 3. 오늘 데이터 수집 (KOSPI + KOSDAQ)
                    today_data = []
                    for market in ['STK', 'KSQ']:
                        market_data = self.collector.fetch_stock_data(target_date, market)
                        today_data.extend(market_data)
                    
                    if not today_data:
                        self.logger.warning(f"수집된 데이터가 없습니다 - 날짜: {target_date}")
                        return False
                    
                    # 4. 데이터베이스에 저장
                    insert_stock_data(conn, today_data)
                    self.logger.info(f"오늘 날짜 KRX 데이터 수집 완료 - 날짜: {target_date}")
                    self._last_rsi_date = formatted_date
                
                # 5. RSI 계산 및 저장 (KOSPI, KOSDAQ 별도 계산)
                all_sector_rsi = []
                for market_type, market_code in [('KOSPI', 'STK'), ('KOSDAQ', 'KSQ')]:
                    self.logger.info(f"{market_type} 시장의 섹터 RSI 계산 시작...")
                    sector_rsi_list = self.rsi_calculator.calculate_sector_rsi_batch(conn, formatted_date, market_type)
                    if sector_rsi_list:
                        all_sector_rsi.extend(sector_rsi_list)
                
                if all_sector_rsi:
                    insert_sector_rsi(conn, all_sector_rsi)
                
                # 6. 업종별 대장주 추적 업데이트
                try:
                    self.leader_tracker.update_sector_leaders(conn, formatted_date)
                except Exception as e:
                    self.logger.error(f"대장주 추적 업데이트 오류: {e}")
                    # 대장주 업데이트 실패해도 계속 진행
                
                return True
                
            finally:
                conn.close()
                
        except Exception as e:
            self.logger.error(f"일일 데이터 수집 오류: {e}")
            return False
    
    def generate_and_send_report(self, target_date=None):
        """테이블 리포트 생성 및 텔레그램 전송"""
        try:
            if target_date is None:
                prev_trading_day = self.collector.get_previous_trading_day()
                target_date = f"{prev_trading_day[:4]}-{prev_trading_day[4:6]}-{prev_trading_day[6:8]}"

            self.logger.info(f"테이블 리포트 생성 시작 - 기준일: {target_date}")

            conn = get_db_connection()
            if not conn:
                raise Exception("데이터베이스 연결 실패")

            try:
                image_paths = []
                captions = []
                market_types = ['KOSPI', 'KOSDAQ']
                rsi_summaries = {}
                leaders_datas = {}
                for market_type in market_types:
                    self.logger.info(f"{market_type} 리포트 생성 중...")

                    rsi_summary = self.rsi_calculator.get_rsi_summary(conn, target_date, market_type)
                    rsi_summaries[market_type] = rsi_summary

                    if not rsi_summary or not rsi_summary.get('total_sectors'):
                        self.logger.warning(f"{market_type} RSI 데이터가 없어 리포트를 생성할 수 없습니다.")
                        leaders_datas[market_type] = None
                        image_paths.append(None)
                        captions.append(None)
                        continue

                    leaders_data = self.leader_tracker.get_sector_leaders_with_streak(conn, target_date, market_type)
                    leaders_datas[market_type] = leaders_data

                    table_image_path = self.table_generator.create_sector_table_report(
                        rsi_summary, leaders_data, target_date, market_type
                    )

                    if table_image_path:
                        image_paths.append(table_image_path)
                        captions.append(f"{target_date} 섹터 RSI & 대장주 분석")
                    else:
                        self.logger.warning(f"{market_type} 테이블 리포트 생성 실패")
                        image_paths.append(None)
                        captions.append(None)

                if all(image_paths) and len(image_paths) == 2:
                    # 텔레그램 전송
                    self.telegram.send_multiple_photo(image_paths, captions[0])
                    # API 전송
                    try:
                        from utils.api_util import ApiUtil, ApiError
                        api_util = ApiUtil()
                        api_util.create_post(
                            title=captions[0],
                            content=f"{target_date} KRX 섹터 RSI & 대장주 분석",
                            category="섹터분석",
                            writer="admin",
                            image_paths=image_paths,
                            thumbnail_image_path=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'thumbnail', 'thumbnail_sector_top.png')
                        )
                    except ApiError as e:
                        error_message = f"❌ [krx-topsector-report] API 오류 발생\n\n{e.message}"
                        self.telegram.send_test_message(error_message)
                else:
                    for idx, market_type in enumerate(market_types):
                        if not image_paths[idx]:
                            self._send_fallback_text_report(
                                target_date,
                                rsi_summaries.get(market_type, {}),
                                leaders_datas.get(market_type, {}),
                                market_type
                            )
                return True

            finally:
                conn.close()

        except Exception as e:
            self.logger.error(f"테이블 리포트 생성 오류: {e}")
            return False
    
    def _send_fallback_text_report(self, target_date, rsi_summary, leaders_data, market_type):
        """이미지 리포트 실패시 폴백 텍스트 리포트 전송"""
        try:
            simple_message = f"🏢 [krx-topsector-report] {market_type} KRX 섹터 리포트\n📅 {target_date}\n📊 총 {rsi_summary.get('total_sectors', 0)}개 업종\n❌ 테이블 리포트 생성 실패"
            self.telegram.send_test_message(simple_message)
            return True
        except Exception as e:
            self.logger.error(f"{market_type} 폴백 텍스트 리포트 전송 실패: {e}")
            return False
    
    def run_daily_job(self):
        """일일 작업 실행"""
        self.logger.info("=== 일일 작업 시작 ===")
        
        # 1. 데이터 수집
        if self.daily_data_collection():
            # 2. 리포트 생성 및 전송 (RSI가 계산된 날짜 사용)
            report_date = getattr(self, '_last_rsi_date', None)
            if not self.generate_and_send_report(report_date):
                self.logger.error("리포트 전송 실패")
                self.telegram.send_test_message("❌ [krx-topsector-report] KRX 리포트 생성 실패")
        else:
            self.logger.error("일일 데이터 수집 실패")
            self.telegram.send_test_message("❌ [krx-topsector-report] KRX 데이터 수집 실패")
        
        self.logger.info("=== 일일 작업 완료 ===")

def main():
    """메인 실행 함수"""
    service = KRXReportService()
    
    # 데이터베이스 초기화
    if not service.initialize_database():
        print("데이터베이스 초기화 실패")
        return
    
    # 초기 데이터 수집 여부 확인
    if len(sys.argv) > 1 and sys.argv[1] == "--init":
        print("초기 데이터 수집을 시작합니다...")
        if service.collect_initial_data(200):
            print("초기 데이터 수집 완료")
        else:
            print("초기 데이터 수집 실패")
        return
    
    # 기본 실행 모드 (일일 작업 실행)
    print("KRX 데이터 수집 및 리포트 작업을 실행합니다...")
    service.run_daily_job()

if __name__ == "__main__":
    main()