from datetime import datetime, timedelta
from collections import defaultdict
from utils.logger_util import LoggerUtil
from utils.db_manager import get_db_connection, insert_sector_leaders

class SectorLeaderTracker:
    def __init__(self):
        self.logger = LoggerUtil().get_logger()
    
    def update_sector_leaders(self, conn, trade_date):
        """
        모든 시장의 업종별 대장주를 업데이트합니다.
        """
        total_updated_count = 0
        for market_type in ['KOSPI', 'KOSDAQ']:
            try:
                self.logger.info(f"{market_type} 시장 대장주 업데이트 시작...")
                current_leaders = self._get_current_top_stocks(conn, trade_date, market_type)
                
                if not current_leaders:
                    self.logger.warning(f"거래일 {trade_date}, 시장 {market_type}에 대장주 데이터가 없습니다.")
                    continue
                
                leaders_with_streak = []
                for industry, stocks in current_leaders.items():
                    for rank, stock in enumerate(stocks[:2], 1):
                        consecutive_days = self._calculate_consecutive_days(
                            conn, industry, rank, stock['stock_code'], market_type, trade_date
                        )
                        
                        leader_data = {
                            'industry': industry,
                            'rank_position': rank,
                            'stock_code': stock['stock_code'],
                            'stock_name': stock['stock_name'],
                            'market_cap': stock['market_cap'],
                            'consecutive_days': consecutive_days,
                            'market_type': market_type
                        }
                        leaders_with_streak.append(leader_data)
                
                if leaders_with_streak:
                    inserted_count = insert_sector_leaders(conn, leaders_with_streak)
                    total_updated_count += inserted_count
                    self.logger.info(f"{market_type} 업종별 대장주 업데이트 완료 - {len(current_leaders)}개 업종, {inserted_count}개 레코드")

            except Exception as e:
                self.logger.error(f"{market_type} 업종별 대장주 업데이트 오류: {e}")
                continue
        return total_updated_count

    def _get_current_top_stocks(self, conn, trade_date, market_type):
        """
        특정 날짜, 특정 시장의 업종별 시가총액 상위 종목들을 조회합니다.
        """
        try:
            with conn.cursor() as cursor:
                # 환경변수에서 제외할 섹터 목록 가져오기
                import os
                from dotenv import load_dotenv
                load_dotenv()
                
                excluded_sectors_str = os.getenv('EXCLUDED_SECTORS', '기타')
                excluded_sectors = [sector.strip() for sector in excluded_sectors_str.split(',') if sector.strip()]
                
                # 제외할 섹터들을 SQL에서 필터링
                excluded_placeholders = ', '.join(['%s'] * len(excluded_sectors))
                
                sql = f"""
                SELECT industry, stock_code, stock_name, market_cap, close_price, change_rate
                FROM krx_stock 
                WHERE trade_date = %s
                AND market_type = %s
                AND industry IS NOT NULL
                AND industry != ''
                AND industry NOT IN ({excluded_placeholders})
                AND market_cap IS NOT NULL
                ORDER BY industry, market_cap DESC
                """
                cursor.execute(sql, (trade_date, market_type) + tuple(excluded_sectors))
                all_stocks = cursor.fetchall()
            
            industry_stocks = defaultdict(list)
            for stock in all_stocks:
                industry_stocks[stock['industry']].append(stock)
            
            return dict(industry_stocks)
            
        except Exception as e:
            self.logger.error(f"업종별 상위 종목 조회 오류 ({market_type}): {e}")
            return {}

    def _calculate_consecutive_days(self, conn, industry, rank_position, stock_code, market_type, trade_date):
        """
        연속 유지 일수를 계산합니다.
        기존 기록과 비교하여 같은 종목이면 +1, 다른 종목이면 1로 리셋
        """
        try:
            with conn.cursor() as cursor:
                sql = """
                SELECT stock_code, consecutive_days, update_date
                FROM krx_sector_leaders 
                WHERE industry = %s 
                AND rank_position = %s
                AND market_type = %s
                """
                cursor.execute(sql, (industry, rank_position, market_type))
                current_record = cursor.fetchone()
                
                if current_record:
                    # 같은 종목이 계속 유지되는 경우
                    if current_record['stock_code'] == stock_code:
                        new_consecutive_days = current_record['consecutive_days'] + 1
                        self.logger.debug(f"{industry} {rank_position}위 {stock_code}: {current_record['consecutive_days']}일 -> {new_consecutive_days}일")
                        return new_consecutive_days
                    else:
                        # 다른 종목으로 변경된 경우 1일로 리셋
                        self.logger.info(f"{industry} {rank_position}위 변경: {current_record['stock_code']} -> {stock_code} (1일)")
                        return 1
                else:
                    # 새로운 업종/순위 조합인 경우
                    self.logger.info(f"{industry} {rank_position}위 신규 등록: {stock_code} (1일)")
                    return 1
                    
        except Exception as e:
            self.logger.error(f"연속 일수 계산 오류 (업종: {industry}, 순위: {rank_position}, 종목: {stock_code}): {e}")
            return 1

    def get_sector_leaders_with_streak(self, conn, trade_date, market_type):
        """
        연속성 정보가 포함된 업종별 대장주 데이터를 조회합니다.
        """
        try:
            with conn.cursor() as cursor:
                sql = """
                SELECT industry, rank_position, stock_code, stock_name,
                       market_cap, consecutive_days
                FROM krx_sector_leaders 
                WHERE update_date >= %s
                AND market_type = %s
                ORDER BY industry, rank_position
                """
                cursor.execute(sql, (trade_date, market_type))
                results = cursor.fetchall()
            
            sector_leaders = defaultdict(list)
            for row in results:
                sector_leaders[row['industry']].append({
                    'rank': row['rank_position'],
                    'stock_code': row['stock_code'],
                    'stock_name': row['stock_name'],
                    'market_cap': row['market_cap'],
                    'consecutive_days': row['consecutive_days']
                })
            
            self.logger.info(f"{market_type} 업종별 대장주 조회 완료 - {len(sector_leaders)}개 업종")
            return dict(sector_leaders)
            
        except Exception as e:
            self.logger.error(f"{market_type} 업종별 대장주 조회 오류: {e}")
            return {}

    def calculate_historical_consecutive_days(self, conn, market_type, industry, rank_position, current_stock_code, latest_date):
        """
        과거 데이터를 기반으로 특정 종목의 연속 유지 일수를 계산합니다.
        
        Args:
            conn: DB 연결 객체
            market_type: 시장 구분 (KOSPI/KOSDAQ)
            industry: 업종명
            rank_position: 순위 (1 또는 2)
            current_stock_code: 현재 해당 순위의 종목코드
            latest_date: 계산 기준일 (최신 거래일)
        
        Returns:
            int: 연속 유지 일수
        """
        try:
            with conn.cursor() as cursor:
                # 과거 거래일들을 최신부터 역순으로 조회
                sql = """
                SELECT DISTINCT trade_date 
                FROM krx_stock 
                WHERE trade_date <= %s
                ORDER BY trade_date DESC
                LIMIT 100
                """
                cursor.execute(sql, (latest_date,))
                trading_dates = [row['trade_date'] for row in cursor.fetchall()]
                
                if not trading_dates:
                    return 1
                
                consecutive_days = 0
                
                # 최신 거래일부터 역순으로 확인
                for trade_date in trading_dates:
                    # 해당 날짜의 업종별 시가총액 순위 조회 (MySQL 호환성을 위해 ORDER BY만 사용)
                    rank_sql = """
                    SELECT stock_code, stock_name, market_cap
                    FROM krx_stock 
                    WHERE trade_date = %s
                    AND market_type = %s
                    AND industry = %s
                    AND market_cap IS NOT NULL
                    ORDER BY market_cap DESC
                    LIMIT 5
                    """
                    cursor.execute(rank_sql, (trade_date, market_type, industry))
                    ranked_stocks = cursor.fetchall()
                    
                    if not ranked_stocks:
                        # 해당 날짜에 데이터가 없으면 중단
                        break
                    
                    # 현재 순위에 해당하는 종목 찾기 (1위=인덱스0, 2위=인덱스1)
                    target_stock = None
                    if len(ranked_stocks) >= rank_position:
                        target_stock = ranked_stocks[rank_position - 1]  # 1-based to 0-based index
                    
                    if target_stock and target_stock['stock_code'] == current_stock_code:
                        # 같은 종목이면 연속일수 증가
                        consecutive_days += 1
                        self.logger.debug(f"{trade_date}: {industry} {rank_position}위 {current_stock_code} 연속 ({consecutive_days}일)")
                    else:
                        # 다른 종목이거나 데이터가 없으면 중단
                        if target_stock:
                            self.logger.debug(f"{trade_date}: {industry} {rank_position}위 변경 {target_stock['stock_code']} != {current_stock_code}")
                        else:
                            self.logger.debug(f"{trade_date}: {industry} {rank_position}위 데이터 없음")
                        break
                
                return max(consecutive_days, 1)  # 최소 1일
                
        except Exception as e:
            self.logger.error(f"과거 연속일수 계산 오류 ({market_type} {industry} {rank_position}위 {current_stock_code}): {e}")
            return 1

    def recalculate_all_consecutive_days(self, conn, latest_date=None):
        """
        모든 krx_sector_leaders 레코드의 연속일수를 과거 데이터 기반으로 재계산하여 업데이트합니다.
        
        Args:
            conn: DB 연결 객체
            latest_date: 계산 기준일 (None이면 최신 거래일 사용)
        
        Returns:
            int: 업데이트된 레코드 수
        """
        try:
            # 기준일 설정
            if latest_date is None:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT MAX(trade_date) as max_date FROM krx_stock")
                    result = cursor.fetchone()
                    latest_date = result['max_date']
            
            self.logger.info(f"연속일수 재계산 시작 - 기준일: {latest_date}")
            
            # 모든 krx_sector_leaders 레코드 조회
            with conn.cursor() as cursor:
                sql = """
                SELECT market_type, industry, rank_position, stock_code, stock_name
                FROM krx_sector_leaders
                ORDER BY market_type, industry, rank_position
                """
                cursor.execute(sql)
                all_leaders = cursor.fetchall()
            
            updated_count = 0
            total_count = len(all_leaders)
            
            for i, leader in enumerate(all_leaders, 1):
                try:
                    market_type = leader['market_type']
                    industry = leader['industry']
                    rank_position = leader['rank_position']
                    stock_code = leader['stock_code']
                    stock_name = leader['stock_name']
                    
                    self.logger.info(f"진행률: {i}/{total_count} - {market_type} {industry} {rank_position}위 {stock_name}({stock_code})")
                    
                    # 과거 데이터 기반으로 연속일수 계산
                    consecutive_days = self.calculate_historical_consecutive_days(
                        conn, market_type, industry, rank_position, stock_code, latest_date
                    )
                    
                    # 계산된 연속일수로 업데이트
                    with conn.cursor() as update_cursor:
                        update_sql = """
                        UPDATE krx_sector_leaders 
                        SET consecutive_days = %s, update_date = CURRENT_TIMESTAMP
                        WHERE market_type = %s AND industry = %s AND rank_position = %s
                        """
                        update_cursor.execute(update_sql, (consecutive_days, market_type, industry, rank_position))
                        
                        if update_cursor.rowcount > 0:
                            updated_count += 1
                            self.logger.info(f"업데이트 완료: {market_type} {industry} {rank_position}위 {stock_name} -> {consecutive_days}일")
                        
                except Exception as e:
                    self.logger.error(f"개별 레코드 업데이트 실패 ({leader}): {e}")
                    continue
            
            conn.commit()
            self.logger.info(f"연속일수 재계산 완료 - 총 {updated_count}/{total_count}개 레코드 업데이트")
            return updated_count
            
        except Exception as e:
            self.logger.error(f"연속일수 재계산 오류: {e}")
            conn.rollback()
            return 0

# 테스트용 실행 코드
if __name__ == "__main__":
    tracker = SectorLeaderTracker()
    
    # DB 연결 테스트
    conn = get_db_connection()
    if conn:
        try:
            # 최신 거래일 기준으로 대장주 업데이트 테스트
            from data_collector import KRXDataCollector
            collector = KRXDataCollector()
            prev_trading_day = collector.get_previous_trading_day()
            formatted_date = f"{prev_trading_day[:4]}-{prev_trading_day[4:6]}-{prev_trading_day[6:8]}"
            
            print(f"테스트 대상 거래일: {formatted_date}")
            
            # 대장주 업데이트
            updated_count = tracker.update_sector_leaders(conn, formatted_date)
            print(f"업데이트된 대장주 레코드 수: {updated_count}")
            
            # 대장주 조회
            leaders = tracker.get_sector_leaders_with_streak(conn, formatted_date, 'KOSPI')
            print(f"조회된 업종 수: {len(leaders)}")
            
            # 샘플 출력
            for industry, stocks in list(leaders.items())[:3]:
                print(f"\n업종: {industry}")
                for stock in stocks:
                    print(f"  {stock['rank']}위: {stock['stock_name']} ({stock['consecutive_days']}일 연속)")
                    
        finally:
            conn.close()
    else:
        print("DB 연결 실패")