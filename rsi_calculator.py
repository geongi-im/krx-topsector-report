import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from utils.logger_util import LoggerUtil
from utils.db_manager import get_db_connection

load_dotenv()

class RSICalculator:
    def __init__(self):
        self.logger = LoggerUtil().get_logger()
    
    def calculate_rsi(self, prices, period=14):
        """
        RSI를 계산합니다. (표준 RSI: 첫 번째는 SMA, 이후는 EMA)
        
        Args:
            prices (list): 종가 리스트 (시간 순서대로 정렬)
            period (int): RSI 계산 기간 (기본 14일)
        
        Returns:
            float: RSI 값 (0~100)
        """
        if len(prices) < period + 1:
            return None
        
        # pandas Series로 변환
        price_series = pd.Series(prices)
        
        # 가격 변화량 계산
        delta = price_series.diff()
        
        # 상승과 하락 분리
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # 첫 번째 평균은 SMA로 계산
        first_avg_gain = gain.iloc[1:period+1].mean()
        first_avg_loss = loss.iloc[1:period+1].mean()
        
        # EMA 계산을 위한 초기값 설정
        avg_gains = [first_avg_gain]
        avg_losses = [first_avg_loss]
        
        # EMA 승수 (smoothing factor)
        alpha = 1.0 / period
        
        # 두 번째 값부터는 EMA로 계산
        for i in range(period + 1, len(price_series)):
            current_gain = gain.iloc[i]
            current_loss = loss.iloc[i]
            
            # EMA 계산: EMA = α × current_value + (1-α) × previous_EMA
            new_avg_gain = alpha * current_gain + (1 - alpha) * avg_gains[-1]
            new_avg_loss = alpha * current_loss + (1 - alpha) * avg_losses[-1]
            
            avg_gains.append(new_avg_gain)
            avg_losses.append(new_avg_loss)
        
        # 최종 평균값들
        final_avg_gain = avg_gains[-1]
        final_avg_loss = avg_losses[-1]
        
        # 0으로 나누기 방지
        if final_avg_loss == 0:
            return 100.0 if final_avg_gain > 0 else 50.0
        
        # RS 계산
        rs = final_avg_gain / final_avg_loss
        
        # RSI 계산
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def calculate_stock_rsi(self, conn, stock_code, trade_date, rsi_periods=None):
        """
        특정 종목의 RSI를 계산합니다.
        
        Args:
            conn: DB 연결 객체
            stock_code (str): 종목코드
            trade_date (str): 기준일 (YYYY-MM-DD)
            rsi_periods (dict): RSI 계산 기간 {'d': 14, 'w': 30, 'm': 90}
        
        Returns:
            dict: RSI 값들 {'rsi_d': float, 'rsi_w': float, 'rsi_m': float}
        """
        if rsi_periods is None:
            rsi_periods = {'d': 14, 'w': 30, 'm': 90}  # 90일 RSI 계산
        
        try:
            with conn.cursor() as cursor:
                # 기준일 이전 최대 120일 데이터 조회 (RSI 계산에 충분한 데이터 확보)
                sql = """
                SELECT close_price, trade_date
                FROM krx_stock 
                WHERE stock_code = %s 
                AND trade_date <= %s
                ORDER BY trade_date DESC 
                LIMIT 120
                """
                cursor.execute(sql, (stock_code, trade_date))
                result = cursor.fetchall()
                
                if not result:
                    self.logger.debug(f"RSI 계산용 데이터 없음 - 종목: {stock_code}, 기준일: {trade_date}")
                    return {'rsi_d': None, 'rsi_w': None, 'rsi_m': None}
                
                # 시간 순서대로 정렬 (과거 -> 현재)
                prices = [row['close_price'] for row in reversed(result) if row['close_price'] is not None]
                
                if len(prices) < max(rsi_periods.values()) + 1:
                    self.logger.debug(f"RSI 계산용 데이터 부족 - 종목: {stock_code}, 데이터 개수: {len(prices)}, 필요: {max(rsi_periods.values()) + 1}")
                    return {'rsi_d': None, 'rsi_w': None, 'rsi_m': None}
                
                # 각 기간별 RSI 계산
                rsi_values = {}
                for period_name, period_days in rsi_periods.items():
                    if len(prices) >= period_days + 1:
                        rsi_value = self.calculate_rsi(prices, period_days)
                        rsi_values[f'rsi_{period_name}'] = rsi_value
                    else:
                        rsi_values[f'rsi_{period_name}'] = None
                
                return rsi_values
                
        except Exception as e:
            self.logger.error(f"RSI 계산 오류 (종목: {stock_code}): {e}")
            return {'rsi_d': None, 'rsi_w': None, 'rsi_m': None}
    
    def calculate_sector_rsi_batch(self, conn, trade_date, market_type, rsi_periods=None):
        """
        특정 시장의 모든 업종의 RSI를 일괄 계산합니다.
        
        Args:
            conn: DB 연결 객체
            trade_date (str): 기준일 (YYYY-MM-DD)
            market_type (str): 시장 구분 ('KOSPI' 또는 'KOSDAQ')
            rsi_periods (dict): RSI 계산 기간
        
        Returns:
            list: 업종별 RSI 데이터 리스트
        """
        if rsi_periods is None:
            rsi_periods = {'d': 14, 'w': 30, 'm': 90}
        
        try:
            with conn.cursor() as cursor:
                sql = """
                SELECT DISTINCT industry, stock_code
                FROM krx_stock 
                WHERE trade_date = %s
                AND market_type = %s
                AND industry IS NOT NULL
                AND industry != ''
                ORDER BY industry, stock_code
                """
                cursor.execute(sql, (trade_date, market_type))
                stocks_by_industry = cursor.fetchall()
            
            if not stocks_by_industry:
                self.logger.warning(f"기준일({trade_date})에 {market_type} 업종 데이터가 없습니다.")
                return []
            
            industry_stocks = defaultdict(list)
            for row in stocks_by_industry:
                industry_stocks[row['industry']].append(row['stock_code'])
            
            self.logger.info(f"{market_type} 업종별 RSI 계산 시작 - {len(industry_stocks)}개 업종, 기준일: {trade_date}")
            
            sector_rsi_list = []
            
            for industry, stock_codes in industry_stocks.items():
                try:
                    industry_rsi_values = {
                        'rsi_d': [],
                        'rsi_w': [],
                        'rsi_m': []
                    }
                    
                    valid_stocks = 0
                    failed_stocks = 0
                    
                    for stock_code in stock_codes:
                        try:
                            stock_rsi = self.calculate_stock_rsi(conn, stock_code, trade_date, rsi_periods)
                            
                            has_valid_rsi = False
                            for period_key in industry_rsi_values.keys():
                                if stock_rsi[period_key] is not None:
                                    industry_rsi_values[period_key].append(stock_rsi[period_key])
                                    has_valid_rsi = True
                            
                            if has_valid_rsi:
                                valid_stocks += 1
                            else:
                                failed_stocks += 1
                                
                        except Exception as e:
                            self.logger.debug(f"개별 종목 RSI 계산 실패 - {stock_code}: {e}")
                            failed_stocks += 1
                            continue
                    
                    sector_rsi = {
                        'trade_date': trade_date,
                        'market_type': market_type,
                        'industry': industry,
                        'rsi_d': np.mean(industry_rsi_values['rsi_d']) if len(industry_rsi_values['rsi_d']) > 0 else None,
                        'rsi_w': np.mean(industry_rsi_values['rsi_w']) if len(industry_rsi_values['rsi_w']) > 0 else None,
                        'rsi_m': np.mean(industry_rsi_values['rsi_m']) if len(industry_rsi_values['rsi_m']) > 0 else None
                    }
                    
                    sector_rsi_list.append(sector_rsi)
                    
                    rsi_d_str = f"{sector_rsi['rsi_d']:.2f}" if sector_rsi['rsi_d'] is not None else 'N/A'
                    total_stocks = len(stock_codes)
                    
                    if sector_rsi['rsi_d'] is not None:
                        self.logger.info(f"업종 RSI 계산 완료 - {market_type} {industry}: 총 {total_stocks}개 종목 중 {valid_stocks}개 유효, 일간RSI: {rsi_d_str}")
                    else:
                        self.logger.warning(f"업종 RSI 계산 불가 - {market_type} {industry}: 총 {total_stocks}개 종목 중 유효한 RSI 데이터가 없음")
                    
                except Exception as e:
                    self.logger.error(f"업종 RSI 계산 오류 - {market_type} {industry}: {e}")
                    continue
            
            self.logger.info(f"전체 {market_type} 업종 RSI 계산 완료 - {len(sector_rsi_list)}개 업종")
            return sector_rsi_list
            
        except Exception as e:
            self.logger.error(f"{market_type} 업종별 RSI 일괄 계산 오류: {e}")
            raise
    
    def find_sector_leaders(self, conn, trade_date, market_type, top_n=3):
        """
        특정 시장의 각 업종별 대장주를 찾습니다 (시가총액 기준).
        
        Args:
            conn: DB 연결 객체
            trade_date (str): 기준일
            market_type (str): 시장 구분 ('KOSPI' 또는 'KOSDAQ')
            top_n (int): 업종별 상위 몇 개 종목을 가져올지
        
        Returns:
            dict: 업종별 대장주 정보
        """
        try:
            with conn.cursor() as cursor:
                sql = """
                SELECT industry, stock_code, stock_name, market_cap, close_price, change_rate
                FROM krx_stock 
                WHERE trade_date = %s
                AND market_type = %s
                AND industry IS NOT NULL
                AND industry != ''
                AND market_cap IS NOT NULL
                ORDER BY industry, market_cap DESC
                """
                cursor.execute(sql, (trade_date, market_type))
                all_stocks = cursor.fetchall()
            
            if not all_stocks:
                return {}
            
            sector_leaders = defaultdict(list)
            current_industry = None
            count = 0
            
            for stock in all_stocks:
                if current_industry != stock['industry']:
                    current_industry = stock['industry']
                    count = 0
                
                if count < top_n:
                    sector_leaders[stock['industry']].append({
                        'stock_code': stock['stock_code'],
                        'stock_name': stock['stock_name'],
                        'market_cap': stock['market_cap'],
                        'close_price': stock['close_price'],
                        'change_rate': stock['change_rate']
                    })
                    count += 1
            
            self.logger.info(f"{market_type} 업종별 대장주 조회 완료 - {len(sector_leaders)}개 업종")
            return dict(sector_leaders)
            
        except Exception as e:
            self.logger.error(f"{market_type} 업종별 대장주 조회 오류: {e}")
            return {}
    
    def get_excluded_sectors(self):
        """환경변수에서 제외할 섹터 목록을 가져옵니다."""
        excluded_sectors_str = os.getenv('EXCLUDED_SECTORS', '기타')
        return [sector.strip() for sector in excluded_sectors_str.split(',') if sector.strip()]
    
    def get_rsi_summary(self, conn, trade_date=None, market_type=None):
        """
        RSI 요약 정보를 조회합니다.
        
        Args:
            conn: DB 연결 객체
            trade_date (str): 조회할 날짜 (None이면 최신)
            market_type (str): 시장 구분 (None이면 전체)
        
        Returns:
            dict: RSI 요약 정보
        """
        try:
            excluded_sectors = self.get_excluded_sectors()
            
            with conn.cursor() as cursor:
                params = []
                sql = """
                SELECT industry, rsi_d, rsi_w, rsi_m, market_type
                FROM krx_sector_rsi 
                """
                
                where_clauses = []
                if trade_date:
                    where_clauses.append("trade_date = %s")
                    params.append(trade_date)
                else:
                    where_clauses.append("trade_date = (SELECT MAX(trade_date) FROM krx_sector_rsi)")
                
                if market_type:
                    where_clauses.append("market_type = %s")
                    params.append(market_type)
                
                # 제외할 섹터들 필터링
                if excluded_sectors:
                    excluded_placeholders = ', '.join(['%s'] * len(excluded_sectors))
                    where_clauses.append(f"industry NOT IN ({excluded_placeholders})")
                    params.extend(excluded_sectors)
                
                if where_clauses:
                    sql += " WHERE " + " AND ".join(where_clauses)
                
                sql += " ORDER BY rsi_d DESC"
                
                cursor.execute(sql, tuple(params))
                sectors = cursor.fetchall()
            
            if not sectors:
                return {}
            
            summary = {
                'total_sectors': len(sectors),
                'all_sectors': sectors,
                'overbought': [],
                'oversold': [],
                'neutral': []
            }
            
            for sector in sectors:
                rsi_d = sector['rsi_d']
                if rsi_d is None:
                    continue
                
                if rsi_d > 70:
                    summary['overbought'].append(sector)
                elif rsi_d < 30:
                    summary['oversold'].append(sector)
                else:
                    summary['neutral'].append(sector)
            
            return summary
            
        except Exception as e:
            self.logger.error(f"RSI 요약 정보 조회 오류: {e}")
            return {}

# 테스트용 실행 코드
if __name__ == "__main__":
    calculator = RSICalculator()
    
    # 테스트 RSI 계산
    test_prices = [100, 102, 101, 103, 105, 104, 106, 108, 107, 109, 
                   111, 110, 112, 114, 113, 115, 117, 116, 118, 120]
    
    rsi_result = calculator.calculate_rsi(test_prices, 14)
    print(f"테스트 RSI 계산 결과: {rsi_result:.2f if rsi_result else 'N/A'}")
    
    # DB 연결 테스트
    conn = get_db_connection()
    if conn:
        try:
            # 최신 RSI 요약 정보 조회
            summary = calculator.get_rsi_summary(conn)
            print(f"RSI 요약 정보: 총 {summary.get('total_sectors', 0)}개 업종")
            print(f"과매수 구간: {len(summary.get('overbought', []))}개 업종")
            print(f"과매도 구간: {len(summary.get('oversold', []))}개 업종")
        finally:
            conn.close()
    else:
        print("DB 연결 실패")