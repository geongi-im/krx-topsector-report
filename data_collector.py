from pykrx.website.krx.market.core import 업종분류현황
from datetime import datetime, timedelta
import pandas as pd
import holidays
from utils.logger_util import LoggerUtil

class KRXDataCollector:
    def __init__(self):
        self.logger = LoggerUtil().get_logger()
        self.kr_holidays = holidays.Korea()
    
    def is_trading_day(self, date):
        """거래일인지 확인 (평일이면서 한국 공휴일이 아닌 날)"""
        if isinstance(date, str):
            date = datetime.strptime(date, '%Y%m%d').date()
        elif isinstance(date, datetime):
            date = date.date()
        
        # 주말 체크
        if date.weekday() >= 5:  # 토요일(5), 일요일(6)
            return False
        
        # 한국 공휴일 체크
        if date in self.kr_holidays:
            return False
        
        return True
    
    def get_previous_trading_day(self, date_str=None):
        """이전 거래일을 반환합니다."""
        if date_str is None:
            target_date = datetime.now().date()
        else:
            target_date = datetime.strptime(date_str, '%Y%m%d').date()
        
        # 하루씩 뒤로 가면서 거래일 찾기
        current_date = target_date - timedelta(days=1)
        while not self.is_trading_day(current_date):
            current_date -= timedelta(days=1)
        
        return current_date.strftime('%Y%m%d')
    
    def fetch_stock_data(self, date_str, market='STK'):
        """
        특정 날짜의 주식 데이터를 수집합니다.
        
        Args:
            date_str (str): 조회할 날짜 (YYYYMMDD 형식)
            market (str): 시장 구분 ('STK' for KOSPI, 'KSQ' for KOSDAQ)
        
        Returns:
            list: 주식 데이터 리스트
        """
        try:
            self.logger.info(f"KRX 데이터 수집 시작 - 날짜: {date_str}, 시장: {market}")
            
            # pykrx 업종분류현황 API 호출
            fetcher = 업종분류현황()
            raw_data = fetcher.fetch(date_str, market)
            
            
            if raw_data is None or (hasattr(raw_data, 'empty') and raw_data.empty):
                self.logger.warning(f"API에서 빈 데이터 반환 - 날짜: {date_str}, 시장: {market}")
                return []
            
            # 데이터프레임이 아닌 경우 처리
            if not hasattr(raw_data, 'iterrows'):
                self.logger.error(f"예상치 못한 데이터 타입: {type(raw_data)} - 날짜: {date_str}, 시장: {market}")
                return []
            
            # 데이터 변환
            stock_data_list = []
            market_type = 'KOSPI' if market == 'STK' else 'KOSDAQ'
            
            # 컬럼명 매핑 (실제 API 응답에 따라 수정)
            column_mapping = {
                '종목코드': ['ISU_SRT_CD', '종목코드', 'ISU_CD', 'Code'],
                '종목명': ['ISU_ABBRV', '종목명', 'ISU_NM', 'Name'],
                '업종명': ['IDX_IND_NM', '업종명', 'SEC_NM', 'Sector'],
                '종가': ['TDD_CLSPRC', '종가', 'Close'],
                '대비': ['CMPPREVDD_PRC', '대비', 'Change'],
                '등락률': ['FLUC_RT', '등락률', 'ChangeRate'],
                '시가총액': ['MKTCAP', '시가총액', 'MarketCap']
            }
            
            def get_column_value(row, key_variations):
                """여러 가능한 컬럼명으로 값 찾기"""
                for col_name in key_variations:
                    if col_name in row.index:
                        return row[col_name]
                return None
            
            processed_count = 0
            for idx, row in raw_data.iterrows():
                try:
                    stock_code = get_column_value(row, column_mapping['종목코드'])
                    stock_name = get_column_value(row, column_mapping['종목명'])
                    industry = get_column_value(row, column_mapping['업종명'])
                    close_price = get_column_value(row, column_mapping['종가'])
                    
                    
                    stock_data = {
                        'stock_code': str(stock_code).strip() if stock_code else '',
                        'stock_name': str(stock_name).strip() if stock_name else '',
                        'market_type': market_type,
                        'industry': str(industry).strip() if industry else '',
                        'trade_date': datetime.strptime(date_str, '%Y%m%d').date(),
                        'close_price': self._safe_float(close_price),
                        'change_amount': self._safe_float(get_column_value(row, column_mapping['대비'])),
                        'change_rate': self._safe_float(get_column_value(row, column_mapping['등락률'])),
                        'market_cap': self._safe_int(get_column_value(row, column_mapping['시가총액']))
                    }
                    
                    # 필수 데이터 검증
                    if (stock_data['stock_code'] and 
                        stock_data['stock_name'] and 
                        stock_data['close_price'] is not None):
                        stock_data_list.append(stock_data)
                        
                    processed_count += 1
                        
                except Exception as e:
                    self.logger.error(f"개별 종목 데이터 처리 오류: {e}, row: {dict(row)}")
                    continue
            
            self.logger.info(f"데이터 수집 완료 - {len(stock_data_list)}개 종목 (날짜: {date_str}, 시장: {market})")
            
            
            return stock_data_list
            
        except Exception as e:
            self.logger.error(f"KRX 데이터 수집 오류 - 날짜: {date_str}, 시장: {market}, 오류: {e}")
            raise
    
    
    def _safe_float(self, value):
        """안전하게 float로 변환 (콤마 제거 포함)"""
        if value is None or value == '' or pd.isna(value):
            return None
        try:
            # 문자열인 경우 콤마 제거
            if isinstance(value, str):
                value = value.replace(',', '')
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value):
        """안전하게 int로 변환 (콤마 제거 포함)"""
        if value is None or value == '' or pd.isna(value):
            return None
        try:
            # 문자열인 경우 콤마 제거
            if isinstance(value, str):
                value = value.replace(',', '')
            return int(float(value))
        except (ValueError, TypeError):
            return None

# 테스트용 실행 코드
if __name__ == "__main__":
    collector = KRXDataCollector()
    
    # 오늘이 거래일인지 확인
    today = datetime.now().strftime('%Y%m%d')
    print(f"오늘({today})이 거래일인가? {collector.is_trading_day(today)}")
    
    # 이전 거래일 확인
    prev_trading_day = collector.get_previous_trading_day()
    print(f"이전 거래일: {prev_trading_day}")
    
    # 테스트 데이터 수집 (이전 거래일)
    try:
        test_data = collector.fetch_stock_data(prev_trading_day, 'STK')
        print(f"KOSPI 데이터 수집 결과: {len(test_data)}개 종목")
        
        if test_data:
            print("첫 번째 종목 데이터 샘플:")
            print(test_data[0])
            
    except Exception as e:
        print(f"테스트 데이터 수집 실패: {e}")