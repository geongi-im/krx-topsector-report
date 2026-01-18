"""
KRX 주식 데이터 서비스 모듈

이 모듈은 다음 기능을 제공합니다:
- KRX API를 통한 주식 데이터 수집 (KRXDataCollector)
- RSI 기술적 지표 계산 (RSICalculator)
- 업종별 대장주 추적 (SectorLeaderTracker)
"""

# Standard library imports
import os
from datetime import datetime, timedelta
from collections import defaultdict

# Third-party imports
import pandas as pd
import numpy as np
import holidays
from dotenv import load_dotenv
from pykrx.website.krx.market.core import 업종분류현황

# Local imports
from utils.logger_util import LoggerUtil
from utils.db_manager import get_db_connection, insert_sector_leaders

# 환경변수 로드 (한 번만)
load_dotenv()


class KRXDataCollector:
    """KRX API를 통한 주식 데이터 수집 클래스"""

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


class RSICalculator:
    """RSI 계산 및 업종별 RSI 요약 클래스"""

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
                    return {'rsi_d': None, 'rsi_w': None, 'rsi_m': None}

                # 시간 순서대로 정렬 (과거 -> 현재)
                prices = [row['close_price'] for row in reversed(result) if row['close_price'] is not None]

                if len(prices) < max(rsi_periods.values()) + 1:
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

                    if sector_rsi['rsi_d'] is None:
                        self.logger.warning(f"업종 RSI 계산 불가 - {market_type} {industry}: 유효한 RSI 데이터 없음")

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


class SectorLeaderTracker:
    """업종별 대장주 추적 및 연속일수 계산 클래스"""

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

                # 기존 대장주 정보 조회
                existing_leaders = self._get_existing_leaders(conn, market_type)

                leaders_with_streak = []
                for industry, stocks in current_leaders.items():
                    for rank, stock in enumerate(stocks[:2], 1):
                        consecutive_days = self._calculate_consecutive_days(
                            conn, industry, rank, stock['stock_code'], market_type, trade_date
                        )
                        # 이전 종목명은 메모리에서만 비교(리포트용)
                        key = (industry, rank)
                        prev_stock_name = existing_leaders.get(key, {}).get('stock_name')
                        leader_data = {
                            'industry': industry,
                            'rank_position': rank,
                            'stock_code': stock['stock_code'],
                            'stock_name': stock['stock_name'],
                            'market_cap': stock['market_cap'],
                            'consecutive_days': consecutive_days,
                            'market_type': market_type,
                            'prev_stock_name': prev_stock_name if prev_stock_name != stock['stock_name'] else None
                        }
                        leaders_with_streak.append(leader_data)

                # DB 저장시 prev_stock_name은 제외
                db_leaders = [
                    {k: v for k, v in leader.items() if k not in ('prev_stock_name',)}
                    for leader in leaders_with_streak
                ]
                if db_leaders:
                    inserted_count = insert_sector_leaders(conn, db_leaders)
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

    def _get_existing_leaders(self, conn, market_type):
        """
        현재 DB에 저장된 기존 대장주 정보를 조회합니다.

        Args:
            conn: DB 연결 객체
            market_type: 시장 구분 (KOSPI/KOSDAQ)

        Returns:
            dict: {(industry, rank_position): {'stock_name': 종목명, 'stock_code': 종목코드}} 형태
        """
        try:
            with conn.cursor() as cursor:
                sql = """
                SELECT industry, rank_position, stock_code, stock_name
                FROM krx_sector_leaders
                WHERE market_type = %s
                """
                cursor.execute(sql, (market_type,))
                results = cursor.fetchall()

            existing_leaders = {}
            for row in results:
                key = (row['industry'], row['rank_position'])
                existing_leaders[key] = {
                    'stock_name': row['stock_name'],
                    'stock_code': row['stock_code']
                }

            self.logger.debug(f"{market_type} 기존 대장주 조회 완료 - {len(existing_leaders)}개 레코드")
            return existing_leaders

        except Exception as e:
            self.logger.error(f"기존 대장주 조회 오류 ({market_type}): {e}")
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
                        return new_consecutive_days
                    else:
                        # 다른 종목으로 변경된 경우 1일로 리셋
                        self.logger.info(f"{industry} {rank_position}위 변경: {current_record['stock_code']} -> {stock_code}")
                        return 1
                else:
                    # 새로운 업종/순위 조합인 경우
                    self.logger.info(f"{industry} {rank_position}위 신규 등록: {stock_code}")
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
                    else:
                        # 다른 종목이거나 데이터가 없으면 중단
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
    print("=== KRX Service Module Test ===\n")

    # 1. KRXDataCollector 테스트
    print("1. KRXDataCollector 테스트")
    collector = KRXDataCollector()
    today = datetime.now().strftime('%Y%m%d')
    print(f"   오늘({today})이 거래일인가? {collector.is_trading_day(today)}")
    prev_trading_day = collector.get_previous_trading_day()
    print(f"   이전 거래일: {prev_trading_day}")

    # 2. RSICalculator 테스트
    print("\n2. RSICalculator 테스트")
    calculator = RSICalculator()
    test_prices = [100, 102, 101, 103, 105, 104, 106, 108, 107, 109,
                   111, 110, 112, 114, 113, 115, 117, 116, 118, 120]
    rsi_result = calculator.calculate_rsi(test_prices, 14)
    print(f"   테스트 RSI 계산 결과: {rsi_result:.2f if rsi_result else 'N/A'}")

    # 3. DB 연결 테스트
    print("\n3. DB 연결 및 SectorLeaderTracker 테스트")
    conn = get_db_connection()
    if conn:
        try:
            tracker = SectorLeaderTracker()
            formatted_date = f"{prev_trading_day[:4]}-{prev_trading_day[4:6]}-{prev_trading_day[6:8]}"
            leaders = tracker.get_sector_leaders_with_streak(conn, formatted_date, 'KOSPI')
            print(f"   조회된 업종 수: {len(leaders)}")
        finally:
            conn.close()
    else:
        print("   DB 연결 실패")
