import pandas as pd
import os
import imgkit
from datetime import datetime
from dotenv import load_dotenv
from utils.telegram_util import TelegramUtil
from utils.logger_util import LoggerUtil
load_dotenv()

class TableReportGenerator:
    def __init__(self):
        self.logger = LoggerUtil().get_logger()
        self.telegram = TelegramUtil()
        self.img_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'img')
        self.wkhtmltoimage_path = os.getenv('WKHTMLTOIMAGE_PATH')
        
        if not os.path.exists(self.img_dir):
            os.makedirs(self.img_dir)
    
    def format_market_cap_billions(self, market_cap):
        """시가총액을 억원 단위로 변환 (천단위 콤마 포함)"""
        if market_cap is None or market_cap == 0:
            return "N/A"
        billions = market_cap // 100000000
        return f"{billions:,}억"
    
    def format_leader_cell(self, stock_name, stock_code, market_cap, consecutive_days, prev_stock_name=None):
        """대장주 셀 정보 포맷팅 (HTML <br> 태그 사용)"""
        result = f"{stock_name}<br>"
        
        # 연속일수 또는 변경 정보 추가 (색상 포함)
        if prev_stock_name:
            # 종목이 변경된 경우 - 파란색
            result += f'<span style="color: #2E86C1;">NEW (이전: {prev_stock_name})</span>'
        else:
            # 동일 종목이 유지되는 경우 - 붉은색
            result += f'<span style="color: #E74C3C;">({consecutive_days}일 연속)</span>'
        
        return result

    def get_text_color(self, background_color):
        """배경색에 따른 적절한 텍스트 색상 반환"""
        # 어두운 배경색에는 흰색 텍스트, 밝은 배경색에는 검은색 텍스트
        dark_backgrounds = ["#5c88c7", "#fc676b"]  # 어두운 배경색들
        if background_color in dark_backgrounds:
            return "#ffffff"
        else:
            return "#000000"
    
    def format_rsi_cell(self, rsi_value):
        """RSI 셀 포맷팅 (단순 숫자 반환)"""
        if rsi_value is None:
            return 'N/A'
        return f'{rsi_value:.2f}'

    def split_dataframe(self, df, rows_per_page=10):
        """DataFrame을 지정된 행 수로 분할"""
        chunks = []
        for i in range(0, len(df), rows_per_page):
            chunks.append(df.iloc[i:i + rows_per_page])
        return chunks
    
    def create_sector_dataframe(self, rsi_data, leaders_data, market_type):
        """섹터별 RSI와 대장주 정보를 DataFrame으로 변환"""
        try:
            # RSI 데이터를 딕셔너리로 변환 (빠른 조회를 위해)
            rsi_dict = {}
            
            # all_sectors 데이터를 사용 (모든 섹터 포함)
            all_sectors = rsi_data['all_sectors']
            
            # RSI 딕셔너리 생성
            for sector in all_sectors:
                industry = sector.get('industry')
                # market_type이 일치하는 데이터만 사용
                if sector.get('market_type') == market_type and industry:
                    # 고유 키로 (industry, market_type) 사용
                    key = (industry, sector.get('market_type'))
                    if key not in rsi_dict:
                        rsi_dict[key] = {
                            'rsi_d': sector.get('rsi_d'),
                            'rsi_w': sector.get('rsi_w'),
                            'rsi_m': sector.get('rsi_m')
                        }
            
            # DataFrame 생성용 데이터 리스트
            table_data = []
            
            # RSI 데이터가 있는 업종들만 처리
            for (industry, m_type), rsi_info in rsi_dict.items():
                leaders = leaders_data.get(industry, [])
                
                # 대장주 1위, 2위 정보
                leader_1_info = "N/A"
                leader_2_info = "N/A"
                
                if leaders:
                    # 1위 대장주
                    leader_1 = next((l for l in leaders if l.get('rank') == 1), None)
                    if leader_1:
                        leader_1_info = self.format_leader_cell(
                            leader_1.get('stock_name', 'Unknown'),
                            leader_1.get('stock_code', '000000'),
                            leader_1.get('market_cap', 0),
                            leader_1.get('consecutive_days', 1),
                            leader_1.get('prev_stock_name')
                        )
                    # 2위 대장주
                    leader_2 = next((l for l in leaders if l.get('rank') == 2), None)
                    if leader_2:
                        leader_2_info = self.format_leader_cell(
                            leader_2.get('stock_name', 'Unknown'),
                            leader_2.get('stock_code', '000000'),
                            leader_2.get('market_cap', 0),
                            leader_2.get('consecutive_days', 1),
                            leader_2.get('prev_stock_name')
                        )
                
                # 업종명 길이 제한 (테이블 가독성을 위해)
                display_industry = industry[:12] + "..." if len(industry) > 12 else industry
                
                table_data.append({
                    '섹터명': display_industry,
                    'RSI(90)': self.format_rsi_cell(rsi_info['rsi_m']),
                    'RSI(30)': self.format_rsi_cell(rsi_info['rsi_w']),
                    'RSI(14)': self.format_rsi_cell(rsi_info['rsi_d']),
                    '1등주': leader_1_info,
                    '2등주': leader_2_info
                })
            
            def extract_rsi_value(rsi_str):
                try:
                    if rsi_str == 'N/A' or rsi_str is None:
                        return 0
                    return float(rsi_str)
                except:
                    return 0
            
            df = pd.DataFrame(table_data)
            return df
        except Exception as e:
            self.logger.error(f"섹터 DataFrame 생성 오류: {e}")
            return pd.DataFrame()  # 빈 DataFrame 반환
    
    def save_df_as_image(self, df, title, file_name='sector_table.png', page_num=None, total_pages=None):
        """DataFrame을 이미지로 저장하고 파일 경로와 제목 반환 (example.py 스타일)

        Args:
            df: 이미지로 변환할 DataFrame
            title: 이미지 제목
            file_name: 저장할 파일명
            page_num: 현재 페이지 번호 (1부터 시작, None이면 페이지 정보 미표시)
            total_pages: 전체 페이지 수 (None이면 페이지 정보 미표시)
        """
        if df is None or df.empty:
            self.logger.warning("빈 DataFrame으로 이미지 생성 불가")
            return None, None

        file_name_base, file_extension = os.path.splitext(file_name)
        current_date = datetime.now().strftime('%Y%m%d')

        # 페이지 번호가 있으면 파일명에 추가
        if page_num is not None:
            new_file_path = os.path.join(self.img_dir, f"{file_name_base}_{current_date}_p{page_num}{file_extension}")
        else:
            new_file_path = os.path.join(self.img_dir, f"{file_name_base}_{current_date}{file_extension}")
        
        # 기본 HTML 테이블 생성
        html_table = df.to_html(index=False, classes='styled-table', escape=False, table_id='styled-table')
        
        # RSI 값에 따른 배경색 적용 함수
        def get_rsi_background_style(rsi_value):
            """RSI 값에 따른 인라인 스타일 반환"""
            if rsi_value == 'N/A' or rsi_value is None:
                return 'background-color: #f0f0f0; color: #666666; font-weight: 500; text-align: center;'
            
            try:
                rsi = float(rsi_value)
                if 0 <= rsi <= 19:
                    return 'background-color: #5c88c7; color: #ffffff; font-weight: 600; text-align: center;'  # 0-19
                elif 20 <= rsi <= 29:
                    return 'background-color: #b2c7e2; color: #000000; font-weight: 600; text-align: center;'  # 20-29
                elif 30 <= rsi <= 44:
                    return 'background-color: #e7eef8; color: #000000; font-weight: 600; text-align: center;'  # 30-44
                elif 45 <= rsi <= 55:
                    return 'background-color: #ffffff; color: #000000; font-weight: 600; text-align: center;'  # 45-55
                elif 56 <= rsi <= 70:
                    return 'background-color: #ffdadb; color: #000000; font-weight: 600; text-align: center;'  # 56-70
                elif 71 <= rsi <= 80:
                    return 'background-color: #fa9396; color: #000000; font-weight: 600; text-align: center;'  # 71-80
                elif 81 <= rsi <= 100:
                    return 'background-color: #fc676b; color: #ffffff; font-weight: 600; text-align: center;'  # 81-100
                else:
                    return 'background-color: #ffffff; color: #000000; font-weight: 600; text-align: center;'
            except:
                return 'background-color: #f0f0f0; color: #666666; font-weight: 500; text-align: center;'
        
        # HTML에서 RSI 컬럼 셀들에 정규식으로 스타일 적용
        import re
        
        # 각 행을 찾아서 RSI 값이 있는 셀에 스타일 적용
        def replace_rsi_cells(match):
            row_html = match.group(0)
            cells = re.findall(r'<td>(.*?)</td>', row_html)
            
            # 헤더 순서에 맞춰 RSI 컬럼 찾기 (RSI(90), RSI(30), RSI(14)는 2,3,4번째 컬럼)
            rsi_columns = [1, 2, 3]  # 0-based 인덱스 (섹터명 다음 세 컬럼)
            
            for i, cell_content in enumerate(cells):
                if i in rsi_columns:
                    rsi_value = cell_content.strip()
                    style = get_rsi_background_style(rsi_value)
                    # 해당 셀을 스타일이 적용된 것으로 교체
                    old_cell = f'<td>{cell_content}</td>'
                    new_cell = f'<td style="{style}">{cell_content}</td>'
                    row_html = row_html.replace(old_cell, new_cell, 1)
            
            return row_html
        
        # tbody 내의 모든 행에 적용
        html_table = re.sub(r'<tr>\s*(<td>.*?</td>\s*)+</tr>', replace_rsi_cells, html_table, flags=re.DOTALL)

        # 페이지 정보가 있으면 제목에 추가
        display_title = title
        if page_num is not None and total_pages is not None:
            display_title = f"{title} ({page_num}/{total_pages})"

        # example.py와 동일한 HTML 스타일
        html_str = f'''
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;700&display=swap" rel="stylesheet">
            <style>
                body {{
                    font-family: 'Noto Sans KR', sans-serif;
                    margin: 20px;
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    margin: 20px auto;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                }}
                th, td {{
                    border: 1px solid #e0e0e0;
                    padding: 12px 15px;
                    text-align: center;
                    vertical-align: middle;
                }}
                th {{
                    background-color: #333333;
                    color: white;
                    font-weight: 700;
                    font-size: 15px;
                }}
                td {{
                    font-size: 13px;
                    font-weight: 500;
                    line-height: 1.4;
                }}
                
                .caption {{
                    text-align: center;
                    font-size: 22px;
                    font-weight: 700;
                    margin: 20px 0;
                    color: #333333;
                }}
                .source {{
                    text-align: right;
                    font-size: 12px;
                    color: #666666;
                    margin-top: 15px;
                    font-weight: 400;
                }}
                /* 대장주 컬럼 너비 조정 */
                #styled-table th:nth-child(5),
                #styled-table th:nth-child(6),
                #styled-table td:nth-child(5),
                #styled-table td:nth-child(6) {{
                    min-width: 150px;
                    white-space: pre-line;
                }}
            </style>
        </head>
        <body>
            <div class="caption">{display_title}</div>
            {html_table}
            <div class="source">※ 출처 : MQ(Money Quotient)</div>
        </body>
        </html>
        '''

        options = {
            'format': 'png',
            'encoding': "UTF-8",
            'quality': 90,
            'width': 800,  # 테이블이 넓어서 800px로 조정
            'enable-local-file-access': None
        }

        try:
            if not self.wkhtmltoimage_path:
                error_message = "❌ 오류 발생\n\nWKHTMLTOIMAGE_PATH 환경변수가 설정되지 않았습니다."
                self.logger.error(error_message)
                raise ValueError("WKHTMLTOIMAGE_PATH 환경변수가 필요합니다.")
                
            config = imgkit.config(wkhtmltoimage=self.wkhtmltoimage_path)
            imgkit.from_string(html_str, new_file_path, options=options, config=config)
            return new_file_path, title
            
        except Exception as e:
            error_message = f"❌ 테이블 이미지 생성 오류\n\n함수: save_df_as_image\n파일: {file_name}\n오류: {str(e)}"
            self.logger.error(error_message)
            return None, None
    
    def create_sector_table_report(self, rsi_data, leaders_data, trade_date, market_type, rows_per_page=10):
        """섹터 테이블 리포트를 생성하고 이미지 경로 리스트 반환

        Args:
            rsi_data: RSI 데이터
            leaders_data: 대장주 데이터
            trade_date: 거래일
            market_type: 시장 유형 (KOSPI/KOSDAQ)
            rows_per_page: 페이지당 행 수 (기본값: 10)

        Returns:
            list[str]: 생성된 이미지 경로 리스트 (실패 시 빈 리스트)
        """
        try:
            self.logger.info(f"{market_type} 섹터 테이블 리포트 생성 시작 - 기준일: {trade_date}")

            df = self.create_sector_dataframe(rsi_data, leaders_data, market_type)

            if df.empty:
                self.logger.warning(f"{market_type}에서 생성할 데이터가 없어 테이블 리포트 생성 중단")
                return []

            # DataFrame을 페이지별로 분할
            df_chunks = self.split_dataframe(df, rows_per_page)
            total_pages = len(df_chunks)

            title = f"{trade_date} {market_type} 섹터 RSI & 대장주 현황"
            file_name_base = f"sector_table_{market_type.lower()}"
            file_extension = ".png"

            # 이미지 생성 전 기존 파일 삭제 (한 번만 실행)
            try:
                for old_file in os.listdir(self.img_dir):
                    if old_file.startswith(file_name_base) and old_file.endswith(file_extension):
                        os.remove(os.path.join(self.img_dir, old_file))
            except Exception as e:
                self.logger.warning(f"기존 파일 삭제 중 오류: {e}")

            image_paths = []
            for page_num, chunk_df in enumerate(df_chunks, start=1):
                img_path, caption = self.save_df_as_image(
                    chunk_df,
                    title,
                    f"{file_name_base}{file_extension}",
                    page_num=page_num,
                    total_pages=total_pages
                )

                if img_path:
                    image_paths.append(img_path)
                    self.logger.info(f"{market_type} 테이블 리포트 ({page_num}/{total_pages}) 생성 완료: {img_path}")
                else:
                    self.logger.error(f"{market_type} 테이블 이미지 ({page_num}/{total_pages}) 생성 실패")

            if image_paths:
                self.logger.info(f"{market_type} 섹터 테이블 리포트 총 {len(image_paths)}개 이미지 생성 완료")
            else:
                self.logger.error(f"{market_type} 테이블 리포트 이미지 생성 실패")

            return image_paths

        except Exception as e:
            self.logger.error(f"{market_type} 섹터 테이블 리포트 생성 오류: {e}")
            return []

# 테스트용 실행 코드
if __name__ == "__main__":
    generator = TableReportGenerator()
    
    # 테스트 데이터
    test_rsi_data = {
        'total_sectors': 15,
        'all_sectors': [
            {'industry': '반도체', 'rsi_d': 75.2, 'rsi_w': 68.5, 'rsi_m': 65.8},
            {'industry': 'IT서비스', 'rsi_d': 72.1, 'rsi_w': 69.3, 'rsi_m': 71.2},
            {'industry': '바이오', 'rsi_d': 68.9, 'rsi_w': 65.4, 'rsi_m': 62.7},
            {'industry': '조선', 'rsi_d': 25.3, 'rsi_w': 28.7, 'rsi_m': 31.2},
            {'industry': '철강', 'rsi_d': 28.7, 'rsi_w': 32.1, 'rsi_m': 35.6},
            {'industry': '화학', 'rsi_d': 31.2, 'rsi_w': 35.8, 'rsi_m': 38.9}
        ]
    }
    
    test_leaders_data = {
        '반도체': [
            {'rank': 1, 'stock_name': '삼성전자', 'stock_code': '005930', 'market_cap': 50000000000000, 'consecutive_days': 15, 'prev_stock_name': 'SK하이닉스'},
            {'rank': 2, 'stock_name': 'SK하이닉스', 'stock_code': '000660', 'market_cap': 30000000000000, 'consecutive_days': 8, 'prev_stock_name': '삼성전자'}
        ],
        'IT서비스': [
            {'rank': 1, 'stock_name': 'NAVER', 'stock_code': '035420', 'market_cap': 33400000000000, 'consecutive_days': 5, 'prev_stock_name': '카카오'},
            {'rank': 2, 'stock_name': '카카오', 'stock_code': '035720', 'market_cap': 20000000000000, 'consecutive_days': 3, 'prev_stock_name': 'NAVER'}
        ]
    }
    
    # 테이블 리포트 생성 테스트
    image_path = generator.create_sector_table_report(test_rsi_data, test_leaders_data, "2024-01-15")
    if image_path:
        print(f"테이블 리포트 생성 성공: {image_path}")
    else:
        print("테이블 리포트 생성 실패")