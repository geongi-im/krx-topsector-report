# KRX Top Sector Report

> 한국거래소(KRX) 섹터별 주식 데이터 자동 수집 및 RSI 지표 분석 시스템

## 프로젝트 개요

KRX Top Sector Report는 한국거래소의 모든 상장 종목을 섹터별로 분석하여 RSI(Relative Strength Index) 지표와 대장주 정보를 자동으로 수집·분석하고, 시각적 리포트를 텔레그램 및 외부 API로 전송하는 자동화 시스템입니다.

### 주요 기능

- **실시간 데이터 수집**: pykrx 라이브러리를 통한 KOSPI/KOSDAQ 전 종목 데이터 자동 수집
- **RSI 지표 계산**: 섹터별 14일/30일/90일 RSI 지표 계산 (Wilder's 표준 방식)
- **대장주 추적**: 시가총액 기준 섹터별 1위, 2위 종목 추적 및 연속 유지 일수 계산
- **시각적 리포트**: RSI 구간별 색상 코딩이 적용된 HTML 테이블 이미지 리포트 생성
- **텔레그램 자동 전송**: 생성된 리포트를 텔레그램 봇을 통해 자동 전송
- **외부 API 연동**: 생성된 리포트를 외부 API로 게시글 자동 등록
- **완전 자동화**: 스케줄링 가능한 데이터 수집부터 리포트 전송까지 원클릭 자동화

## 시스템 아키텍처

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   KRX API       │───▶│  Data Collector  │───▶│   MySQL DB      │
│   (pykrx)       │    │                  │    │   (3 Tables)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ Telegram Bot &  │◀───│  Report Generator│◀───│  RSI Calculator │
│  External API   │    │  (HTML to Image) │    │ & Leader Tracker│
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 프로젝트 구조

```
krx-topsector-report/
├── main.py                          # 메인 실행 파일
├── data_collector.py                # KRX 데이터 수집기
├── rsi_calculator.py                # RSI 지표 계산기
├── sector_leader_tracker.py         # 섹터 대장주 추적기
├── table_report_generator.py        # 테이블 리포트 생성기 (HTML to Image)
├── recalculate_consecutive_days.py  # 연속일수 재계산 유틸리티
├── requirements.txt                 # 의존성 패키지 목록
├── .env                             # 환경변수 설정
├── CLAUDE.md                        # Claude Code용 프로젝트 설명
├── utils/                           # 유틸리티 모듈
│   ├── db_manager.py                # 데이터베이스 연결 및 테이블 관리
│   ├── logger_util.py               # 로깅 시스템
│   ├── telegram_util.py             # 텔레그램 봇 메시지/사진 전송
│   └── api_util.py                  # 외부 API 통신 및 이미지 압축
├── logs/                            # 로그 파일 저장소 (YYYY-MM-DD_log.log)
├── img/                             # 생성된 리포트 이미지
└── thumbnail/                       # API 게시글용 썸네일 이미지
```

## 데이터베이스 스키마

### 1. krx_stock (일별 주가 데이터)
```sql
CREATE TABLE IF NOT EXISTS krx_stock (
    idx INT AUTO_INCREMENT PRIMARY KEY COMMENT '내부 고유 ID',
    stock_code VARCHAR(10) NOT NULL COMMENT '종목코드',
    stock_name VARCHAR(100) NOT NULL COMMENT '종목명',
    market_type VARCHAR(10) NOT NULL COMMENT '시장구분 (KOSPI, KOSDAQ)',
    industry VARCHAR(100) COMMENT '업종명',
    trade_date DATE NOT NULL COMMENT '거래일',
    close_price FLOAT NOT NULL COMMENT '종가',
    change_amount FLOAT COMMENT '대비 (전일 대비 금액)',
    change_rate FLOAT COMMENT '등락률 (%)',
    market_cap BIGINT COMMENT '시가총액',
    reg_date DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '등록일시',

    UNIQUE KEY uq_stock_trade_date (stock_code, trade_date),
    KEY idx_stock_code (stock_code),
    KEY idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='KRX 일별 주가 데이터';
```

- 보존 기간: 365일 (자동 삭제)
- 일일 수집: KOSPI 및 KOSDAQ 전 종목

### 2. krx_sector_rsi (섹터별 RSI 지표)
```sql
CREATE TABLE IF NOT EXISTS krx_sector_rsi (
    idx INT AUTO_INCREMENT PRIMARY KEY COMMENT '내부 고유 ID',
    trade_date DATE NOT NULL COMMENT 'RSI 계산 기준일',
    market_type VARCHAR(10) NOT NULL COMMENT '시장구분 (KOSPI, KOSDAQ)',
    industry VARCHAR(100) NOT NULL COMMENT '업종명',
    rsi_d FLOAT COMMENT '14일 RSI',
    rsi_w FLOAT COMMENT '30일 RSI',
    rsi_m FLOAT COMMENT '90일 RSI',
    reg_date DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '등록일시',

    UNIQUE KEY uq_trade_date_market_industry (trade_date, market_type, industry),
    KEY idx_trade_date (trade_date),
    KEY idx_industry (industry)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='업종별 RSI 일일 요약';
```

- RSI 계산 기간: 14일(단기), 30일(중기), 90일(장기)
- 계산 방식: Wilder's RSI (첫 평균은 SMA, 이후 EMA)

### 3. krx_sector_leaders (섹터별 대장주 추적)
```sql
CREATE TABLE IF NOT EXISTS krx_sector_leaders (
    idx INT AUTO_INCREMENT PRIMARY KEY COMMENT '내부 고유 ID',
    market_type VARCHAR(10) NOT NULL COMMENT '시장구분 (KOSPI, KOSDAQ)',
    industry VARCHAR(100) NOT NULL COMMENT '업종명',
    rank_position TINYINT NOT NULL COMMENT '순위 (1위 또는 2위)',
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
```

- 시가총액 기준 1위, 2위 종목 추적
- 연속 유지 일수 자동 계산 (종목 변경 시 1로 리셋)

## 설치 및 설정

### 1. 필수 요구사항

- **Python 3.8 이상**
- **MySQL 5.7 이상**
- **wkhtmltopdf** (HTML to Image 변환용)

### 2. 패키지 설치

```bash
# 저장소 클론
git clone https://github.com/yourusername/krx-topsector-report.git
cd krx-topsector-report

# 가상환경 생성 (권장)
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt
```

### 3. wkhtmltopdf 설치

wkhtmltopdf는 HTML을 이미지로 변환하는데 필수입니다.

#### Windows
1. https://wkhtmltopdf.org/downloads.html 에서 다운로드
2. 설치 후 실행 파일 경로 확인 (기본값: `C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe`)
3. .env 파일에 경로 설정

#### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install wkhtmltopdf
```

#### macOS
```bash
brew install wkhtmltopdf
```

### 4. 환경변수 설정

프로젝트 루트에 `.env` 파일을 생성하고 다음 정보를 입력하세요:

```env
# 텔레그램 봇 설정
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
TELEGRAM_CHAT_TEST_ID=your_test_chat_id_here

# 데이터베이스 설정
DB_HOST=localhost
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_NAME=your_db_name
DB_PORT=3306

# wkhtmltoimage 경로 (OS별 조정 필요)
# Windows 예: C:\\Program Files\\wkhtmltopdf\\bin\\wkhtmltoimage.exe
# Linux 예: /usr/bin/wkhtmltoimage
# macOS 예: /usr/local/bin/wkhtmltoimage
WKHTMLTOIMAGE_PATH=/usr/local/bin/wkhtmltoimage

# 외부 API 설정 (선택사항)
API_URL=https://your-api-endpoint.com/api/posts
API_TOKEN=your_api_token_here

# 제외할 섹터 (쉼표로 구분)
EXCLUDED_SECTORS=기타
```

### 5. 데이터베이스 초기화

```bash
# 데이터베이스 테이블 자동 생성 및 초기 데이터 수집 (과거 200거래일)
python main.py --init
```

- 테이블이 없으면 자동으로 생성됩니다
- 초기 실행 시 과거 200거래일 데이터를 수집합니다
- 주말 및 공휴일은 자동으로 제외됩니다

## 사용 방법

### 일일 작업 실행

```bash
# 기본 실행 (당일 데이터 수집 및 리포트 생성)
python main.py

# 초기 설정 (과거 200거래일 데이터 수집)
python main.py --init
```

**실행 과정:**
1. KRX API에서 당일 KOSPI/KOSDAQ 전 종목 데이터 수집
2. 365일 이상 된 오래된 데이터 자동 삭제
3. 섹터별 RSI(14/30/90일) 계산 및 저장
4. 섹터별 시가총액 1위, 2위 대장주 업데이트
5. HTML 테이블 리포트를 이미지로 변환
6. 텔레그램으로 리포트 전송
7. 외부 API로 게시글 자동 등록 (설정된 경우)

### 유틸리티 스크립트

```bash
# 대장주 연속 유지 일수를 과거 데이터 기반으로 재계산
python recalculate_consecutive_days.py
```

이 스크립트는 과거 거래일 데이터를 분석하여 각 섹터의 대장주가 실제로 몇 일 연속으로 해당 순위를 유지했는지 계산합니다.

### 개별 모듈 테스트

각 모듈은 독립적으로 실행 가능합니다:

```bash
# 데이터 수집기 테스트
python data_collector.py

# RSI 계산기 테스트
python rsi_calculator.py

# 대장주 추적기 테스트
python sector_leader_tracker.py

# 리포트 생성기 테스트
python table_report_generator.py
```

## RSI 지표 해석

리포트에 적용되는 RSI 색상 코드 및 의미:

| RSI 구간 | 상태 | 배경 색상 | 의미 |
|----------|------|-----------|------|
| 0-19 | 매우 과매도 | 진한 파랑 (#5c88c7) | 강력한 매수 신호 |
| 20-29 | 과매도 | 파랑 (#b2c7e2) | 매수 고려 구간 |
| 30-44 | 약간 과매도 | 연한 파랑 (#e7eef8) | 매수 관심 구간 |
| 45-55 | 중립 | 흰색 (#ffffff) | 보합 구간 |
| 56-70 | 약한 과매수 | 연한 빨강 (#ffdadb) | 매도 관심 구간 |
| 71-80 | 과매수 | 빨강 (#fa9396) | 매도 고려 구간 |
| 81-100 | 매우 과매수 | 진한 빨강 (#fc676b) | 강력한 매도 신호 |

**RSI 계산 방식:**
- Wilder's RSI 방법 사용
- 첫 번째 평균: SMA (Simple Moving Average)
- 이후 평균: EMA (Exponential Moving Average)
- 섹터 RSI: 해당 섹터 전체 종목 RSI의 평균값

## 자동화 설정

### Cron (Linux/macOS)

```bash
# 평일 오후 6시에 자동 실행 (주말 제외)
0 18 * * 1-5 cd /path/to/krx-topsector-report && /path/to/venv/bin/python main.py >> /path/to/logs/cron.log 2>&1
```

### Windows 작업 스케줄러

1. `작업 스케줄러` 실행 (taskschd.msc)
2. `작업 만들기` 선택
3. **트리거 설정**:
   - 매일 오후 6시
   - 반복: 주말 제외 (월~금)
4. **작업 설정**:
   - 프로그램: `C:\path\to\venv\Scripts\python.exe`
   - 인수: `main.py`
   - 시작 위치: `C:\path\to\krx-topsector-report`
5. 조건: "작업을 실행하기 위해 깨우기" 선택 (선택사항)

### Docker (추천)

Docker를 사용하면 환경 일관성을 보장할 수 있습니다:

```dockerfile
FROM python:3.9-slim

# wkhtmltopdf 설치
RUN apt-get update && apt-get install -y wkhtmltopdf

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
```

## 리포트 샘플

생성되는 리포트는 다음 정보를 포함합니다:

- **섹터명**: 업종 분류 (예: 반도체, 화학, 바이오 등)
- **RSI(90)**: 90일 RSI 지표 (장기)
- **RSI(30)**: 30일 RSI 지표 (중기)
- **RSI(14)**: 14일 RSI 지표 (단기)
- **1등주**: 시가총액 1위 종목 및 연속 유지 일수
- **2등주**: 시가총액 2위 종목 및 연속 유지 일수
- **색상 코딩**: RSI 구간별 직관적 시각화

**대장주 표시 방식:**
- 동일 종목 유지: "삼성전자 (15일 연속)" - 빨간색
- 종목 변경: "삼성전자 NEW (이전: SK하이닉스)" - 파란색

## 기술적 특징

### RSI 계산 방식
- **표준 Wilder's RSI**: 첫 번째 평균은 SMA, 이후는 EMA 적용
- **다중 기간 계산**: 14일(단기), 30일(중기), 90일(장기) 동시 계산
- **섹터 집계**: 해당 섹터 전체 종목 RSI의 평균값으로 산출
- **최소 데이터**: 90일 RSI 계산을 위해 최대 120일치 데이터 조회

### 데이터 관리
- **자동 정리**: 365일 이상 된 데이터 자동 삭제 (매일 실행)
- **중복 방지**: UNIQUE KEY 제약 조건으로 데이터 정합성 보장
- **거래일 필터링**: holidays 라이브러리를 사용한 한국 공휴일 자동 제외
- **배치 처리**: 섹터별 병렬 계산으로 성능 최적화

### 이미지 생성
- **HTML to Image**: wkhtmltoimage를 사용한 고품질 이미지 변환
- **Noto Sans KR 폰트**: 한글 가독성 최적화
- **반응형 테이블**: 자동 너비 조정 및 색상 코딩
- **이미지 압축**: API 전송 시 Pillow를 사용한 이미지 최적화

### 에러 핸들링
- **포괄적 로깅**: 일별 로그 파일에 모든 작업 과정 기록
- **예외 처리**: 네트워크, 데이터베이스, API 오류 개별 대응
- **Fallback 메커니즘**: 이미지 생성 실패 시 텍스트 리포트로 대체
- **테스트 채널**: API 오류 발생 시 텔레그램 테스트 채널로 알림

### 외부 API 연동
- **멀티파트 업로드**: 여러 이미지를 하나의 요청으로 전송
- **이미지 압축**: 파일 크기 최적화 (품질: 85%)
- **에러 핸들링**: ApiError 클래스를 통한 상세한 오류 메시지
- **썸네일 지원**: 별도의 썸네일 이미지 지정 가능

## 주요 의존성

```
pandas==2.2.2              # 데이터 처리 및 분석
pykrx==1.0.47              # KRX 데이터 수집 API
requests==2.32.3           # HTTP 요청 및 API 통신
holidays==0.54             # 한국 공휴일 처리
python-dotenv==1.0.1       # 환경변수 관리
imgkit==1.2.3              # HTML to 이미지 변환 (wkhtmltopdf 래퍼)
Pillow>=10.0.0             # 이미지 처리 및 압축
pymysql==1.1.0             # MySQL 데이터베이스 연결
numpy>=1.24.0              # 수치 계산
ta==0.10.2                 # 기술적 분석 지표 (사용하지 않음, 제거 가능)
```

**참고:**
- `ta` 라이브러리는 requirements.txt에 포함되어 있지만 현재 코드에서는 사용하지 않습니다
- RSI 계산은 직접 구현한 함수를 사용합니다 (Wilder's 방식)

## 데이터 흐름

```
1. 데이터 수집 (Data Collection)
   └─> pykrx.website.krx.market.core.업종분류현황()
       └─> KOSPI/KOSDAQ 전 종목 데이터
           └─> krx_stock 테이블에 저장

2. RSI 계산 (RSI Calculation)
   └─> krx_stock 테이블에서 과거 120일 데이터 조회
       └─> 종목별 14/30/90일 RSI 계산
           └─> 섹터별 평균 RSI 계산
               └─> krx_sector_rsi 테이블에 저장

3. 대장주 추적 (Leader Tracking)
   └─> krx_stock 테이블에서 섹터별 시가총액 순위 조회
       └─> 1위, 2위 종목 식별
           └─> 기존 대장주와 비교하여 연속일수 계산
               └─> krx_sector_leaders 테이블에 저장/업데이트

4. 리포트 생성 (Report Generation)
   └─> krx_sector_rsi 및 krx_sector_leaders 테이블 조회
       └─> HTML 테이블 생성 (색상 코딩 적용)
           └─> wkhtmltoimage로 이미지 변환
               └─> img/ 디렉토리에 저장

5. 전송 (Distribution)
   ├─> 텔레그램 봇으로 이미지 전송
   └─> 외부 API로 게시글 등록 (선택사항)
```

## 문제 해결

### wkhtmltoimage 오류
```
Error: WKHTMLTOIMAGE_PATH 환경변수가 설정되지 않았습니다.
```
**해결방법:** .env 파일에 wkhtmltoimage 실행 파일 경로를 설정하세요.

### 데이터베이스 연결 오류
```
DB 연결 오류: (2003, "Can't connect to MySQL server...")
```
**해결방법:**
- MySQL 서버가 실행 중인지 확인
- .env 파일의 DB_HOST, DB_PORT, DB_USER, DB_PASSWORD 확인

### 텔레그램 전송 실패
```
Telegram API error: Unauthorized
```
**해결방법:**
- TELEGRAM_BOT_TOKEN이 올바른지 확인
- 봇이 채팅방에 추가되어 있는지 확인
- TELEGRAM_CHAT_ID가 올바른지 확인

### RSI 계산 데이터 부족
```
업종 RSI 계산 불가 - 유효한 RSI 데이터 없음
```
**해결방법:**
- `python main.py --init`으로 과거 데이터를 충분히 수집
- 최소 90일 이상의 거래일 데이터 필요

## 라이선스

이 프로젝트는 개인 및 상업적 용도로 자유롭게 사용 가능합니다.

## 기여

버그 리포트, 기능 제안, Pull Request를 환영합니다.

## 문의

프로젝트 관련 문의사항은 이슈 트래커를 통해 남겨주세요.