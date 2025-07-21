# 🏢 KRX Top Sector Report

> 한국거래소(KRX) 섹터별 주식 데이터 자동 수집 및 RSI 지표 분석 시스템

## 📋 프로젝트 개요

KRX Top Sector Report는 한국거래소의 모든 상장 종목을 섹터별로 분석하여 RSI(Relative Strength Index) 지표와 대장주 정보를 자동으로 수집·분석하고, 시각적 리포트를 텔레그램으로 전송하는 자동화 시스템입니다.

### ✨ 주요 기능

- 📊 **실시간 데이터 수집**: pykrx 라이브러리를 통한 KOSPI/KOSDAQ 전 종목 데이터 자동 수집
- 📈 **RSI 지표 계산**: 섹터별 14일/30일/90일 RSI 지표 계산 (Wilder's 표준 방식)
- 🏆 **대장주 추적**: 시가총액 기준 섹터별 1, 2위 종목 추적 및 연속 유지 일수 계산
- 🎨 **시각적 리포트**: RSI 구간별 색상 코딩이 적용된 HTML 테이블 리포트 생성
- 🤖 **텔레그램 자동 전송**: 생성된 리포트를 텔레그램 봇을 통해 자동 전송
- 🔄 **완전 자동화**: 스케줄링 가능한 데이터 수집부터 리포트 전송까지 원클릭 자동화

## 🏗️ 시스템 아키텍처

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   KRX API       │───▶│  Data Collector  │───▶│   MySQL DB      │
│   (pykrx)       │    │                  │    │   (3 Tables)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Telegram Bot   │◀───│  Report Generator │◀───│  RSI Calculator │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## 📁 프로젝트 구조

```
krx-topsector-report/
├── 📄 main.py                          # 메인 실행 파일
├── 📊 data_collector.py                # KRX 데이터 수집기
├── 📈 rsi_calculator.py                # RSI 지표 계산기  
├── 🏆 sector_leader_tracker.py         # 섹터 대장주 추적기
├── 📋 table_report_generator.py        # 테이블 리포트 생성기
├── 🔧 recalculate_consecutive_days.py  # 연속일수 재계산 유틸리티
├── 📋 requirements.txt                 # 의존성 패키지 목록
├── ⚙️ .env                             # 환경변수 설정
├── 🛠️ utils/                           # 유틸리티 모듈
│   ├── 🗄️ db_manager.py                # 데이터베이스 관리
│   ├── 📝 logger_util.py               # 로깅 시스템
│   ├── 💬 telegram_util.py             # 텔레그램 통신
│   └── 🌐 api_util.py                  # 외부 API 통신
├── 📂 logs/                            # 로그 파일 저장소
└── 🖼️ img/                             # 생성된 리포트 이미지
```

## 🗄️ 데이터베이스 스키마

### 1. krx_stock (일별 주가 데이터)
```sql
CREATE TABLE krx_stock (
    idx INT AUTO_INCREMENT PRIMARY KEY,
    stock_code VARCHAR(10) NOT NULL,        -- 종목코드
    stock_name VARCHAR(100) NOT NULL,       -- 종목명
    market_type VARCHAR(10) NOT NULL,       -- 시장구분 (KOSPI/KOSDAQ)
    industry VARCHAR(100),                  -- 업종명
    trade_date DATE NOT NULL,               -- 거래일
    close_price FLOAT NOT NULL,             -- 종가
    change_amount FLOAT,                    -- 전일대비 금액
    change_rate FLOAT,                      -- 등락률 (%)
    market_cap BIGINT,                      -- 시가총액
    reg_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE KEY uq_stock_trade_date (stock_code, trade_date)
);
```

### 2. krx_sector_rsi (섹터별 RSI 지표)
```sql
CREATE TABLE krx_sector_rsi (
    idx INT AUTO_INCREMENT PRIMARY KEY,
    trade_date DATE NOT NULL,               -- 기준일
    market_type VARCHAR(10) NOT NULL,       -- 시장구분
    industry VARCHAR(100) NOT NULL,         -- 업종명
    rsi_d FLOAT,                           -- 14일 RSI
    rsi_w FLOAT,                           -- 30일 RSI  
    rsi_m FLOAT,                           -- 90일 RSI
    reg_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    PRIMARY KEY (trade_date, market_type, industry)
);
```

### 3. krx_sector_leaders (섹터별 대장주 추적)
```sql
CREATE TABLE krx_sector_leaders (
    idx INT AUTO_INCREMENT PRIMARY KEY,
    market_type VARCHAR(10) NOT NULL,       -- 시장구분
    industry VARCHAR(100) NOT NULL,         -- 업종명
    rank_position TINYINT NOT NULL,         -- 순위 (1위/2위)
    stock_code VARCHAR(10) NOT NULL,        -- 종목코드
    stock_name VARCHAR(100) NOT NULL,       -- 종목명
    market_cap BIGINT NOT NULL,             -- 시가총액
    consecutive_days INT DEFAULT 1,         -- 연속 유지 일수
    reg_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_date DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    UNIQUE KEY uq_market_industry_rank (market_type, industry, rank_position)
);
```

## 🚀 설치 및 설정

### 1. 필수 요구사항

- **Python 3.8+**
- **MySQL 5.7+**
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

#### Windows
```bash
# https://wkhtmltopdf.org/downloads.html 에서 다운로드
# 기본 설치 경로: C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe
```

#### Ubuntu/Debian
```bash
sudo apt-get install wkhtmltopdf
```

#### macOS
```bash
brew install wkhtmltopdf
```

### 4. 환경변수 설정

`.env` 파일을 생성하고 다음 정보를 입력하세요:

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
WKHTMLTOIMAGE_PATH=/usr/local/bin/wkhtmltoimage

# 제외할 섹터 (쉼표로 구분)
EXCLUDED_SECTORS=기타
```

### 5. 데이터베이스 초기화

```bash
# 데이터베이스 테이블 생성 및 초기 데이터 수집 (과거 200일)
python main.py --init
```

## 🏃‍♂️ 사용 방법

### 일일 데이터 수집 및 리포트 생성

```bash
# 기본 실행 (당일 데이터)
python main.py

# 특정 날짜 데이터 수집
python main.py --date 20240315

# 초기 설정 (과거 200일 데이터 수집)
python main.py --init
```

### 연속일수 재계산 (선택사항)

```bash
# 대장주 연속 유지 일수를 과거 데이터 기반으로 재계산
python recalculate_consecutive_days.py
```

### 개별 모듈 테스트

```bash
# RSI 계산기 테스트
python rsi_calculator.py

# 대장주 추적기 테스트  
python sector_leader_tracker.py

# 리포트 생성기 테스트
python table_report_generator.py
```

## 📊 RSI 지표 해석

| RSI 구간 | 상태 | 색상 코드 | 의미 |
|----------|------|-----------|------|
| 0-19 | 매우 과매도 | `#5c88c7` | 강력한 매수 신호 |
| 20-29 | 과매도 | `#b2c7e2` | 매수 고려 구간 |
| 30-44 | 약간 과매도 | `#e7eef8` | 매수 관심 구간 |
| 45-55 | 중립 | `#ffffff` | 보합 구간 |
| 56-70 | 약한 과매수 | `#ffdadb` | 매도 관심 구간 |
| 71-80 | 과매수 | `#fa9396` | 매도 고려 구간 |
| 81-100 | 매우 과매수 | `#fc676b` | 강력한 매도 신호 |

## 🤖 자동화 설정

### Cron (Linux/macOS)

```bash
# 평일 오후 6시에 자동 실행
0 18 * * 1-5 cd /path/to/krx-topsector-report && python main.py
```

### Windows 작업 스케줄러

1. `작업 스케줄러` 실행
2. `작업 만들기` 선택
3. 트리거: 매일 오후 6시, 주말 제외
4. 작업: `python.exe`, 인수: `main.py`
5. 시작 위치: 프로젝트 폴더 경로

## 📈 리포트 샘플

생성되는 리포트는 다음 정보를 포함합니다:

- **섹터명**: 업종 분류
- **RSI(M/W/D)**: 90일/30일/14일 RSI 지표
- **시총 1위/2위**: 시가총액 기준 대장주 및 연속 유지 일수
- **색상 코딩**: RSI 구간별 직관적 시각화

## 🔧 기술적 특징

### RSI 계산 방식
- **표준 Wilder's RSI**: 첫 번째 평균은 SMA, 이후는 EMA 적용
- **다중 기간**: 14일(단기), 30일(중기), 90일(장기) 동시 계산
- **섹터별 집계**: 개별 종목 RSI의 가중평균으로 섹터 RSI 산출

### 데이터 관리
- **자동 정리**: 365일 이상 된 데이터 자동 삭제
- **중복 방지**: UNIQUE 제약 조건으로 데이터 정합성 보장
- **배치 처리**: 대용량 데이터 효율적 처리

### 에러 핸들링
- **포괄적 로깅**: 모든 작업 과정 상세 로깅
- **예외 처리**: 네트워크, 데이터베이스 오류 대응
- **복구 메커니즘**: 실패한 작업 재시도 로직

## 📦 주요 의존성

```
pandas==2.2.2              # 데이터 처리 및 분석
pykrx==1.0.47              # KRX 데이터 수집 API
requests==2.32.3           # HTTP 요청
holidays==0.54             # 한국 공휴일 처리
python-dotenv==1.0.1       # 환경변수 관리
imgkit==1.2.3              # HTML to 이미지 변환
Pillow>=10.0.0             # 이미지 처리
pymysql==1.1.0             # MySQL 연결
numpy>=1.24.0              # 수치 계산
ta==0.10.2                 # 기술적 분석 (RSI 등)
```
---