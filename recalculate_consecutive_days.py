#!/usr/bin/env python3
"""
krx_sector_leaders 테이블의 연속일수를 과거 데이터 기반으로 재계산하는 스크립트
"""

import sys
import os
from datetime import datetime

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from sector_leader_tracker import SectorLeaderTracker
from utils.db_manager import get_db_connection
from utils.logger_util import LoggerUtil

def main():
    """연속일수 재계산 메인 함수"""
    logger = LoggerUtil().get_logger()
    tracker = SectorLeaderTracker()
    
    logger.info("=== krx_sector_leaders 연속일수 재계산 시작 ===")
    
    # DB 연결
    conn = get_db_connection()
    if not conn:
        logger.error("데이터베이스 연결 실패")
        return False
    
    try:
        # 재계산 전 현재 상태 확인
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM krx_sector_leaders")
            total_records = cursor.fetchone()['count']
            
            cursor.execute("SELECT COUNT(*) as count FROM krx_sector_leaders WHERE consecutive_days = 1")
            one_day_records = cursor.fetchone()['count']
            
            logger.info(f"재계산 전 상태: 전체 {total_records}개 레코드 중 {one_day_records}개가 1일")
        
        # 연속일수 재계산 실행
        updated_count = tracker.recalculate_all_consecutive_days(conn)
        
        # 재계산 후 상태 확인
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) as count FROM krx_sector_leaders WHERE consecutive_days = 1")
            one_day_records_after = cursor.fetchone()['count']
            
            cursor.execute("""
                SELECT AVG(consecutive_days) as avg_days, 
                       MIN(consecutive_days) as min_days, 
                       MAX(consecutive_days) as max_days
                FROM krx_sector_leaders
            """)
            stats = cursor.fetchone()
            
            logger.info(f"재계산 후 상태: {one_day_records_after}개가 1일, 평균 {stats['avg_days']:.2f}일, 범위 {stats['min_days']}~{stats['max_days']}일")
        
        # 샘플 결과 출력
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT market_type, industry, rank_position, stock_name, consecutive_days
                FROM krx_sector_leaders
                WHERE consecutive_days > 5
                ORDER BY consecutive_days DESC
                LIMIT 10
            """)
            top_streaks = cursor.fetchall()
            
            if top_streaks:
                logger.info("연속일수 상위 10개:")
                for i, record in enumerate(top_streaks, 1):
                    logger.info(f"  {i}. {record['market_type']} {record['industry']} {record['rank_position']}위 "
                              f"{record['stock_name']} - {record['consecutive_days']}일 연속")
        
        logger.info(f"=== 연속일수 재계산 완료 - {updated_count}개 레코드 업데이트 ===")
        return True
        
    except Exception as e:
        logger.error(f"연속일수 재계산 실패: {e}")
        return False
        
    finally:
        conn.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)