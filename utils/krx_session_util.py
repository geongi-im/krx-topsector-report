"""KRX Data Marketplace 계정을 pykrx 에 연결하는 유틸.

pykrx 1.2.8 은 환경변수 KRX_ID / KRX_PW 를 읽어 스스로 로그인하고, 세션이
만료되면 자동으로 재로그인한다 (pykrx.website.comm.auth). 데이터 조회
엔드포인트도 https 로 바뀌어 있어, 로그인 세션 쿠키(JSESSIONID, Secure)가
정상적으로 실려 나간다.

이 프로젝트의 .env 는 계정을 KRX_LOGIN_ID / KRX_LOGIN_PASSWORD 라는 이름으로
두므로, pykrx 가 읽는 이름으로 옮겨준 뒤 pykrx 를 import 해야 한다. import
시점에 로그인이 수행되기 때문에 순서가 중요하다. main.py 에서 reports.* 보다
먼저 이 모듈을 import 하는 이유다.

pykrx 는 로그인 진행 상황을 stdout 으로 출력하므로, import 시점 출력은
가로채서 보관해 두었다가 실패했을 때 에러 메시지에 붙여준다.
"""

from __future__ import annotations

import contextlib
import io
import os
from typing import Optional

from dotenv import load_dotenv

# pykrx import 이전에 .env 를 읽어 계정을 환경변수로 올린다.
load_dotenv()


def _export_credentials() -> tuple[Optional[str], Optional[str]]:
    """.env 의 KRX_LOGIN_* 값을 pykrx 가 읽는 KRX_ID / KRX_PW 로 옮긴다."""
    login_id = os.getenv("KRX_LOGIN_ID") or os.getenv("KRX_ID")
    password = os.getenv("KRX_LOGIN_PASSWORD") or os.getenv("KRX_PW")
    if login_id:
        os.environ["KRX_ID"] = login_id
    if password:
        os.environ["KRX_PW"] = password
    return login_id, password


_export_credentials()

# import 시점에 pykrx 가 로그인을 시도한다. 출력은 붙잡아 둔다.
_import_log = io.StringIO()
with contextlib.redirect_stdout(_import_log):
    from pykrx.website.comm import auth as _auth


class KrxSessionError(RuntimeError):
    """KRX 로그인/세션 준비 실패"""


def install_krx_session(
    login_id: Optional[str] = None,
    password: Optional[str] = None,
    force: bool = False,
):
    """pykrx 가 사용할 KRX 로그인 세션을 확보한다.

    Args:
        login_id: KRX Data Marketplace 계정 ID. 생략 시 KRX_LOGIN_ID 환경변수.
        password: 계정 비밀번호. 생략 시 KRX_LOGIN_PASSWORD 환경변수.
        force: True 이면 이미 로그인된 세션이 있어도 다시 로그인.

    Returns:
        pykrx 의 KRXSession (로그인 완료 상태)

    Raises:
        KrxSessionError: 자격 증명 누락 또는 KRX 로그인 실패 시.
    """
    if login_id:
        os.environ["KRX_ID"] = login_id
    if password:
        os.environ["KRX_PW"] = password

    env_id, env_pw = _export_credentials()
    if not env_id or not env_pw:
        raise KrxSessionError(
            "KRX_LOGIN_ID / KRX_LOGIN_PASSWORD 환경변수가 필요합니다."
        )

    log = io.StringIO()
    with contextlib.redirect_stdout(log):
        session = _auth.get_auth_session()
        if session is not None and force:
            if not session.refresh(env_id, env_pw):
                session = None

    if session is None or not session.is_authenticated:
        detail = (_import_log.getvalue() + log.getvalue()).strip()
        detail = detail.replace("\n", " / ") or "원인 불명"
        raise KrxSessionError(f"KRX 로그인 실패: {detail}")

    return session
