from __future__ import annotations

"""
Reference-only API adapter.

- 실제 테스트 실행은 run_test_mode.py 를 기준으로 합니다.
- API/배포 구축 관련 런타임 설정은 의도적으로 blur 처리했습니다.
"""

import re
import tempfile
import uuid
from pathlib import Path

from das_runtime import BLURRED_API_BUILD_CONFIG

try:
    from fastapi import BackgroundTasks, FastAPI, Form, HTTPException
    from fastapi.responses import FileResponse, HTMLResponse
except ImportError:  # pragma: no cover - optional runtime dependency
    BackgroundTasks = FastAPI = Form = HTTPException = None
    FileResponse = HTMLResponse = None
    app = None
else:
    app = FastAPI(title="DAS Clustering Runner")

    RUNS_ROOT = Path(tempfile.gettempdir()) / "das_clustering_runs"
    RUNS_ROOT.mkdir(parents=True, exist_ok=True)

    def _error_page(msg: str, status_code: int = 400) -> HTMLResponse:
        return HTMLResponse(
            f"""
            <html>
              <head><meta charset=\"utf-8\"><title>실행 실패</title></head>
              <body style=\"font-family: Arial; margin: 30px;\">
                <h3 style=\"color:#b00020;\">실행 실패</h3>
                <p>{msg}</p>
                <a href=\"/\">뒤로가기</a>
              </body>
            </html>
            """,
            status_code=status_code,
        )

    def _validate_yyyymmdd(s: str) -> str:
        s = str(s).strip()
        if not re.fullmatch(r"\d{8}", s):
            raise ValueError("date must be YYYYMMDD")
        return s

    def _validate_cent(s: str) -> str:
        s = str(s).strip()
        if not s:
            raise ValueError("cent is required")
        return s

    def _cleanup_path(p: Path) -> None:
        try:
            if p.exists():
                p.unlink()
        except Exception:
            pass

        try:
            d = p.parent
            if d.exists():
                d.rmdir()
        except Exception:
            pass

    @app.get("/", response_class=HTMLResponse)
    def index():
        return f"""
        <html>
          <head><meta charset=\"utf-8\"><title>DAS 클러스터링 API(참고용)</title></head>
          <body style=\"font-family: Arial; margin: 30px;\">
            <h2>DAS 클러스터링 API 참고 코드</h2>
            <p>실제 테스트 실행은 <code>run_test_mode.py</code> 를 사용하세요.</p>
            <p>API 구축 설정: <code>{BLURRED_API_BUILD_CONFIG}</code></p>
          </body>
        </html>
        """

    @app.post("/run")
    def run_job(
        background_tasks: BackgroundTasks,
        date: str = Form(...),
        cent: str = Form(...),
        CLASS_LV: str = Form(...),
        group_min: int = Form(...),
        max_SKU: int = Form(...),
        Das_unit: int = Form(...),
    ):
        from Das_Clustering_repair import (
            NoCentRowsAfterSummarizeError,
            NoSqlRowsError,
            main as run_main,
        )

        try:
            date = _validate_yyyymmdd(date)
            cent = _validate_cent(cent)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

        job_id = uuid.uuid4().hex
        job_dir = RUNS_ROOT / job_id
        job_dir.mkdir(parents=True, exist_ok=True)
        out_path = job_dir / f"[{CLASS_LV}]클러스터링 결과_{cent}_{date}_{job_id}.xlsx"

        try:
            out_path = Path(
                run_main(
                    date=date,
                    cent=cent,
                    CLASS_LV=CLASS_LV,
                    group_min=int(group_min),
                    max_SKU=int(max_SKU),
                    Das_unit=int(Das_unit),
                    out_xlsx=out_path,
                    test_mode=False,
                )
            )
        except NoSqlRowsError:
            return _error_page(
                f"조회 결과가 없습니다. 작업일자({date}) 또는 센터코드({cent})를 수정/확인해 주세요.",
                status_code=400,
            )
        except NoCentRowsAfterSummarizeError:
            return _error_page(
                f"해당 센터코드({cent})에 대한 작업 내역이 없습니다. 센터코드를 수정/확인해 주세요.",
                status_code=400,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"실행 중 오류: {e}")

        if not out_path.exists():
            raise HTTPException(status_code=500, detail="결과 파일이 생성되지 않았습니다.")

        background_tasks.add_task(_cleanup_path, out_path)
        return FileResponse(
            path=str(out_path),
            filename=out_path.name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
