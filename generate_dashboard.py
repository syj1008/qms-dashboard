"""
QMS 대시보드 자동 생성 스크립트
- Snowflake에서 E-D-I-C-A 체인 데이터를 쿼리하여 HTML 생성
- GitHub Actions에서 매일 06:00 KST (21:00 UTC) 실행
"""

import snowflake.connector
import json
import re
import os
from datetime import datetime, timezone, timedelta

# ─────────────────────────────────────────
# Snowflake 연결 설정 (환경변수에서 읽음)
# ─────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]    # e.g. aa66473.ap-northeast-2.aws
SNOWFLAKE_USER      = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ROLE      = os.environ.get("SNOWFLAKE_ROLE", "APQR_DEV_ROLE")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "GCBP_WH")
SNOWFLAKE_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE", "GCBP_DB")

KST = timezone(timedelta(hours=9))
TODAY = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")

QUERY = """
WITH
event_base AS (
  SELECT DISTINCT DOCUMENT_NUM AS event_num,
         REPLACE(TITLE_NM, DOCUMENT_NUM || ': ', '') AS event_title,
         DOC_STATUS AS event_status,
         LEFT(CREATE_DATE, 10) AS event_create,
         LEFT(RELEASE_DATE, 10) AS event_release
  FROM GCBP_DB.L0.L0_QMS_V_QMS_INFOCARD_ALL
  WHERE DOCUMENT_NUM LIKE 'EVENT-HSP-%'
    AND YEAR(TRY_TO_TIMESTAMP(CREATE_DATE)) >= 2024
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DOCUMENT_NUM ORDER BY LOAD_DT DESC) = 1
),
dev_info AS (
  SELECT a.DOCUMENT_NUM AS dev_num, a.DOC_STATUS AS dev_status,
         LEFT(a.CREATE_DATE, 10) AS dev_create,
         LEFT(a.RELEASE_DATE, 10) AS dev_release,
         l.FIELD_DATA AS event_num
  FROM GCBP_DB.L0.L0_QMS_V_QMS_INFOCARD_ALL a
  JOIN (
    SELECT DOCUMENT_NUM, FIELD_DATA FROM GCBP_DB.L0.L0_QMS_V_QMS_INFOCARD_ALL
    WHERE FIELD_LABEL IN ('Help Parent Form Id', 'T1 출처 번호') AND FIELD_DATA LIKE 'EVENT-HSP-%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DOCUMENT_NUM ORDER BY LOAD_DT DESC) = 1
  ) l ON l.DOCUMENT_NUM = a.DOCUMENT_NUM
  WHERE a.DOCUMENT_NUM LIKE 'DEV-HSP-%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.DOCUMENT_NUM ORDER BY a.LOAD_DT DESC) = 1
),
issue_info AS (
  SELECT a.DOCUMENT_NUM AS issue_num, a.DOC_STATUS AS issue_status,
         LEFT(a.CREATE_DATE, 10) AS issue_create,
         LEFT(a.RELEASE_DATE, 10) AS issue_release, l.FIELD_DATA AS dev_num
  FROM GCBP_DB.L0.L0_QMS_V_QMS_INFOCARD_ALL a
  JOIN (
    SELECT DOCUMENT_NUM, FIELD_DATA FROM GCBP_DB.L0.L0_QMS_V_QMS_INFOCARD_ALL
    WHERE FIELD_LABEL = 'T2 출처 관리번호' AND FIELD_DATA LIKE 'DEV-HSP-%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DOCUMENT_NUM ORDER BY LOAD_DT DESC) = 1
  ) l ON l.DOCUMENT_NUM = a.DOCUMENT_NUM
  WHERE a.DOCUMENT_NUM LIKE 'ISSUE-HSP-%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.DOCUMENT_NUM ORDER BY a.LOAD_DT DESC) = 1
),
capa_base AS (
  SELECT a.DOCUMENT_NUM AS capa_num, a.DOC_STATUS AS capa_status,
         LEFT(a.CREATE_DATE, 10) AS capa_create,
         LEFT(a.RELEASE_DATE, 10) AS capa_release,
         a.CREATOR_DEPARTMENT AS capa_dept, l.FIELD_DATA AS issue_num
  FROM GCBP_DB.L0.L0_QMS_V_QMS_INFOCARD_ALL a
  JOIN (
    SELECT DOCUMENT_NUM, FIELD_DATA FROM GCBP_DB.L0.L0_QMS_V_QMS_INFOCARD_ALL
    WHERE FIELD_LABEL = 'H_CAPA 사전 검토 관리번호' AND FIELD_DATA LIKE 'ISSUE-HSP-%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DOCUMENT_NUM ORDER BY LOAD_DT DESC) = 1
  ) l ON l.DOCUMENT_NUM = a.DOCUMENT_NUM
  WHERE a.DOCUMENT_NUM LIKE 'CAPA-HSP-%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.DOCUMENT_NUM ORDER BY a.LOAD_DT DESC) = 1
),
capa_action_fields AS (
  SELECT DOCUMENT_NUM AS capa_num, FIELD_LABEL, FIELD_DATA
  FROM GCBP_DB.L0.L0_QMS_V_QMS_INFOCARD_ALL
  WHERE DOCUMENT_NUM LIKE 'CAPA-HSP-%'
    AND FIELD_LABEL IN (
      'T9 Action Item 1','T9 Action Item 2','T9 Action Item 3','T9 Action Item 4','T9 Action Item 5',
      'T9 Task Date Due 1','T9 Task Date Due 2','T9 Task Date Due 3','T9 Task Date Due 4','T9 Task Date Due 5',
      'T9 Task Completion Date 1','T9 Task Completion Date 2','T9 Task Completion Date 3',
      'T9 Task Completion Date 4','T9 Task Completion Date 5',
      'T9 Task Description 1','T9 Task Description 2','T9 Task Description 3',
      'T9 Task Description 4','T9 Task Description 5',
      'T7 시정업무담당자1','T7 시정업무담당자2','T7 시정업무담당자3',
      'T7 시정업무담당자4','T7 시정업무담당자5'
    )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DOCUMENT_NUM, FIELD_LABEL ORDER BY LOAD_DT DESC) = 1
),
capa_actions AS (
  SELECT capa_num, v.idx,
    MAX(CASE WHEN FIELD_LABEL = 'T9 Action Item ' || v.idx THEN FIELD_DATA END) AS action_num,
    MAX(CASE WHEN FIELD_LABEL = 'T9 Task Date Due ' || v.idx THEN FIELD_DATA END) AS due_date,
    MAX(CASE WHEN FIELD_LABEL = 'T9 Task Completion Date ' || v.idx THEN FIELD_DATA END) AS completion_date,
    MAX(CASE WHEN FIELD_LABEL = 'T9 Task Description ' || v.idx THEN FIELD_DATA END) AS description,
    MAX(CASE WHEN FIELD_LABEL = 'T7 시정업무담당자' || v.idx THEN FIELD_DATA END) AS assignee_raw
  FROM capa_action_fields
  CROSS JOIN (SELECT '1' AS idx UNION ALL SELECT '2' UNION ALL SELECT '3'
              UNION ALL SELECT '4' UNION ALL SELECT '5') v
  GROUP BY capa_num, v.idx
  HAVING MAX(CASE WHEN FIELD_LABEL = 'T9 Action Item ' || v.idx THEN FIELD_DATA END) IS NOT NULL
     AND MAX(CASE WHEN FIELD_LABEL = 'T9 Action Item ' || v.idx THEN FIELD_DATA END) LIKE 'ACTION-%'
),
action_status AS (
  SELECT DISTINCT DOCUMENT_NUM AS action_num, DOC_STATUS AS action_status
  FROM GCBP_DB.L0.L0_QMS_V_QMS_INFOCARD_STEP
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DOCUMENT_NUM ORDER BY LOAD_DT DESC) = 1
)
SELECT
  e.event_num, e.event_status, e.event_create, e.event_release,
  LEFT(e.event_title, 80) AS event_title,
  d.dev_num, d.dev_status, d.dev_create, d.dev_release,
  i.issue_num, i.issue_status, i.issue_create, i.issue_release,
  c.capa_num, c.capa_status, c.capa_create, c.capa_release, c.capa_dept,
  ca.action_num, ca.idx AS action_idx,
  LEFT(ca.description, 100) AS action_description,
  ca.due_date AS action_due_date,
  ca.completion_date AS action_completion_date,
  ca.assignee_raw,
  ac.action_status,
  CASE
    WHEN ac.action_status = 'Release' THEN '완료'
    WHEN ca.due_date IS NOT NULL AND ca.due_date NOT IN ('None','')
         AND TRY_TO_DATE(ca.due_date) < CURRENT_DATE() THEN '기한초과'
    WHEN ca.action_num IS NOT NULL THEN '진행중'
    ELSE NULL
  END AS action_progress
FROM event_base e
LEFT JOIN dev_info d ON d.event_num = e.event_num
LEFT JOIN issue_info i ON i.dev_num = d.dev_num
LEFT JOIN capa_base c ON c.issue_num = i.issue_num
LEFT JOIN capa_actions ca ON ca.capa_num = c.capa_num
LEFT JOIN action_status ac ON ac.action_num = ca.action_num
WHERE d.dev_num IS NOT NULL
ORDER BY e.event_create DESC, c.capa_num, ca.idx::INT
LIMIT 500
"""


def fetch_data():
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
    )
    try:
        cur = conn.cursor()
        cur.execute(QUERY)
        columns = [desc[0].lower() for desc in cur.description]
        rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def clean(v):
    if v is None or str(v).strip() in ("None", ""):
        return ""
    return str(v).strip()


def parse_assignee(raw):
    raw = clean(raw)
    if not raw:
        return {"name": "", "id": ""}
    m = re.match(r"^(.+?)\s*\(([^)]+)\)\s*$", raw)
    if m:
        return {"name": m.group(1).strip(), "id": m.group(2).strip()}
    return {"name": raw, "id": ""}


def build_records(rows):
    records = []
    for r in rows:
        a = parse_assignee(r.get("assignee_raw"))
        records.append({
            "event_num":      clean(r.get("event_num")),
            "event_status":   clean(r.get("event_status")),
            "event_create":   clean(r.get("event_create")),
            "event_release":  clean(r.get("event_release")),
            "event_title":    clean(r.get("event_title")),
            "dev_num":        clean(r.get("dev_num")),
            "dev_status":     clean(r.get("dev_status")),
            "dev_create":     clean(r.get("dev_create")),
            "dev_release":    clean(r.get("dev_release")),
            "issue_num":      clean(r.get("issue_num")),
            "issue_status":   clean(r.get("issue_status")),
            "issue_create":   clean(r.get("issue_create")),
            "issue_release":  clean(r.get("issue_release")),
            "capa_num":       clean(r.get("capa_num")),
            "capa_status":    clean(r.get("capa_status")),
            "capa_create":    clean(r.get("capa_create")),
            "capa_release":   clean(r.get("capa_release")),
            "capa_dept":      clean(r.get("capa_dept")),
            "action_num":     clean(r.get("action_num")),
            "action_idx":     clean(r.get("action_idx")),
            "action_desc":    clean(r.get("action_description")),
            "action_due":     clean(r.get("action_due_date")),
            "action_complete":clean(r.get("action_completion_date")),
            "action_status":  clean(r.get("action_status")),
            "action_progress":clean(r.get("action_progress")),
            "assignee_name":  a["name"],
            "assignee_id":    a["id"],
        })
    return records


def compute_stats(records):
    done  = sum(1 for r in records if r["action_progress"] == "완료")
    over  = sum(1 for r in records if r["action_progress"] == "기한초과")
    prog  = sum(1 for r in records if r["action_progress"] == "진행중")
    total_events = len({r["event_num"] for r in records if r["event_num"]})
    total_capas  = len({r["capa_num"]  for r in records if r["capa_num"]})
    return dict(done=done, over=over, prog=prog,
                total_events=total_events, total_capas=total_capas)


# ─────────────────────────────────────────
# HTML 템플릿 (기존 대시보드와 동일)
# ─────────────────────────────────────────
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>QMS HSP 진행 현황 | E-D-I-C-A 체인 관리</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Malgun Gothic','Segoe UI',sans-serif;background:#f0f4f8;color:#1e293b;font-size:14px}}
header{{background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 60%,#0369a1 100%);color:#fff;padding:18px 28px;display:flex;justify-content:space-between;align-items:center}}
header h1{{font-size:1.35rem;font-weight:800;letter-spacing:0.3px}}
header .subtitle{{font-size:0.75rem;opacity:.65;margin-top:2px}}
.hdr-right{{font-size:0.72rem;opacity:.55;text-align:right;line-height:1.8}}
.wrap{{max-width:1500px;margin:0 auto;padding:18px 20px}}
.cards{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:18px}}
.card{{background:#fff;border-radius:10px;padding:16px 18px;box-shadow:0 1px 6px rgba(0,0,0,.08);border-top:3px solid transparent}}
.card.blue{{border-color:#0369a1}}.card.green{{border-color:#16a34a}}
.card.orange{{border-color:#ea580c}}.card.red{{border-color:#dc2626}}.card.gray{{border-color:#94a3b8}}
.card-label{{font-size:.7rem;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.4px;margin-bottom:6px}}
.card-val{{font-size:1.9rem;font-weight:900}}
.card.blue .card-val{{color:#0369a1}}.card.green .card-val{{color:#16a34a}}
.card.orange .card-val{{color:#ea580c}}.card.red .card-val{{color:#dc2626}}.card.gray .card-val{{color:#64748b}}
.card-sub{{font-size:.7rem;color:#94a3b8;margin-top:2px}}
.charts-row{{display:grid;grid-template-columns:1fr 2fr;gap:12px;margin-bottom:18px}}
.chart-card{{background:#fff;border-radius:10px;padding:16px 18px;box-shadow:0 1px 6px rgba(0,0,0,.08)}}
.chart-card h3{{font-size:.8rem;font-weight:700;color:#475569;margin-bottom:12px}}
.chart-wrap{{position:relative;height:200px}}
.filter-bar{{background:#fff;border-radius:10px;padding:14px 18px;margin-bottom:14px;box-shadow:0 1px 6px rgba(0,0,0,.08);display:flex;gap:10px;flex-wrap:wrap;align-items:center}}
.filter-bar select,.filter-bar input{{padding:7px 10px;border:1px solid #e2e8f0;border-radius:7px;font-size:.82rem;outline:none;transition:border-color .15s;background:#fff}}
.filter-bar select:focus,.filter-bar input:focus{{border-color:#0369a1}}
.filter-bar input{{flex:1;min-width:180px}}
.fg{{display:flex;align-items:center;gap:5px}}
.fg label{{font-size:.72rem;font-weight:700;color:#64748b;white-space:nowrap}}
.result-count{{margin-left:auto;font-size:.78rem;color:#64748b;white-space:nowrap;font-weight:600}}
.clear-btn{{padding:7px 12px;border:1px solid #e2e8f0;border-radius:7px;background:#fff;font-size:.8rem;cursor:pointer;color:#64748b}}
.legend{{display:flex;gap:16px;margin-bottom:10px;flex-wrap:wrap;padding:0 2px}}
.legend-item{{display:flex;align-items:center;gap:5px;font-size:.72rem;font-weight:600;color:#475569}}
.dot{{width:10px;height:10px;border-radius:2px;display:inline-block}}
.table-wrap{{background:#fff;border-radius:10px;box-shadow:0 1px 6px rgba(0,0,0,.08);overflow:hidden}}
.tbl-scroll{{overflow-x:auto;max-height:580px;overflow-y:auto}}
table{{width:100%;border-collapse:collapse;font-size:.8rem}}
thead th{{background:#0f172a;color:#fff;padding:10px 12px;text-align:left;font-size:.72rem;font-weight:600;letter-spacing:.3px;position:sticky;top:0;z-index:2;cursor:pointer;white-space:nowrap;user-select:none}}
thead th:hover{{background:#1e3a5f}}
tbody tr{{border-bottom:1px solid #f1f5f9;transition:background .1s}}
tbody tr:hover{{background:#f8fafc}}
tbody tr.overdue{{background:#fff5f5}}
tbody tr.overdue:hover{{background:#fee2e2}}
td{{padding:9px 12px;vertical-align:middle}}
.chain-row{{display:flex;align-items:center;gap:4px;flex-wrap:nowrap;font-size:.72rem}}
.chain-badge{{padding:2px 6px;border-radius:4px;font-weight:700;font-size:.68rem;white-space:nowrap}}
.c-event{{background:#dbeafe;color:#1d4ed8}}.c-dev{{background:#f3e8ff;color:#7c3aed}}
.c-issue{{background:#dcfce7;color:#15803d}}.c-capa{{background:#fef9c3;color:#854d0e}}
.arrow{{color:#94a3b8;font-size:.75rem}}
.status-dot{{width:7px;height:7px;border-radius:50%;display:inline-block;margin-right:3px;flex-shrink:0}}
.s-Release{{background:#16a34a}}.s-Draft{{background:#f59e0b}}.s-Abort{{background:#ef4444}}
.prog-badge{{padding:3px 10px;border-radius:12px;font-size:.72rem;font-weight:700;display:inline-block;white-space:nowrap}}
.pb-완료{{background:#dcfce7;color:#15803d}}
.pb-진행중{{background:#fff7ed;color:#c2410c}}
.pb-기한초과{{background:#fee2e2;color:#991b1b;animation:pulse 2s infinite}}
.pb-empty{{background:#f1f5f9;color:#94a3b8}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.6}}}}
.dday{{font-size:.75rem;font-weight:700}}
.dday.over{{color:#dc2626}}.dday.soon{{color:#ea580c}}.dday.ok{{color:#16a34a}}.dday.done{{color:#64748b}}
.doc-num{{font-family:monospace;font-size:.75rem;color:#0369a1;font-weight:700}}
.action-desc{{max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:#334155}}
.dept-text{{font-size:.72rem;color:#64748b;max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.assignee{{font-size:.78rem;font-weight:600;color:#334155}}
.no-data{{text-align:center;padding:40px;color:#94a3b8}}
@media(max-width:900px){{.cards{{grid-template-columns:repeat(3,1fr)}}.charts-row{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<header>
  <div>
    <h1>QMS HSP 진행 현황 대시보드</h1>
    <div class="subtitle">EVENT → DEV → ISSUE → CAPA → ACTION 체인 관리 (화순공장, 2024~현재)</div>
  </div>
  <div class="hdr-right">데이터 기준: {TODAY}<br>자동 업데이트: 매일 06:00 KST</div>
</header>
<div class="wrap">
  <div class="cards">
    <div class="card blue"><div class="card-label">총 이벤트 체인</div><div class="card-val">{total_events}</div><div class="card-sub">EVENT (HSP 2024~현재)</div></div>
    <div class="card gray"><div class="card-label">CAPA 체인 수</div><div class="card-val">{total_capas}</div><div class="card-sub">ACTION 포함 CAPA</div></div>
    <div class="card green"><div class="card-label">ACTION 완료</div><div class="card-val">{done}</div><div class="card-sub">Release 처리 완료</div></div>
    <div class="card orange"><div class="card-label">ACTION 진행중</div><div class="card-val">{prog}</div><div class="card-sub">기한 내 처리 중</div></div>
    <div class="card red"><div class="card-label">ACTION 기한초과</div><div class="card-val">{over}</div><div class="card-sub">즉시 조치 필요</div></div>
  </div>
  <div class="charts-row">
    <div class="chart-card"><h3>ACTION 처리 현황</h3><div class="chart-wrap"><canvas id="donutChart"></canvas></div></div>
    <div class="chart-card"><h3>월별 ACTION 기한 분포</h3><div class="chart-wrap"><canvas id="barChart"></canvas></div></div>
  </div>
  <div class="filter-bar">
    <div class="fg"><label>연도</label>
      <select id="fYear" onchange="applyFilters()">
        <option value="">전체</option><option value="2026">2026</option>
        <option value="2025">2025</option><option value="2024">2024</option>
      </select></div>
    <div class="fg"><label>ACTION 상태</label>
      <select id="fProgress" onchange="applyFilters()">
        <option value="">전체</option><option value="기한초과">기한초과</option>
        <option value="진행중">진행중</option><option value="완료">완료</option>
      </select></div>
    <div class="fg"><label>체인 단계</label>
      <select id="fChainStage" onchange="applyFilters()">
        <option value="">전체</option>
        <option value="has_action">ACTION 있음</option>
        <option value="no_action">CAPA 미발생</option>
      </select></div>
    <input type="text" id="fSearch" placeholder="문서번호, 제목, 담당자 검색..." oninput="applyFilters()">
    <button class="clear-btn" onclick="clearFilters()">초기화</button>
    <span class="result-count" id="resultCount"></span>
  </div>
  <div class="legend">
    <span class="legend-item"><span class="dot" style="background:#1d4ed8"></span>EVENT</span>
    <span class="legend-item"><span class="dot" style="background:#7c3aed"></span>DEV</span>
    <span class="legend-item"><span class="dot" style="background:#15803d"></span>ISSUE</span>
    <span class="legend-item"><span class="dot" style="background:#854d0e"></span>CAPA</span>
    <span class="legend-item" style="margin-left:10px"><span class="status-dot s-Release"></span>Release(완료)</span>
    <span class="legend-item"><span class="status-dot s-Draft"></span>Draft(진행)</span>
  </div>
  <div class="table-wrap" style="margin-top:10px">
    <div class="tbl-scroll">
      <table>
        <thead><tr>
          <th onclick="sortBy('event_create')">이벤트 생성일 <span>↕</span></th>
          <th>E→D→I→C→A 체인</th>
          <th onclick="sortBy('action_num')">ACTION <span>↕</span></th>
          <th>ACTION 내용</th>
          <th onclick="sortBy('assignee_name')">담당자 <span>↕</span></th>
          <th>담당부서</th>
          <th onclick="sortBy('action_due')">완료기한 <span>↕</span></th>
          <th onclick="sortBy('action_complete')">실제완료일 <span>↕</span></th>
          <th onclick="sortBy('action_progress')">상태 <span>↕</span></th>
          <th onclick="sortBy('dday_sort')">D-Day <span>↕</span></th>
        </tr></thead>
        <tbody id="tableBody"></tbody>
      </table>
      <div id="noData" class="no-data" style="display:none">검색 결과가 없습니다.</div>
    </div>
  </div>
</div>
<script>
const DATA = {DATA_JSON};
// 부서 매핑 (추후 사원ID-부서 파일 수령 후 입력)
const DEPT_MAP = {DEPT_MAP_JSON};
DATA.forEach(r => {{ r.assignee_dept = DEPT_MAP[r.assignee_id] || ''; }});

let sortKey='event_create', _sortAsc=false, filtered=[];
function fmt(d){{return(!d||d==='None'||d==='')?'-':d.substring(0,10);}}
function dday(due,progress){{
  if(!due||due===''||due==='None')return{{text:'-',cls:'done',sort:9999}};
  if(progress==='완료')return{{text:'완료',cls:'done',sort:9998}};
  const diff=Math.ceil((new Date(due)-new Date())/(1000*60*60*24));
  if(diff<0)return{{text:`D+${{Math.abs(diff)}}`,cls:'over',sort:diff}};
  if(diff<=7)return{{text:`D-${{diff}}`,cls:'soon',sort:diff}};
  return{{text:`D-${{diff}}`,cls:'ok',sort:diff}};
}}
function statusDot(s){{const c=s==='Release'?'s-Release':s==='Draft'?'s-Draft':'s-Abort';return`<span class="status-dot ${{c}}"></span>`;}}
function chainBadge(num,status,type){{
  if(!num)return`<span style="color:#cbd5e1;font-size:.68rem">-</span>`;
  const dot=status?statusDot(status):'';
  return`<span class="chain-badge c-${{type}}" title="${{num}}">${{dot}}${{num}}</span>`;
}}
function renderTable(data){{
  const tbody=document.getElementById('tableBody');
  document.getElementById('resultCount').textContent=`${{data.length}}건`;
  if(data.length===0){{tbody.innerHTML='';document.getElementById('noData').style.display='block';return;}}
  document.getElementById('noData').style.display='none';
  tbody.innerHTML=data.map(r=>{{
    const dd=dday(r.action_due,r.action_progress);
    const isOverdue=r.action_progress==='기한초과';
    const trCls=isOverdue?' class="overdue"':'';
    const progCls=r.action_progress?`pb-${{r.action_progress}}`:'pb-empty';
    const progText=r.action_progress||(r.capa_num?'ACTION 없음':'체인 진행중');
    const chain=`<div class="chain-row">
      ${{chainBadge(r.event_num,r.event_status,'event')}}
      <span class="arrow">→</span>${{chainBadge(r.dev_num,r.dev_status,'dev')}}
      <span class="arrow">→</span>${{chainBadge(r.issue_num,r.issue_status,'issue')}}
      <span class="arrow">→</span>${{chainBadge(r.capa_num,r.capa_status,'capa')}}
    </div>`;
    return`<tr${{trCls}}>
      <td style="white-space:nowrap;font-size:.78rem;color:#475569">${{r.event_create||'-'}}</td>
      <td>${{chain}}</td>
      <td class="doc-num">${{r.action_num||'-'}}</td>
      <td class="action-desc" title="${{(r.action_desc||'').replace(/"/g,'&quot;')}}">${{(r.action_desc||'').substring(0,50)||'-'}}</td>
      <td class="assignee">${{r.assignee_name||'-'}}</td>
      <td class="dept-text" title="${{r.assignee_dept||''}}">${{r.assignee_dept||''}}</td>
      <td style="white-space:nowrap;font-size:.78rem">${{fmt(r.action_due)}}</td>
      <td style="white-space:nowrap;font-size:.78rem">${{fmt(r.action_complete)}}</td>
      <td><span class="prog-badge ${{progCls}}">${{progText}}</span></td>
      <td><span class="dday ${{dd.cls}}">${{dd.text}}</span></td>
    </tr>`;
  }}).join('');
}}
function applyFilters(){{
  const yr=document.getElementById('fYear').value;
  const prog=document.getElementById('fProgress').value;
  const stage=document.getElementById('fChainStage').value;
  const search=document.getElementById('fSearch').value.toLowerCase();
  filtered=DATA.filter(r=>{{
    if(yr&&!r.event_create.startsWith(yr))return false;
    if(prog&&r.action_progress!==prog)return false;
    if(stage==='has_action'&&!r.action_num)return false;
    if(stage==='no_action'&&r.action_num)return false;
    if(search){{
      const text=[r.event_num,r.dev_num,r.issue_num,r.capa_num,r.action_num,
                  r.event_title,r.action_desc,r.assignee_name].join(' ').toLowerCase();
      if(!text.includes(search))return false;
    }}
    return true;
  }});
  sortAndRender();
}}
function clearFilters(){{
  ['fYear','fProgress','fChainStage'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('fSearch').value='';
  applyFilters();
}}
function sortBy(key){{
  if(sortKey===key)_sortAsc=!_sortAsc;else{{sortKey=key;_sortAsc=false;}}
  sortAndRender();
}}
function sortAndRender(){{
  filtered.sort((a,b)=>{{
    let va,vb;
    if(sortKey==='dday_sort'){{va=dday(a.action_due,a.action_progress).sort;vb=dday(b.action_due,b.action_progress).sort;}}
    else{{va=a[sortKey]||'';vb=b[sortKey]||'';}}
    if(va<vb)return _sortAsc?-1:1;
    if(va>vb)return _sortAsc?1:-1;
    return 0;
  }});
  renderTable(filtered);
}}
applyFilters();
const done={done},prog={prog},over={over};
new Chart(document.getElementById('donutChart'),{{
  type:'doughnut',
  data:{{labels:['완료','진행중','기한초과'],
    datasets:[{{data:[done,prog,over],backgroundColor:['#16a34a','#ea580c','#dc2626'],borderWidth:2,borderColor:'#fff'}}]}},
  options:{{responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{position:'bottom',labels:{{font:{{size:11}},padding:10}}}},
      tooltip:{{callbacks:{{label:ctx=>`${{ctx.label}}: ${{ctx.raw}}건`}}}}}}}}
}});
const monthCount={{}};
DATA.forEach(r=>{{
  if(r.action_due&&r.action_due.length>=7&&r.action_due!=='None'){{
    const ym=r.action_due.substring(0,7);
    monthCount[ym]=monthCount[ym]||{{done:0,prog:0,over:0}};
    if(r.action_progress==='완료')monthCount[ym].done++;
    else if(r.action_progress==='기한초과')monthCount[ym].over++;
    else monthCount[ym].prog++;
  }}
}});
const months=Object.keys(monthCount).sort().slice(-18);
new Chart(document.getElementById('barChart'),{{
  type:'bar',
  data:{{labels:months,datasets:[
    {{label:'완료',data:months.map(m=>monthCount[m]?.done||0),backgroundColor:'#16a34a',borderRadius:3}},
    {{label:'진행중',data:months.map(m=>monthCount[m]?.prog||0),backgroundColor:'#ea580c',borderRadius:3}},
    {{label:'기한초과',data:months.map(m=>monthCount[m]?.over||0),backgroundColor:'#dc2626',borderRadius:3}}
  ]}},
  options:{{responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{position:'top',labels:{{font:{{size:11}}}}}}}},
    scales:{{x:{{stacked:true,ticks:{{font:{{size:9}},maxRotation:45}}}},y:{{stacked:true,beginAtZero:true}}}}}}
}});
</script>
</body>
</html>"""


def generate_html(records, stats):
    # 부서 매핑 (추후 파일 수령 후 여기에 추가)
    dept_map = {}

    data_json    = json.dumps(records, ensure_ascii=False)
    dept_map_json = json.dumps(dept_map, ensure_ascii=False)

    return (HTML_TEMPLATE
        .replace("{TODAY}", TODAY)
        .replace("{total_events}", str(stats["total_events"]))
        .replace("{total_capas}",  str(stats["total_capas"]))
        .replace("{done}",  str(stats["done"]))
        .replace("{prog}",  str(stats["prog"]))
        .replace("{over}",  str(stats["over"]))
        .replace("{DATA_JSON}",     data_json)
        .replace("{DEPT_MAP_JSON}", dept_map_json)
    )


def main():
    print(f"[{TODAY}] Snowflake 데이터 조회 시작...")
    rows = fetch_data()
    print(f"  → {len(rows)}건 조회 완료")

    records = build_records(rows)
    stats   = compute_stats(records)
    print(f"  → 완료:{stats['done']} / 진행중:{stats['prog']} / 기한초과:{stats['over']}")

    html = generate_html(records, stats)

    output_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  → index.html 생성 완료 ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
