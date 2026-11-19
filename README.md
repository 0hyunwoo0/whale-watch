# 🐋 Whale Watch

개인용 아이폰 웹앱 — 종목 검색, 실시간 주가, 뉴스, 애널리스트 평가, 거물 투자자 13F 매매내역을 한 화면에서.

- **앱 주소**: https://0hyunwoo0.github.io/whale-watch/
- **설치**: 아이폰 Safari로 위 주소 열기 → 공유 버튼 → *홈 화면에 추가*
- **Finnhub 키**: 앱의 *설정* 탭에서 입력 (기기에만 저장, 저장소에는 올라가지 않음)

## 구성
| 파일 | 역할 |
|---|---|
| `index.html`, `app.js`, `style.css` | 앱 화면 |
| `scripts/filers.json` | 추적할 거물 목록 (CIK) — 여기에 추가/삭제 |
| `scripts/ticker_overrides.json` | 자동 매핑이 안 되는 종목의 티커 수동 지정 |
| `scripts/update_13f.py` | SEC EDGAR에서 최신 2개 분기 13F를 받아 비교 → `data/13f.json` |
| `.github/workflows/update-13f.yml` | 매일 오전 9시 17분(KST) 자동 갱신, Actions 탭에서 수동 실행도 가능 |

## 데이터 출처
- 13F: SEC EDGAR (분기 말 기준, 분기 종료 후 최대 45일 뒤 공시 / 미국 상장주식 롱 포지션만)
- CUSIP→티커: OpenFIGI (선택: `OPENFIGI_API_KEY` 시크릿을 넣으면 더 빠름)
- 주가·뉴스·애널리스트: Finnhub 무료 API
- 차트: TradingView 위젯
