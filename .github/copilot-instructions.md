# AI Agent Instructions for Excel Dashboard System

## Project Overview
Python system converting Excel spreadsheets → interactive HTML analytics dashboards. Three independent converters:
- **exceltest.py**: Multi-restaurant system (main index + individual dashboards per restaurant)
- **ifoodbot.py**: Single-file section-based dashboard
- **Untitled-1.py**: Single-file analytics dashboard with KPIs and charts

Core pipeline: Excel parse → smart header detection → data cleaning → JSON embed → HTML generation.

## Critical Architecture Patterns

### Smart Header Detection (`_find_header_row`)
Excel files have inconsistent structure. System scores first 5 rows by `non_null + text_count`, picks highest. **If wrong, all downstream data corrupts.** Test with headers in rows 1-3, not row 0.

### Data Cleaning Pipeline (identical across all converters)
```
Read raw → Find header row → Re-read with header → Drop empty columns
→ Strip Unnamed* columns → Drop empty rows → Replace error codes (#DIV/0!, #REF!, etc.)
→ Convert to numeric (50%+ threshold) → Filter sheets with numeric data
```
Error codes replaced BEFORE numeric conversion; numeric detection checks if 50%+ of values parse successfully.

### Column Inference (drives all visualizations)
- **Label columns** (x-axis): Keywords = `['mês','mes','month','data','date','dia','day','período','periodo','period','semana','week']`
- **Metric columns**: Keywords = `['vendas','ticket médio','valor bruto','líquido','cancelamentos','visitas']`
- Fallback: First text column = labels, all numeric = metrics

Portuguese/English keywords—language-aware matching.

## Converter Variants (Choose Based on Use Case)

| Converter | Input | Output | Use Case | Key Classes |
|-----------|-------|--------|----------|------------|
| **exceltest.py** | Multi-sheet Excel + restaurant list | `dashboard_output/index.html` + `/restaurants/*.html` | Multi-location analytics (chain restaurants, multi-vendor) | `MultiRestaurantDashboard` |
| **ifoodbot.py** | Single Excel with section headers | `dashboard.html` | Simple single-restaurant dashboard grouped by sections | `ExcelDashboardConverter` |
| **Untitled-1.py** | Single Excel with labeled data | `dashboard.html` (KPI-focused) | Analytics dashboard emphasizing KPIs with trends | `ExcelAnalyticsDashboard` |

All three share identical data cleaning pipeline; key difference is **output structure** and **metric extraction granularity**.

## Converter Specifics

### exceltest.py: `add_restaurant()`
```python
add_restaurant(excel_file, name=None, manager="Gerente", platforms=None)
```
- `name`: If omitted, infers from filename (stem, title case)
- `platforms`: Auto-detected from sheet names with numeric data; override with list
- `manager`: Stored in restaurant metadata but not used in current version
- Must call `setup_output_folder()` before adding restaurants, then `render_all()` to generate HTML

### ifoodbot.py & Untitled-1.py: Constructor Setup
Both take excel file path and optional output filename. Call `generate_html()` after initialization.
- Different visual structure but identical core parsing logic
- Use `Untitled-1.py` for KPI-heavy dashboards; `ifoodbot.py` for section-organized data

## Multi-Restaurant System (exceltest.py)

**Two-stage processing:**
1. Read: Extract all sheets, auto-detect headers, clean → `Dict[sheet_name, DataFrame]`
2. Render: Calculate metrics from cleaned data, embed JSON, generate HTML

**Restaurant object structure**:
```python
{
    'id': hashlib.md5(name.encode()).hexdigest()[:8],  # Allows reusing same Excel file
    'name': str,
    'file': str,
    'manager': str,
    'platforms': List[str]  # Auto-detected from sheet names containing numeric data
}
```

**Metric extraction** (`extract_summary_metrics()`):
- From **first sheet only** (`list(all_data.values())[0]`)
- Values from **last row** (`.iloc[-1]`) + `last_period` from label column
- Trends: `((current - previous) / previous) * 100` where previous = `.iloc[-2]`
- Returns: `{'vendas': float, 'ticket_medio': float, 'valor_bruto': float, 'liquido': float, 'trends': {metric: percent}}`

**Column mapping** (case-insensitive, substring match):
```python
'vendas': ['vendas', 'sales', 'pedidos', 'orders']
'ticket_medio': ['ticket médio', 'ticket medio', 'average ticket']
'valor_bruto': ['valor bruto', 'bruto', 'gross', 'faturamento']
'liquido': ['líquido', 'liquido', 'net', 'receita líquida']
```

**Critical gotcha**: Sheet order matters—metrics come from **first sheet only**. If sheets are reordered, summary metrics change for ALL restaurants.

Platform detection: Sheet names auto-checked via `_has_data()` which requires numeric data in sheet. Non-numeric sheets silently filtered.

Main page (index.html): Restaurant cards, sorting, search handled **entirely via browser JavaScript**—`restaurantsData` JSON array embedded at render time.

## Common Data Issues

| Issue | Pattern | Solution |
|-------|---------|----------|
| Wrong header detected | `_find_header_row` picks row 0 | Check first 5 rows manually; system picks highest-scoring row |
| Headers in row 2+ | Column names appear as data | System auto-detects; verify with small test Excel |
| Mixed columns | Strings + numbers in same column | Numeric check needs 50%+ successful conversions |
| Error codes as text | `#DIV/0!`, `#N/A` appear in data | Replaced before numeric conversion (already implemented) |
| Empty separator columns | Blank columns in middle | Dropped via `dropna(axis=1, how='all')` |
| Unnamed columns | `Unnamed: 0`, `Unnamed: 1` | Filtered with regex `^Unnamed` pattern |

## Key Dependencies
- **pandas**: Excel read, dataframe ops (openpyxl engine)
- **numpy**: Numeric checks, type operations
- **pathlib**: Cross-platform file handling (mandatory)
- **json**: Chart data embedding in HTML
- **hashlib**: Restaurant ID generation (MD5)
- **Chart.js**: CDN for line charts (8-color palette, wraps with modulo)

## Development Workflows

### Adding Metric Types
1. Extend `col_mapping` in `extract_summary_metrics()` (exceltest.py)
2. Add Portuguese/English keywords
3. Test metric detection with real Excel files

### Extending Data Cleaning
Modify `read_excel()` in relevant converter:
- Insert filters between "Drop empty rows" and "Replace error codes"
- Test on existing sheets first to avoid breaking current pipeline

### New Dashboard Features
In `generate_html()` method:
1. Prepare data dict (merge chart data + metrics)
2. Inject as JSON: `<script>const data = {...}</script>`
3. Reference in HTML sections

## Execution & Debugging

**Run converters** (independent, no shared state):
```bash
python exceltest.py       # Creates dashboard_output/index.html + dashboard_output/restaurants/{id}.html
python ifoodbot.py        # Creates dashboard.html (single file)
python Untitled-1.py      # Creates dashboard.html (single file)
```

**Debug missing data:**
- Print statements show "✓ Sheet: X rows" or "✗ Sheet: no numeric data"
- Check `_find_header_row()` logic—if wrong, all downstream breaks
- Verify numeric conversion threshold (50%+ of values must parse)

**Dashboard not rendering:**
- Inspect browser console (JSON parsing errors)
- Validate `labels` array length = all `datasets[x].data` length
- Check `chartData` object structure in DevTools

**Validate output** (open HTML in browser):
- Sheet selector populated
- Charts render with correct labels and trends
- Metrics formatted (R$ for currency, % for trends)
- Tables show category headers (colored bands)

## Code Style
- **Type hints required**: `Dict[str, pd.DataFrame]`, `List[str]`, `Optional[str]`
- **Column normalization**: Always `col_lower = str(col).lower()` for keyword matching
- **Path handling**: Use `pathlib.Path` exclusively (cross-platform)
- **Docstrings**: Explain "what & why", not implementation
- **Exception handling**: Broad `except:` with fallbacks—data quality issues shouldn't crash pipeline

## Template System & HTML Generation

The `templates/` folder contains reusable HTML/CSS/JS components:
- **template_main_html.html**: Main index page template (used by exceltest.py)
- **template_dashboard_html.html**: Individual restaurant/dashboard template
- **template_styles.css**: Shared stylesheet (colors, layout, responsive design)
- **main_page.js**: Index page JavaScript (restaurant search, sort, filter)
- **dashboard.js**: Dashboard page JavaScript (sheet selector, chart rendering)

When generating HTML:
1. Read template from `templates/` directory
2. Inject data as `<script>const chartData = {...}</script>` before closing `</head>`
3. Write to output location (either `dashboard_output/index.html` or `dashboard_output/restaurants/{id}.html`)

Chart.js is loaded from CDN (unpkg). No build step needed.
