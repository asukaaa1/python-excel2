#!/usr/bin/env python3
"""
Excel to Analytics Dashboard Converter
Converts Excel spreadsheets into beautiful analytics dashboards with KPIs and charts
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys
from datetime import datetime
import numpy as np


class ExcelAnalyticsDashboard:
    """Convert Excel files to analytics dashboards with KPIs and visualizations"""
    
    def __init__(self, excel_file: str, output_file: str = "dashboard.html"):
        self.excel_file = excel_file
        self.output_file = output_file
        
    def read_excel(self) -> Dict[str, pd.DataFrame]:
        """Read all sheets from Excel file with smart header detection"""
        try:
            xl = pd.ExcelFile(self.excel_file, engine='openpyxl')
            cleaned_data = {}
            
            for sheet_name in xl.sheet_names:
                # First, read raw to detect structure
                df_raw = pd.read_excel(xl, sheet_name=sheet_name, header=None)
                
                # Find the header row (row with most non-null unique values)
                header_row = self._find_header_row(df_raw)
                
                # Read again with proper header
                df = pd.read_excel(xl, sheet_name=sheet_name, header=header_row)
                
                # Remove completely empty columns (separator columns)
                df = df.dropna(axis=1, how='all')
                
                # Remove columns with no name or unnamed
                df = df.loc[:, ~df.columns.astype(str).str.contains('^Unnamed')]
                
                # Remove completely empty rows
                df = df.dropna(how='all')
                
                # Convert numeric columns properly
                for col in df.columns:
                    if df[col].dtype == 'object':
                        # Try to convert to numeric, handling #DIV/0! and other errors
                        df[col] = df[col].replace(['#DIV/0!', '#REF!', '#N/A', '#VALUE!'], np.nan)
                        numeric_col = pd.to_numeric(df[col], errors='coerce')
                        # Check if conversion was mostly successful
                        if numeric_col.notna().sum() >= df[col].notna().sum() * 0.5:
                            df[col] = numeric_col
                
                # Reset index
                df = df.reset_index(drop=True)
                
                # Only include sheets that have actual data
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                has_data = any(df[col].notna().sum() > 0 for col in numeric_cols) if len(numeric_cols) > 0 else False
                
                if len(df) > 0 and has_data:
                    cleaned_data[sheet_name] = df
                    print(f"  ✓ {sheet_name}: {len(df)} rows, {len(df.columns)} columns with data")
                else:
                    print(f"  ✗ {sheet_name}: skipped (no numeric data)")
            
            return cleaned_data
            
        except Exception as e:
            print(f"Error reading Excel file: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    def _find_header_row(self, df: pd.DataFrame) -> int:
        """Find the row that contains column headers"""
        best_row = 0
        best_score = 0
        
        # Check first 5 rows
        for i in range(min(5, len(df))):
            row = df.iloc[i]
            # Count non-null, non-empty string values
            non_null = row.notna().sum()
            # Prefer rows with text values (headers are usually text)
            text_count = sum(1 for v in row if isinstance(v, str) and v.strip())
            # Score based on both
            score = non_null + text_count
            
            if score > best_score:
                best_score = score
                best_row = i
        
        return best_row
    
    def find_label_column(self, df: pd.DataFrame) -> Optional[str]:
        """Find the best column to use as labels"""
        label_keywords = ['mês', 'mes', 'month', 'data', 'date', 'dia', 'day', 
                         'período', 'periodo', 'period', 'semana', 'week']
        
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if any(keyword in col_lower for keyword in label_keywords):
                return col
        
        # First non-numeric column
        for col in df.columns:
            if df[col].dtype == 'object':
                return col
        
        return df.columns[0] if len(df.columns) > 0 else None
    
    def get_numeric_columns(self, df: pd.DataFrame, exclude_col: Optional[str] = None) -> List[str]:
        """Get all numeric columns with actual data"""
        numeric_cols = []
        for col in df.select_dtypes(include=[np.number]).columns:
            if col != exclude_col and df[col].notna().sum() > 0:
                numeric_cols.append(col)
        return numeric_cols
    
    def extract_metrics(self, df: pd.DataFrame) -> List[Dict]:
        """Extract key metrics from the data"""
        metrics = []
        df_clean = df.dropna(how='all').copy()
        
        label_col = self.find_label_column(df_clean)
        numeric_cols = self.get_numeric_columns(df_clean, label_col)
        
        # Metric descriptions
        metric_descriptions = {
            'vendas': 'Total de vendas realizadas',
            'novos clientes': 'Novos clientes no período',
            'clientes': 'Total de clientes',
            'visitas': 'Visitas ao cardápio',
            'visualizações': 'Visualizações de produtos',
            'sacola': 'Itens adicionados à sacola',
            'revisão': 'Pedidos em revisão',
            'concluídos': 'Pedidos concluídos',
            'ticket médio': 'Valor médio por pedido',
            'valor bruto': 'Faturamento bruto',
            'líquido': 'Faturamento líquido',
            'cancelamentos': 'Taxa de cancelamento',
        }
        
        # Priority order
        priority_keywords = ['vendas', 'concluídos', 'novos clientes', 'valor bruto', 
                           'líquido', 'ticket médio', 'visitas', 'visualizações']
        
        def get_priority(col):
            col_lower = str(col).lower()
            for i, keyword in enumerate(priority_keywords):
                if keyword in col_lower:
                    return i
            return len(priority_keywords)
        
        ordered_cols = sorted(numeric_cols, key=get_priority)
        
        for col in ordered_cols[:4]:
            col_data = df_clean[col].dropna()
            if len(col_data) == 0:
                continue
            
            # Determine if this is a rate/percentage or a sum
            col_lower = str(col).lower()
            is_average_metric = any(kw in col_lower for kw in ['médio', 'media', 'average', 'taxa', 'rate', '%'])
            
            if is_average_metric:
                value = col_data.mean()
            else:
                value = col_data.sum()
            
            # Calculate trend
            mid = len(col_data) // 2
            if mid > 0:
                first_half = col_data.iloc[:mid].mean()
                second_half = col_data.iloc[mid:].mean()
                if first_half > 0:
                    trend = ((second_half - first_half) / first_half) * 100
                else:
                    trend = 0
            else:
                trend = 0
            
            # Get description
            description = None
            for key, desc in metric_descriptions.items():
                if key in col_lower:
                    description = desc
                    break
            if not description:
                description = f'Total de {col}'
            
            # Determine format
            format_type = 'number'
            if any(kw in col_lower for kw in ['valor', 'bruto', 'líquido', 'receita', 'faturamento']):
                format_type = 'currency'
            elif any(kw in col_lower for kw in ['%', 'taxa', 'rate']):
                format_type = 'percent'
            
            metrics.append({
                'name': str(col),
                'description': description,
                'value': float(value) if not pd.isna(value) else 0,
                'trend': float(trend) if not pd.isna(trend) else 0,
                'format': format_type
            })
        
        return metrics
    
    def prepare_all_chart_data(self, df: pd.DataFrame) -> Dict:
        """Prepare data for charts"""
        df_clean = df.dropna(how='all').copy()
        
        label_col = self.find_label_column(df_clean)
        
        # Check if we should truncate at December 26
        cutoff_index = None
        if label_col and label_col in df_clean.columns:
            labels_raw = df_clean[label_col].fillna('').astype(str).tolist()
            labels_raw = [str(l).strip() for l in labels_raw if str(l).strip()]
            
            for i, label in enumerate(labels_raw):
                label_str = str(label).lower().strip()
                # Look for Dec 26 pattern - match any date format containing 26 and december/dez
                has_26 = '26' in label_str
                has_dec = any(term in label_str for term in ['dez', 'dec', '12', 'dezembro', 'december', 'dezembro de', 'de dez', 'de dec'])
                
                if has_26 and has_dec:
                    cutoff_index = i
                    break
        
        # Truncate dataframe at Dec 26 if found, then remove empty columns
        if cutoff_index is not None:
            df_clean = df_clean.iloc[:cutoff_index + 1].copy()
            # Remove columns that are completely empty after truncation
            df_clean = df_clean.dropna(axis=1, how='all')
        
        numeric_cols = self.get_numeric_columns(df_clean, label_col)
        
        # Get labels
        if label_col and label_col in df_clean.columns:
            labels = df_clean[label_col].fillna('').astype(str).tolist()
            labels = [str(l).strip() for l in labels if str(l).strip()]
        else:
            labels = [f"Item {i+1}" for i in range(len(df_clean))]
        
        # Prepare datasets
        datasets = {}
        colors = [
            {'line': 'rgb(239, 68, 68)', 'bg': 'rgba(239, 68, 68, 0.1)'},
            {'line': 'rgb(59, 130, 246)', 'bg': 'rgba(59, 130, 246, 0.1)'},
            {'line': 'rgb(34, 197, 94)', 'bg': 'rgba(34, 197, 94, 0.1)'},
            {'line': 'rgb(168, 85, 247)', 'bg': 'rgba(168, 85, 247, 0.1)'},
            {'line': 'rgb(249, 115, 22)', 'bg': 'rgba(249, 115, 22, 0.1)'},
            {'line': 'rgb(236, 72, 153)', 'bg': 'rgba(236, 72, 153, 0.1)'},
            {'line': 'rgb(20, 184, 166)', 'bg': 'rgba(20, 184, 166, 0.1)'},
            {'line': 'rgb(245, 158, 11)', 'bg': 'rgba(245, 158, 11, 0.1)'},
        ]
        
        for idx, col in enumerate(numeric_cols):
            col_data = df_clean[col].fillna(0).tolist()
            col_data = col_data[:len(labels)]
            # Only pad if we didn't truncate at Dec 26
            if cutoff_index is None:
                while len(col_data) < len(labels):
                    col_data.append(0)
            col_data = [float(x) if not pd.isna(x) else 0 for x in col_data]
            
            color_idx = idx % len(colors)
            datasets[str(col)] = {
                'label': str(col),
                'data': col_data,
                'borderColor': colors[color_idx]['line'],
                'backgroundColor': colors[color_idx]['bg'],
                'tension': 0.4,
                'fill': True
            }
        
        return {
            'labels': labels,
            'datasets': datasets,
            'columns': [str(col) for col in numeric_cols]
        }
    
    def generate_html(self, all_data: Dict[str, pd.DataFrame]) -> str:
        """Generate complete HTML dashboard"""
        
        # Use first sheet with data
        first_sheet_name = list(all_data.keys())[0]
        first_sheet = all_data[first_sheet_name]
        
        metrics = self.extract_metrics(first_sheet)
        chart_data = self.prepare_all_chart_data(first_sheet)
        
        # Prepare all sheets data
        all_sheets_chart_data = {}
        for sheet_name, df in all_data.items():
            all_sheets_chart_data[sheet_name] = self.prepare_all_chart_data(df)
        
        # Tables data
        tables_data = []
        for sheet_name, df in all_data.items():
            df_json = df.copy()
            for col in df_json.columns:
                if df_json[col].dtype == 'datetime64[ns]':
                    df_json[col] = df_json[col].dt.strftime('%Y-%m-%d')
                df_json[col] = df_json[col].fillna('')
            
            tables_data.append({
                'name': sheet_name,
                'columns': [str(col) for col in df_json.columns.tolist()],
                'data': df_json.to_dict('records')
            })

        html_content = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Dashboard de Analytics</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        :root {{
            --bg-primary: #ffffff;
            --bg-secondary: #f8fafc;
            --text-primary: #0f172a;
            --text-secondary: #64748b;
            --border: #e2e8f0;
            --accent: #ef4444;
            --accent-light: #fee2e2;
            --green: #22c55e;
            --shadow: 0 1px 3px rgba(0,0,0,0.1);
            --shadow-lg: 0 4px 6px -1px rgba(0,0,0,0.1);
        }}
        
        body {{
            font-family: 'Inter', system-ui, sans-serif;
            background: var(--bg-secondary);
            color: var(--text-primary);
            line-height: 1.6;
        }}
        
        .dashboard {{ max-width: 1400px; margin: 0 auto; padding: 2rem; }}
        
        .header {{ margin-bottom: 2rem; }}
        .header h1 {{
            font-size: 1.5rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }}
        .header .subtitle {{ font-size: 0.875rem; color: var(--text-secondary); margin-top: 0.25rem; }}
        .icon {{ width: 28px; height: 28px; color: var(--accent); }}
        
        /* Sheet Selector */
        .sheet-selector {{
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1.5rem;
            flex-wrap: wrap;
        }}
        .sheet-btn {{
            padding: 0.5rem 1rem;
            font-size: 0.875rem;
            font-weight: 500;
            color: var(--text-secondary);
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .sheet-btn:hover {{ border-color: var(--accent); color: var(--text-primary); }}
        .sheet-btn.active {{
            background: var(--accent);
            color: white;
            border-color: var(--accent);
        }}
        
        /* Metrics */
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }}
        .metric-card {{
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 1.25rem;
            box-shadow: var(--shadow);
        }}
        .metric-label {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            font-weight: 500;
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.025em;
        }}
        .metric-value {{
            font-size: 1.75rem;
            font-weight: 700;
            color: var(--text-primary);
            margin-bottom: 0.25rem;
        }}
        .metric-trend {{
            font-size: 0.8rem;
            font-weight: 500;
            display: flex;
            align-items: center;
            gap: 0.25rem;
        }}
        .metric-trend.positive {{ color: var(--green); }}
        .metric-trend.negative {{ color: var(--accent); }}
        
        /* Chart Section */
        .chart-section {{
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 12px;
            margin-bottom: 2rem;
            box-shadow: var(--shadow);
            overflow: hidden;
        }}
        .chart-header {{
            padding: 1rem 1.5rem;
            border-bottom: 1px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
        }}
        .chart-title {{ font-weight: 600; font-size: 1rem; }}
        
        .column-tabs {{
            display: flex;
            gap: 0.25rem;
            flex-wrap: wrap;
        }}
        .column-tab {{
            padding: 0.4rem 0.75rem;
            font-size: 0.75rem;
            font-weight: 500;
            color: var(--text-secondary);
            background: var(--bg-secondary);
            border: none;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s;
        }}
        .column-tab:hover {{ background: var(--border); color: var(--text-primary); }}
        .column-tab.active {{ background: var(--accent); color: white; }}
        
        .chart-container {{ padding: 1.5rem; }}
        .chart-wrapper {{ position: relative; height: 320px; }}
        
        .chart-stats {{
            display: flex;
            gap: 2rem;
            padding: 1rem 1.5rem;
            border-top: 1px solid var(--border);
            background: var(--bg-secondary);
            flex-wrap: wrap;
        }}
        .stat {{ font-size: 0.8rem; color: var(--text-secondary); }}
        .stat strong {{ color: var(--text-primary); }}
        
        /* Tables */
        .data-section {{
            background: var(--bg-primary);
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
            margin-bottom: 1.5rem;
            box-shadow: var(--shadow);
        }}
        .section-header {{
            padding: 1rem 1.5rem;
            font-weight: 600;
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border);
        }}
        .table-container {{ overflow-x: auto; }}
        .data-table {{ width: 100%; border-collapse: collapse; }}
        .data-table th {{
            padding: 0.75rem 1rem;
            text-align: left;
            font-size: 0.7rem;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border);
            white-space: nowrap;
        }}
        .data-table td {{
            padding: 0.75rem 1rem;
            font-size: 0.85rem;
            border-bottom: 1px solid var(--border);
        }}
        .data-table tbody tr:hover {{ background: var(--bg-secondary); }}
        .data-table td.numeric {{ text-align: right; font-variant-numeric: tabular-nums; }}
        
        .no-data {{ padding: 3rem; text-align: center; color: var(--text-secondary); }}
        
        @media (max-width: 768px) {{
            .dashboard {{ padding: 1rem; }}
            .metrics-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .metric-value {{ font-size: 1.25rem; }}
            .chart-wrapper {{ height: 250px; }}
        }}
    </style>
</head>
<body>
    <div class="dashboard">
        <div class="header">
            <h1>
                <svg class="icon" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"/>
                </svg>
                Dashboard de Vendas
            </h1>
            <div class="subtitle">Análise de performance por canal</div>
        </div>
        
        <div class="sheet-selector" id="sheetSelector"></div>
        
        <div class="metrics-grid" id="metricsGrid"></div>
        
        <div class="chart-section">
            <div class="chart-header">
                <div class="chart-title" id="chartTitle">Evolução Mensal</div>
                <div class="column-tabs" id="columnTabs"></div>
            </div>
            <div class="chart-container">
                <div class="chart-wrapper">
                    <canvas id="mainChart"></canvas>
                </div>
            </div>
            <div class="chart-stats" id="chartStats"></div>
        </div>
        
        <div id="dataTablesContainer"></div>
    </div>

    <script>
        // Data from Python
        const allSheetsChartData = {json.dumps(all_sheets_chart_data, ensure_ascii=False)};
        const tablesData = {json.dumps(tables_data, ensure_ascii=False)};
        const sheetNames = {json.dumps(list(all_data.keys()), ensure_ascii=False)};
        
        // State
        let mainChart = null;
        let activeSheet = sheetNames[0];
        let activeColumn = null;
        
        function getCurrentChartData() {{
            return allSheetsChartData[activeSheet] || {{ labels: [], datasets: {{}}, columns: [] }};
        }}
        
        function formatNumber(value, format) {{
            if (value === null || value === undefined || isNaN(value)) return '-';
            if (format === 'currency') {{
                return 'R$ ' + value.toLocaleString('pt-BR', {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }});
            }}
            if (format === 'percent') {{
                return (value * 100).toFixed(1) + '%';
            }}
            if (Math.abs(value) >= 1000000) return (value / 1000000).toFixed(1) + 'M';
            if (Math.abs(value) >= 1000) return (value / 1000).toFixed(1) + 'K';
            return value.toLocaleString('pt-BR', {{ maximumFractionDigits: 0 }});
        }}
        
        function renderSheetSelector() {{
            const container = document.getElementById('sheetSelector');
            container.innerHTML = sheetNames.map(name => 
                `<button class="sheet-btn ${{name === activeSheet ? 'active' : ''}}" data-sheet="${{name}}">${{name}}</button>`
            ).join('');
            
            container.querySelectorAll('.sheet-btn').forEach(btn => {{
                btn.addEventListener('click', () => {{
                    activeSheet = btn.dataset.sheet;
                    activeColumn = null;
                    renderAll();
                }});
            }});
        }}
        
        function renderMetrics() {{
            const container = document.getElementById('metricsGrid');
            const chartData = getCurrentChartData();
            
            if (!chartData.columns || chartData.columns.length === 0) {{
                container.innerHTML = '<div class="no-data">Nenhuma métrica disponível</div>';
                return;
            }}
            
            // Calculate metrics from chart data
            const metrics = chartData.columns.slice(0, 4).map(col => {{
                const dataset = chartData.datasets[col];
                if (!dataset) return null;
                
                const data = dataset.data.filter(d => d !== 0 && !isNaN(d));
                const total = data.reduce((a, b) => a + b, 0);
                
                // Calculate trend
                const mid = Math.floor(data.length / 2);
                let trend = 0;
                if (mid > 0) {{
                    const firstHalf = data.slice(0, mid).reduce((a, b) => a + b, 0) / mid;
                    const secondHalf = data.slice(mid).reduce((a, b) => a + b, 0) / (data.length - mid);
                    if (firstHalf > 0) trend = ((secondHalf - firstHalf) / firstHalf) * 100;
                }}
                
                const colLower = col.toLowerCase();
                const isAvg = colLower.includes('médio') || colLower.includes('taxa') || colLower.includes('%');
                const isCurrency = colLower.includes('valor') || colLower.includes('bruto') || colLower.includes('líquido');
                
                return {{
                    name: col,
                    value: isAvg ? (data.length > 0 ? total / data.length : 0) : total,
                    trend: trend,
                    format: isCurrency ? 'currency' : 'number'
                }};
            }}).filter(m => m !== null);
            
            container.innerHTML = metrics.map(m => `
                <div class="metric-card">
                    <div class="metric-label">${{m.name}}</div>
                    <div class="metric-value">${{formatNumber(m.value, m.format)}}</div>
                    <div class="metric-trend ${{m.trend >= 0 ? 'positive' : 'negative'}}">
                        ${{m.trend >= 0 ? '↑' : '↓'}} ${{Math.abs(m.trend).toFixed(1)}}%
                    </div>
                </div>
            `).join('');
        }}
        
        function renderColumnTabs() {{
            const container = document.getElementById('columnTabs');
            const chartData = getCurrentChartData();
            
            if (!chartData.columns || chartData.columns.length === 0) {{
                container.innerHTML = '';
                return;
            }}
            
            if (!activeColumn || !chartData.columns.includes(activeColumn)) {{
                activeColumn = chartData.columns[0];
            }}
            
            container.innerHTML = chartData.columns.slice(0, 8).map(col => 
                `<button class="column-tab ${{col === activeColumn ? 'active' : ''}}" data-column="${{col}}">${{col}}</button>`
            ).join('');
            
            container.querySelectorAll('.column-tab').forEach(btn => {{
                btn.addEventListener('click', () => {{
                    activeColumn = btn.dataset.column;
                    container.querySelectorAll('.column-tab').forEach(t => t.classList.remove('active'));
                    btn.classList.add('active');
                    updateChart();
                }});
            }});
        }}
        
        function updateChart() {{
            const ctx = document.getElementById('mainChart').getContext('2d');
            const chartData = getCurrentChartData();
            
            if (!chartData.datasets || !activeColumn || !chartData.datasets[activeColumn]) {{
                if (mainChart) mainChart.destroy();
                document.getElementById('chartStats').innerHTML = '';
                return;
            }}
            
            const dataset = chartData.datasets[activeColumn];
            
            if (mainChart) mainChart.destroy();
            
            mainChart = new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: chartData.labels,
                    datasets: [{{
                        label: dataset.label,
                        data: dataset.data,
                        borderColor: dataset.borderColor,
                        backgroundColor: dataset.backgroundColor,
                        borderWidth: 2,
                        pointRadius: 5,
                        pointHoverRadius: 7,
                        pointBackgroundColor: '#fff',
                        pointBorderColor: dataset.borderColor,
                        pointBorderWidth: 2,
                        tension: 0.3,
                        fill: true
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            backgroundColor: 'rgba(15, 23, 42, 0.9)',
                            padding: 12,
                            cornerRadius: 8,
                            titleFont: {{ weight: '600' }},
                            callbacks: {{
                                label: ctx => `${{ctx.dataset.label}}: ${{ctx.parsed.y.toLocaleString('pt-BR')}}`
                            }}
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            grid: {{ color: '#f1f5f9' }},
                            ticks: {{
                                color: '#64748b',
                                font: {{ size: 11 }},
                                callback: v => v.toLocaleString('pt-BR')
                            }}
                        }},
                        x: {{
                            grid: {{ display: false }},
                            ticks: {{ color: '#64748b', font: {{ size: 11 }} }}
                        }}
                    }}
                }}
            }});
            
            // Update stats
            const data = dataset.data.filter(d => d !== 0 && !isNaN(d));
            const total = data.reduce((a, b) => a + b, 0);
            const avg = data.length > 0 ? total / data.length : 0;
            const max = Math.max(...data);
            const min = Math.min(...data.filter(d => d > 0));
            
            document.getElementById('chartStats').innerHTML = `
                <div class="stat"><strong>Total:</strong> ${{total.toLocaleString('pt-BR', {{maximumFractionDigits: 0}})}}</div>
                <div class="stat"><strong>Média:</strong> ${{avg.toLocaleString('pt-BR', {{maximumFractionDigits: 1}})}}</div>
                <div class="stat"><strong>Máximo:</strong> ${{max.toLocaleString('pt-BR', {{maximumFractionDigits: 0}})}}</div>
                <div class="stat"><strong>Mínimo:</strong> ${{min.toLocaleString('pt-BR', {{maximumFractionDigits: 0}})}}</div>
            `;
        }}
        
        function renderTables() {{
            const container = document.getElementById('dataTablesContainer');
            const currentTable = tablesData.find(t => t.name === activeSheet);
            
            if (!currentTable || !currentTable.data || currentTable.data.length === 0) {{
                container.innerHTML = '<div class="data-section"><div class="no-data">Nenhum dado disponível</div></div>';
                return;
            }}
            
            container.innerHTML = `
                <div class="data-section">
                    <div class="section-header">Dados: ${{currentTable.name}}</div>
                    <div class="table-container">
                        <table class="data-table">
                            <thead>
                                <tr>${{currentTable.columns.map(col => `<th>${{col}}</th>`).join('')}}</tr>
                            </thead>
                            <tbody>
                                ${{currentTable.data.map(row => `
                                    <tr>
                                        ${{currentTable.columns.map((col, idx) => {{
                                            const val = row[col];
                                            const isNum = !isNaN(val) && val !== '' && val !== null;
                                            const formatted = isNum ? parseFloat(val).toLocaleString('pt-BR', {{maximumFractionDigits: 2}}) : (val || '-');
                                            return `<td class="${{isNum && idx > 0 ? 'numeric' : ''}}">${{formatted}}</td>`;
                                        }}).join('')}}
                                    </tr>
                                `).join('')}}
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        }}
        
        function renderAll() {{
            document.querySelectorAll('.sheet-btn').forEach(btn => {{
                btn.classList.toggle('active', btn.dataset.sheet === activeSheet);
            }});
            renderMetrics();
            renderColumnTabs();
            updateChart();
            renderTables();
        }}
        
        // Initialize
        renderSheetSelector();
        renderAll();
    </script>
</body>
</html>"""
        
        return html_content
    
    def convert(self):
        """Main conversion process"""
        print(f"Reading Excel file: {self.excel_file}")
        excel_data = self.read_excel()
        
        if not excel_data:
            print("Error: No sheets with data found")
            sys.exit(1)
        
        print(f"\nFound {len(excel_data)} sheet(s) with data")
        
        print("\nGenerating analytics dashboard...")
        html = self.generate_html(excel_data)
        
        print(f"Writing output to: {self.output_file}")
        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print("✅ Dashboard created successfully!")
        return self.output_file


def main():
    if len(sys.argv) < 2:
        print("Usage: python excel_to_dashboard.py <excel_file> [output_file]")
        sys.exit(1)
    
    excel_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "dashboard.html"
    
    if not Path(excel_file).exists():
        print(f"Error: File '{excel_file}' not found")
        sys.exit(1)
    
    converter = ExcelAnalyticsDashboard(excel_file, output_file)
    converter.convert()


if __name__ == "__main__":
    main()